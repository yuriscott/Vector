use std::panic;
use std::time::Duration;

use futures::FutureExt;
use itertools::Itertools;
use rumqttc::{AsyncClient, ClientError, EventLoop, Incoming, MqttOptions, QoS};
use rumqttc::{Event as MqttEvent, Publish};
use snafu::Snafu;
use tokio::{pin, select};

use vector_lib::config::LogNamespace;
use vector_lib::internal_event::EventsReceived;
use vector_lib::tls::TlsError;

use crate::{
    codecs::Decoder,
    event::BatchNotifier,
    internal_events::{EndpointBytesReceived, StreamClosedError},
    shutdown::ShutdownSignal,
    sources::util,
    SourceSender,
};

use super::config::ConfigurationError;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum MqttError {
    #[snafu(display("TLS error: {}", source))]
    Tls { source: TlsError },
    #[snafu(display("MQTT configuration error: {}", source))]
    Configuration { source: ConfigurationError },
}

#[derive(Clone)]
pub struct MqttConnector {
    options: MqttOptions,
    topic: String,
}

impl MqttConnector {
    pub const fn new(options: MqttOptions, topic: String) -> Result<Self, MqttError> {
        Ok(Self { options, topic })
    }

    async fn connect(&self) -> Result<(AsyncClient, EventLoop), ClientError> {
        let (client, eventloop) = AsyncClient::new(self.options.clone(), 1024);
        client.subscribe(&self.topic, QoS::AtLeastOnce).await?;
        Ok((client, eventloop))
    }
}

#[derive(Clone)]
pub struct MqttSource {
    connectors: Vec<MqttConnector>,
    decoder: Decoder,
    log_namespace: LogNamespace,
}

impl MqttSource {
    pub fn new(
        connectors: Vec<MqttConnector>,
        decoder: Decoder,
        log_namespace: LogNamespace,
    ) -> crate::Result<Self> {
        Ok(Self {
            connectors,
            decoder,
            log_namespace,
        })
    }

    pub async fn run(self, out: SourceSender, shutdown: ShutdownSignal) -> Result<(), ()> {
        let mut task_handles = vec![];
        let connectors = self.connectors.clone();
        for connector in connectors {
            let mut out = out.clone();
            let source = self.clone();
            let shutdown = shutdown.clone().fuse();
            let (_client, mut event_loop) = connector.connect().await.map_err(|_| ())?;
            task_handles.push(tokio::spawn(
                async move {
                    pin!(shutdown);
                    loop {
                        select! {
                            _ = &mut shutdown => break,
                            mqtt_event = event_loop.poll() => {
                                // If an error is returned here there is currently no way to tie this back
                                // to the event that was posted which means we can't accurately provide
                                // delivery guarantees.
                                // We need this issue resolved first:
                                // https://github.com/bytebeamio/rumqtt/issues/349
                                match mqtt_event {
                                    Ok(MqttEvent::Incoming(Incoming::Publish(publish))) => {
                                        source.process_message(&connector, publish, &mut out).await;
                                    }
                                    Ok(MqttEvent::Incoming(
                                        Incoming::PubAck(_) | Incoming::PubRec(_) | Incoming::PubComp(_),
                                    )) => {
                                        // TODO Handle acknowledgement
                                        error!("Receiver Disconnect|Unsubscribe packet from MQTT broker, try to resubscribe topic {}",connector.topic);
                                                    tokio::time::sleep(Duration::from_secs(5)).await;
                                                     _client.subscribe(connector.topic.clone(), QoS::AtLeastOnce).await.unwrap();
                                    }
                                    Err(error) => {
                                        error!("Get error from mqtt broker: {}, try to resubscribe the topic {}",error,connector.topic);
                                        tokio::time::sleep(Duration::from_secs(5)).await;
                                        _client.subscribe(connector.topic.clone(), QoS::AtLeastOnce).await.unwrap();
                                    }
                                    Ok(_) => { }
                                }
                            }
                        }
                    }
                }
            ));
        }

        for task_handle in task_handles.drain(..) {
            if let Err(e) = task_handle.await {
                if e.is_panic() {
                    panic::resume_unwind(e.into_panic());
                }
            }
        }

        Ok(())
    }

    async fn process_message(
        &self,
        connector: &MqttConnector,
        publish: Publish,
        out: &mut SourceSender,
    ) {
        emit!(EndpointBytesReceived {
            byte_size: publish.payload.len(),
            protocol: "mqtt",
            endpoint: connector.options.broker_address().0.as_str(),
        });
        let events_received = register!(EventsReceived);

        let (batch, _batch_receiver) = BatchNotifier::maybe_new_with_receiver(false);
        // Error is logged by `crate::codecs::Decoder`, no further handling
        // is needed here.
        let decoded = util::decode_message(
            self.decoder.clone(),
            "mqtt",
            &publish.payload,
            None,
            &batch,
            self.log_namespace,
            &events_received,
        )
        .collect_vec();

        let count = decoded.len();

        match out.send_batch(decoded).await {
            Ok(()) => {}
            Err(e) => {
                info!("decodec exception!!!! {}", e);
                emit!(StreamClosedError { count })
            }
        }
    }
}
