use std::panic;
use std::time::Duration;

use bytes::Bytes;
use futures::FutureExt;
use http::StatusCode;
use prost::Message;
use rumqttc::{AsyncClient, ClientError, EventLoop, Incoming, MqttOptions, QoS};
use rumqttc::{Event as MqttEvent, Publish};
use snafu::Snafu;
use tokio::{pin, select};

use vector_lib::prometheus::parser::proto;
use vector_lib::tls::TlsError;

use crate::{
    internal_events::{EndpointBytesReceived, StreamClosedError},
    shutdown::ShutdownSignal,
    SourceSender,
};
use crate::common::http::ErrorMessage;
use crate::event::Event;
use crate::internal_events::DecoderFramingError;
use crate::sources::prometheus::parser::parse_request;
use crate::sources::util::{decode};

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
        let (client, eventloop) = AsyncClient::new(self.options.clone(), 65535);
        client.subscribe(&self.topic, QoS::AtLeastOnce).await?;
        Ok((client, eventloop))
    }
}

#[derive(Clone)]
pub struct MqttSource {
    connectors: Vec<MqttConnector>,
}

impl MqttSource {
    pub fn new(connectors: Vec<MqttConnector>) -> crate::Result<Self> {
        Ok(Self { connectors })
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
                                    }
                                    Ok(MqttEvent::Incoming(Incoming::Disconnect |Incoming::Unsubscribe(_))) => {
                                        error!("Receiver Disconnect|Unsubscribe packet from MQTT broker, try to resubscribe topic {}",connector.topic);
                                        tokio::time::sleep(Duration::from_secs(5)).await;
                                         _client.subscribe(connector.topic.clone(), QoS::AtLeastOnce).await.unwrap();
                                    }
                                    Ok(_) => { }
                                    Err(error) => {
                                        error!("Get error from mqtt broker: {}, try to resubscribe the topic {}",error,connector.topic);
                                        tokio::time::sleep(Duration::from_secs(5)).await;
                                        _client.subscribe(connector.topic.clone(), QoS::AtLeastOnce).await.unwrap();
                                    }
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

    fn decode_body(&self, body: Bytes) -> Result<Vec<Event>, ErrorMessage> {
        let request = proto::WriteRequest::decode(body).map_err(|error| {
            ErrorMessage::new(
                StatusCode::BAD_REQUEST,
                format!("Could not decode write request: {}", error),
            )
        })?;
        parse_request(request).map_err(|error| {
            ErrorMessage::new(
                StatusCode::BAD_REQUEST,
                format!("Could not parse write request to event: {}", error),
            )
        })
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
        let events = decode(Some("snappy"), publish.payload).and_then(|body| {
            let events = self.decode_body(body)?;
            Ok(events)
        });
        self.handle_request(events, out.clone()).await;
    }

    async fn handle_request(
        &self,
        events: Result<Vec<Event>, ErrorMessage>,
        mut out: SourceSender,
    ) {
        match events {
            Ok(events) => {
                let count = events.len();
                match out.send_batch(events).await {
                    Ok(()) => {}
                    Err(_) => emit!(StreamClosedError { count }),
                }
            }
            Err(e) => {
                info!("decodec exception!!!! {}", e);
                emit!(DecoderFramingError { error: e })
            }
        }
    }
}
