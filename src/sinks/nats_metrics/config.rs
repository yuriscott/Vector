use bytes::Bytes;

use futures_util::TryFutureExt;
use snafu::ResultExt;
use vector_lib::tls::TlsEnableableConfig;

use crate::{
    nats::{from_tls_auth_config, NatsAuthConfig, NatsConfigError},
    sinks::{prelude::*, util::service::TowerRequestConfigDefaults},
};

use super::{sink::NatsMetricsSink, ConfigSnafu, ConnectSnafu, NatsError};

#[derive(Clone, Copy, Debug)]
pub struct NatsTowerRequestConfigDefaults;

impl TowerRequestConfigDefaults for NatsTowerRequestConfigDefaults {
    const CONCURRENCY: Concurrency = Concurrency::None;
}

/// Configuration for the `nats-metrics` sink.
#[configurable_component(sink(
    "nats_metrics",
    "Publish metrics data to subjects on the NATS messaging system."
))]
#[derive(Clone, Debug, Derivative)]
#[serde(deny_unknown_fields)]
pub struct NatsMetricsSinkConfig {
    #[configurable(derived)]
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::is_default"
    )]
    pub acknowledgements: AcknowledgementsConfig,

    /// A NATS [name][nats_connection_name] assigned to the NATS connection.
    ///
    /// [nats_connection_name]: https://docs.nats.io/using-nats/developer/connecting/name
    #[serde(default = "default_name", alias = "name")]
    #[configurable(metadata(docs::examples = "foo"))]
    pub(super) connection_name: String,

    /// The NATS [subject][nats_subject] to publish messages to.
    ///
    /// [nats_subject]: https://docs.nats.io/nats-concepts/subjects
    #[configurable(metadata(docs::templateable))]
    #[configurable(metadata(
        docs::examples = "{{ host }}",
        docs::examples = "foo",
        docs::examples = "time.us.east",
        docs::examples = "time.*.east",
        docs::examples = "time.>",
        docs::examples = ">"
    ))]
    pub(super) subject: Template,

    /// The NATS [URL][nats_url] to connect to.
    ///
    /// The URL must take the form of `nats://server:port`.
    /// If the port is not specified it defaults to 4222.
    ///
    /// [nats_url]: https://docs.nats.io/using-nats/developer/connecting#nats-url
    #[configurable(metadata(docs::examples = "nats://demo.nats.io"))]
    #[configurable(metadata(docs::examples = "nats://127.0.0.1:4242"))]
    #[configurable(metadata(
        docs::examples = "nats://localhost:4222,nats://localhost:5222,nats://localhost:6222"
    ))]
    pub(super) url: String,

    #[configurable(derived)]
    pub(super) tls: Option<TlsEnableableConfig>,

    #[configurable(derived)]
    pub(super) auth: Option<NatsAuthConfig>,

    #[configurable(derived)]
    #[serde(default)]
    pub(super) request: TowerRequestConfig<NatsTowerRequestConfigDefaults>,

    /// Send messages using [Jetstream][jetstream].
    ///
    /// If set, the `subject` must belong to an existing JetStream stream.
    ///
    /// [jetstream]: https://docs.nats.io/nats-concepts/jetstream
    #[serde(default)]
    pub(super) jetstream: bool,

    /// The default namespace for any metrics sent.
    ///
    /// This namespace is only used if a metric has no existing namespace. When a namespace is
    /// present, it is used as a prefix to the metric name, and separated with an underscore (`_`).
    ///
    /// It should follow the Prometheus [naming conventions][prom_naming_docs].
    ///
    /// [prom_naming_docs]: https://prometheus.io/docs/practices/naming/#metric-names
    #[configurable(metadata(docs::examples = "service"))]
    #[configurable(metadata(docs::advanced))]
    pub default_namespace: Option<String>,

    /// Default buckets to use for aggregating [distribution][dist_metric_docs] metrics into histograms.
    ///
    /// [dist_metric_docs]: https://vector.dev/docs/about/under-the-hood/architecture/data-model/metric/#distribution
    #[serde(default = "crate::sinks::nats_metrics::default_histogram_buckets")]
    #[configurable(metadata(docs::advanced))]
    pub buckets: Vec<f64>,

    /// Quantiles to use for aggregating [distribution][dist_metric_docs] metrics into a summary.
    ///
    /// [dist_metric_docs]: https://vector.dev/docs/about/under-the-hood/architecture/data-model/metric/#distribution
    #[serde(default = "crate::sinks::nats_metrics::default_summary_quantiles")]
    #[configurable(metadata(docs::advanced))]
    pub quantiles: Vec<f64>,

    #[configurable(derived)]
    #[serde(default)]
    pub batch: NatsBatchConfig,

    #[configurable(derived)]
    #[configurable(metadata(docs::advanced))]
    #[serde(default = "default_compression")]
    #[derivative(Default(value = "default_compression()"))]
    pub compression: Compression,
}

/// The batch config for remote write.
#[configurable_component]
#[derive(Clone, Copy, Debug, Derivative)]
#[derivative(Default)]
pub struct NatsBatchConfig {
    #[configurable(derived)]
    #[serde(flatten)]
    pub batch_settings: BatchConfig<NatsBatchSettings>,

    /// Whether or not to aggregate metrics within a batch.
    #[serde(default = "crate::serde::default_true")]
    #[derivative(Default(value = "true"))]
    pub aggregate: bool,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NatsBatchSettings;

impl SinkBatchSettings for NatsBatchSettings {
    const MAX_EVENTS: Option<usize> = Some(1_000);
    const MAX_BYTES: Option<usize> = None;
    const TIMEOUT_SECS: f64 = 1.0;
}

fn default_name() -> String {
    String::from("vector")
}

const fn default_compression() -> Compression {
    Compression::Snappy
}

impl GenerateConfig for NatsMetricsSinkConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            acknowledgements: Default::default(),
            auth: None,
            connection_name: "vector".into(),
            subject: Template::try_from("from.vector").unwrap(),
            tls: None,
            url: "nats://127.0.0.1:4222".into(),
            request: Default::default(),
            jetstream: Default::default(),
            default_namespace: Default::default(),
            buckets: Default::default(),
            quantiles: Default::default(),
            batch: Default::default(),
            compression: Default::default(),
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "nats_metrics")]
impl SinkConfig for NatsMetricsSinkConfig {
    async fn build(&self, _cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let sink = NatsMetricsSink::new(self.clone()).await?;
        let healthcheck = healthcheck(self.clone()).boxed();
        Ok((VectorSink::from_event_streamsink(sink), healthcheck))
    }

    fn input(&self) -> Input {
        Input::new(DataType::Metric)
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

impl TryFrom<&NatsMetricsSinkConfig> for async_nats::ConnectOptions {
    type Error = NatsConfigError;

    fn try_from(config: &NatsMetricsSinkConfig) -> Result<Self, Self::Error> {
        from_tls_auth_config(&config.connection_name, &config.auth, &config.tls)
    }
}

impl NatsMetricsSinkConfig {
    pub(super) async fn connect(&self) -> Result<async_nats::Client, NatsError> {
        let options: async_nats::ConnectOptions = self.try_into().context(ConfigSnafu)?;

        let urls = self.parse_server_addresses()?;
        options.connect(urls).await.context(ConnectSnafu)
    }

    fn parse_server_addresses(&self) -> Result<Vec<async_nats::ServerAddr>, NatsError> {
        self.url
            .split(',')
            .map(|url| {
                url.parse::<async_nats::ServerAddr>()
                    .map_err(|_| NatsError::Connect {
                        source: async_nats::ConnectErrorKind::ServerParse.into(),
                    })
            })
            .collect()
    }

    pub(super) async fn publisher(&self) -> Result<NatsPublisher, NatsError> {
        let connection = self.connect().await?;

        if self.jetstream {
            Ok(NatsPublisher::JetStream(async_nats::jetstream::new(
                connection,
            )))
        } else {
            Ok(NatsPublisher::Core(connection))
        }
    }
}

async fn healthcheck(config: NatsMetricsSinkConfig) -> crate::Result<()> {
    config.connect().map_ok(|_| ()).map_err(|e| e.into()).await
}

pub enum NatsPublisher {
    Core(async_nats::Client),
    JetStream(async_nats::jetstream::Context),
}

impl NatsPublisher {
    pub(super) async fn publish<S: async_nats::subject::ToSubject>(
        &self,
        subject: S,
        payload: Bytes,
    ) -> Result<(), NatsError> {
        match self {
            NatsPublisher::Core(client) => {
                client
                    .publish(subject, payload)
                    .await
                    .map_err(|e| NatsError::PublishError {
                        source: Box::new(e),
                    })?;
                client
                    .flush()
                    .map_ok(|_| ())
                    .map_err(|e| NatsError::PublishError {
                        source: Box::new(e),
                    })
                    .await
            }
            NatsPublisher::JetStream(jetstream) => {
                let ack = jetstream.publish(subject, payload).await.map_err(|e| {
                    NatsError::PublishError {
                        source: Box::new(e),
                    }
                })?;
                ack.await.map(|_| ()).map_err(|e| NatsError::PublishError {
                    source: Box::new(e),
                })
            }
        }
    }
}
