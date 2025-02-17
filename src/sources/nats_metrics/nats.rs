use crate::config::GenerateConfig;
use crate::config::SourceConfig;
use crate::config::SourceContext;
use crate::config::SourceOutput;
use crate::event::Event;
use crate::internal_events::StreamClosedError;
use crate::nats::from_tls_auth_config;
use crate::nats::NatsAuthConfig;
use crate::nats::NatsConfigError;
use crate::shutdown::ShutdownSignal;
use crate::tls::TlsEnableableConfig;
use crate::SourceSender;
use bytes::Bytes;
use futures::{pin_mut, StreamExt};
use http::StatusCode;
use prost::Message;
use snafu::{ResultExt, Snafu};
use tokio::task;
use vector_lib::config::LogNamespace;
use vector_lib::configurable::configurable_component;
use vector_lib::internal_event::{ByteSize, BytesReceived, InternalEventHandle as _, Protocol};
use vector_lib::lookup::{lookup_v2::OptionalValuePath, owned_value_path};
use vector_lib::prometheus::parser::proto;

use crate::internal_events::DecoderFramingError;
use crate::sources::prometheus::parser::parse_request;
use crate::sources::util::{decode, ErrorMessage};

#[derive(Debug, Snafu)]
enum BuildError {
    #[snafu(display("NATS Config Error: {}", source))]
    Config { source: NatsConfigError },
    #[snafu(display("NATS Connect Error: {}", source))]
    Connect { source: async_nats::ConnectError },
    #[snafu(display("NATS Subscribe Error: {}", source))]
    Subscribe { source: async_nats::SubscribeError },
}

/// Configuration for the `nats` source.
#[configurable_component(source(
    "nats_metrics",
    "Read metrics from subjects on the NATS messaging system."
))]
#[derive(Clone, Debug, Derivative)]
#[derivative(Default)]
#[serde(deny_unknown_fields)]
pub struct NatsSourceConfig {
    /// The NATS URL to connect to.
    ///
    /// The URL takes the form of `nats://server:port`.
    /// If the port is not specified it defaults to 4222.
    #[configurable(metadata(docs::examples = "nats://demo.nats.io"))]
    #[configurable(metadata(docs::examples = "nats://127.0.0.1:4242,"))]
    #[configurable(metadata(
        docs::examples = "nats://localhost:4222,nats://localhost:5222,nats://localhost:6222"
    ))]
    url: String,

    /// A [name][nats_connection_name] assigned to the NATS connection.
    ///
    /// [nats_connection_name]: https://docs.nats.io/using-nats/developer/connecting/name
    #[serde(alias = "name")]
    #[configurable(metadata(docs::examples = "vector"))]
    connection_name: String,

    /// The NATS [subject][nats_subject] to pull messages from.
    ///
    /// [nats_subject]: https://docs.nats.io/nats-concepts/subjects
    #[configurable(metadata(docs::examples = "foo"))]
    #[configurable(metadata(docs::examples = "time.us.east"))]
    #[configurable(metadata(docs::examples = "time.*.east"))]
    #[configurable(metadata(docs::examples = "time.>"))]
    #[configurable(metadata(docs::examples = ">"))]
    subject: String,

    /// The NATS queue group to join.
    queue: Option<String>,

    #[configurable(derived)]
    #[serde(default = "default_concurrency")]
    #[derivative(Default(value = "default_concurrency()"))]
    concurrency: u16,

    // /// The namespace to use for logs. This overrides the global setting.
    // #[configurable(metadata(docs::hidden))]
    // #[serde(default)]
    // pub log_namespace: Option<bool>,
    #[configurable(derived)]
    tls: Option<TlsEnableableConfig>,

    #[configurable(derived)]
    auth: Option<NatsAuthConfig>,

    /// The `NATS` subject key.
    #[serde(default = "default_subject_key_field")]
    subject_key_field: OptionalValuePath,

    // #[configurable(derived)]
    // #[serde(default = "default_framing_message_based")]
    // #[derivative(Default(value = "default_framing_message_based()"))]
    // framing: FramingConfig,

    // #[configurable(derived)]
    // #[serde(default = "default_decoding")]
    // #[derivative(Default(value = "default_decoding()"))]
    // decoding: DeserializerConfig,
    /// The buffer capacity of the underlying NATS subscriber.
    ///
    /// This value determines how many messages the NATS subscriber buffers
    /// before incoming messages are dropped.
    ///
    /// See the [async_nats documentation][async_nats_subscription_capacity] for more information.
    ///
    /// [async_nats_subscription_capacity]: https://docs.rs/async-nats/latest/async_nats/struct.ConnectOptions.html#method.subscription_capacity
    #[serde(default = "default_subscription_capacity")]
    #[derivative(Default(value = "default_subscription_capacity()"))]
    subscriber_capacity: usize,
}

fn default_subject_key_field() -> OptionalValuePath {
    OptionalValuePath::from(owned_value_path!("subject"))
}

const fn default_subscription_capacity() -> usize {
    4096
}

const fn default_concurrency() -> u16 {
    16
}

impl GenerateConfig for NatsSourceConfig {
    fn generate_config() -> toml::Value {
        toml::from_str(
            r#"
            connection_name = "vector"
            subject = "from.vector"
            url = "nats://127.0.0.1:4222""#,
        )
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "nats_metrics")]
impl SourceConfig for NatsSourceConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<crate::sources::Source> {
        // let (connection, subscription) = create_subscription(self).await?;
        Ok(Box::pin(nats_source(self.clone(), cx.shutdown, cx.out)))
    }

    fn outputs(&self, _global_log_namespace: LogNamespace) -> Vec<SourceOutput> {
        vec![SourceOutput::new_metrics()]
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}

impl NatsSourceConfig {
    async fn connect(&self) -> Result<async_nats::Client, BuildError> {
        let options: async_nats::ConnectOptions = self.try_into().context(ConfigSnafu)?;
        options.connect(&self.url).await.context(ConnectSnafu)
    }
}

impl TryFrom<&NatsSourceConfig> for async_nats::ConnectOptions {
    type Error = NatsConfigError;

    fn try_from(config: &NatsSourceConfig) -> Result<Self, Self::Error> {
        from_tls_auth_config(&config.connection_name, &config.auth, &config.tls)
            .map(|options| options.subscription_capacity(config.subscriber_capacity))
    }
}

async fn nats_source(
    config: NatsSourceConfig,
    shutdown: ShutdownSignal,
    out: SourceSender,
) -> Result<(), ()> {
    let concurrency = config.concurrency;
    for _i in 0..concurrency {
        let shutdown = shutdown.clone();
        let config = config.clone();
        let mut out = out.clone();
        task::spawn(async move {
            let (_con, subscriber) = create_subscription(&config).await.unwrap();
            let stream = subscriber.take_until(shutdown);
            pin_mut!(stream);
            let bytes_received = register!(BytesReceived::from(Protocol::TCP));
            while let Some(msg) = stream.next().await {
                bytes_received.emit(ByteSize(msg.payload.len()));
                let events = decode(Some("snappy"), msg.payload).and_then(|body| {
                    let events = decode_body(body)?;
                    Ok(events)
                });
                match events {
                    Ok(events) => {
                        let count = events.len();
                        match out.send_batch(events).await {
                            Ok(()) => {}
                            Err(_) => emit!(StreamClosedError { count }),
                        }
                    }
                    Err(e) => {
                        info!("decode exception!!!! {}", e);
                        emit!(DecoderFramingError { error: e })
                    }
                }
            }
        });
    }

    Ok(())
}

fn decode_body(body: Bytes) -> Result<Vec<Event>, ErrorMessage> {
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

async fn create_subscription(
    config: &NatsSourceConfig,
) -> Result<(async_nats::Client, async_nats::Subscriber), BuildError> {
    let nc = config.connect().await?;

    let subscription = match &config.queue {
        None => nc.subscribe(config.subject.clone()).await,
        Some(queue) => {
            nc.queue_subscribe(config.subject.clone(), queue.clone())
                .await
        }
    };

    let subscription = subscription.context(SubscribeSnafu)?;

    Ok((nc, subscription))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::print_stdout)] //tests

    use vector_lib::lookup::{owned_value_path, OwnedTargetPath};
    use vector_lib::schema::Definition;
    use vrl::value::{kind::Collection, Kind};

    use super::*;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<NatsSourceConfig>();
    }

    #[test]
    fn output_schema_definition_vector_namespace() {
        let config = NatsSourceConfig {
            subject_key_field: default_subject_key_field(),
            ..Default::default()
        };

        let definitions = config
            .outputs(LogNamespace::Vector)
            .remove(0)
            .schema_definition(true);

        let expected_definition =
            Definition::new_with_default_metadata(Kind::bytes(), [LogNamespace::Vector])
                .with_meaning(OwnedTargetPath::event_root(), "message")
                .with_metadata_field(
                    &owned_value_path!("vector", "source_type"),
                    Kind::bytes(),
                    None,
                )
                .with_metadata_field(
                    &owned_value_path!("vector", "ingest_timestamp"),
                    Kind::timestamp(),
                    None,
                )
                .with_metadata_field(&owned_value_path!("nats", "subject"), Kind::bytes(), None);

        assert_eq!(definitions, Some(expected_definition));
    }

    #[test]
    fn output_schema_definition_legacy_namespace() {
        let config = NatsSourceConfig {
            subject_key_field: default_subject_key_field(),
            ..Default::default()
        };
        let definitions = config
            .outputs(LogNamespace::Legacy)
            .remove(0)
            .schema_definition(true);

        let expected_definition = Definition::new_with_default_metadata(
            Kind::object(Collection::empty()),
            [LogNamespace::Legacy],
        )
        .with_event_field(
            &owned_value_path!("message"),
            Kind::bytes(),
            Some("message"),
        )
        .with_event_field(&owned_value_path!("timestamp"), Kind::timestamp(), None)
        .with_event_field(&owned_value_path!("source_type"), Kind::bytes(), None)
        .with_event_field(&owned_value_path!("subject"), Kind::bytes(), None);

        assert_eq!(definitions, Some(expected_definition));
    }
}

#[cfg(feature = "nats-integration-tests")]
#[cfg(test)]
mod integration_tests {
    #![allow(clippy::print_stdout)] //tests

    use bytes::Bytes;
    use vector_lib::config::log_schema;

    use super::*;
    use crate::nats::{NatsAuthCredentialsFile, NatsAuthNKey, NatsAuthToken, NatsAuthUserPassword};
    use crate::test_util::{
        collect_n,
        components::{assert_source_compliance, SOURCE_TAGS},
        random_string,
    };
    use crate::tls::TlsConfig;

    async fn publish_and_check(conf: NatsSourceConfig) -> Result<(), BuildError> {
        let subject = conf.subject.clone();
        let (nc, sub) = create_subscription(&conf).await?;
        let nc_pub = nc.clone();
        let msg = "my message";

        let events = assert_source_compliance(&SOURCE_TAGS, async move {
            let (tx, rx) = SourceSender::new_test();
            tokio::spawn(nats_source(conf.clone(), ShutdownSignal::noop(), tx));
            nc_pub
                .publish(subject, Bytes::from_static(msg.as_bytes()))
                .await
                .unwrap();

            collect_n(rx, 1).await
        })
        .await;

        println!("Received event  {:?}", events[0].as_log());
        assert_eq!(
            events[0].as_log()[log_schema().message_key().unwrap().to_string()],
            msg.into()
        );
        Ok(())
    }

    #[tokio::test]
    async fn nats_no_auth() {
        let subject = format!("test-{}", random_string(10));
        let url =
            std::env::var("NATS_ADDRESS").unwrap_or_else(|_| String::from("nats://localhost:4222"));

        let conf = NatsSourceConfig {
            connection_name: "".to_owned(),
            subject: subject.clone(),
            url,
            queue: None,
            tls: None,
            auth: None,
            subject_key_field: default_subject_key_field(),
            ..Default::default()
        };

        let r = publish_and_check(conf).await;
        assert!(
            r.is_ok(),
            "publish_and_check failed, expected Ok(()), got: {:?}",
            r
        );
    }

    #[tokio::test]
    async fn nats_userpass_auth_valid() {
        let subject = format!("test-{}", random_string(10));
        let url = std::env::var("NATS_USERPASS_ADDRESS")
            .unwrap_or_else(|_| String::from("nats://localhost:4222"));

        let conf = NatsSourceConfig {
            connection_name: "".to_owned(),
            subject: subject.clone(),
            url,
            queue: None,
            tls: None,
            auth: Some(NatsAuthConfig::UserPassword {
                user_password: NatsAuthUserPassword {
                    user: "natsuser".to_string(),
                    password: "natspass".to_string().into(),
                },
            }),
            subject_key_field: default_subject_key_field(),
            ..Default::default()
        };

        let r = publish_and_check(conf).await;
        assert!(
            r.is_ok(),
            "publish_and_check failed, expected Ok(()), got: {:?}",
            r
        );
    }

    #[tokio::test]
    async fn nats_userpass_auth_invalid() {
        let subject = format!("test-{}", random_string(10));
        let url = std::env::var("NATS_USERPASS_ADDRESS")
            .unwrap_or_else(|_| String::from("nats://localhost:4222"));

        let conf = NatsSourceConfig {
            connection_name: "".to_owned(),
            subject: subject.clone(),
            url,
            queue: None,
            tls: None,
            auth: Some(NatsAuthConfig::UserPassword {
                user_password: NatsAuthUserPassword {
                    user: "natsuser".to_string(),
                    password: "wrongpass".to_string().into(),
                },
            }),
            subject_key_field: default_subject_key_field(),
            ..Default::default()
        };

        let r = publish_and_check(conf).await;
        assert!(
            matches!(r, Err(BuildError::Connect { .. })),
            "publish_and_check failed, expected BuildError::Connect, got: {:?}",
            r
        );
    }

    #[tokio::test]
    async fn nats_token_auth_valid() {
        let subject = format!("test-{}", random_string(10));
        let url = std::env::var("NATS_TOKEN_ADDRESS")
            .unwrap_or_else(|_| String::from("nats://localhost:4222"));

        let conf = NatsSourceConfig {
            connection_name: "".to_owned(),
            subject: subject.clone(),
            url,
            queue: None,
            tls: None,
            auth: Some(NatsAuthConfig::Token {
                token: NatsAuthToken {
                    value: "secret".to_string().into(),
                },
            }),
            subject_key_field: default_subject_key_field(),
            ..Default::default()
        };

        let r = publish_and_check(conf).await;
        assert!(
            r.is_ok(),
            "publish_and_check failed, expected Ok(()), got: {:?}",
            r
        );
    }

    #[tokio::test]
    async fn nats_token_auth_invalid() {
        let subject = format!("test-{}", random_string(10));
        let url = std::env::var("NATS_TOKEN_ADDRESS")
            .unwrap_or_else(|_| String::from("nats://localhost:4222"));

        let conf = NatsSourceConfig {
            connection_name: "".to_owned(),
            subject: subject.clone(),
            url,
            queue: None,
            tls: None,
            auth: Some(NatsAuthConfig::Token {
                token: NatsAuthToken {
                    value: "wrongsecret".to_string().into(),
                },
            }),
            subject_key_field: default_subject_key_field(),
            ..Default::default()
        };

        let r = publish_and_check(conf).await;
        assert!(
            matches!(r, Err(BuildError::Connect { .. })),
            "publish_and_check failed, expected BuildError::Connect, got: {:?}",
            r
        );
    }

    #[tokio::test]
    async fn nats_nkey_auth_valid() {
        let subject = format!("test-{}", random_string(10));
        let url = std::env::var("NATS_NKEY_ADDRESS")
            .unwrap_or_else(|_| String::from("nats://localhost:4222"));

        let conf = NatsSourceConfig {
            connection_name: "".to_owned(),
            subject: subject.clone(),
            url,
            queue: None,
            tls: None,
            auth: Some(NatsAuthConfig::Nkey {
                nkey: NatsAuthNKey {
                    nkey: "UD345ZYSUJQD7PNCTWQPINYSO3VH4JBSADBSYUZOBT666DRASFRAWAWT".into(),
                    seed: "SUANIRXEZUROTXNFN3TJYMT27K7ZZVMD46FRIHF6KXKS4KGNVBS57YAFGY".into(),
                },
            }),
            subject_key_field: default_subject_key_field(),
            ..Default::default()
        };

        let r = publish_and_check(conf).await;
        assert!(
            r.is_ok(),
            "publish_and_check failed, expected Ok(()), got: {:?}",
            r
        );
    }

    #[tokio::test]
    async fn nats_nkey_auth_invalid() {
        let subject = format!("test-{}", random_string(10));
        let url = std::env::var("NATS_NKEY_ADDRESS")
            .unwrap_or_else(|_| String::from("nats://localhost:4222"));

        let conf = NatsSourceConfig {
            connection_name: "".to_owned(),
            subject: subject.clone(),
            url,
            queue: None,
            tls: None,
            auth: Some(NatsAuthConfig::Nkey {
                nkey: NatsAuthNKey {
                    nkey: "UAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".into(),
                    seed: "SBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB".into(),
                },
            }),
            subject_key_field: default_subject_key_field(),
            ..Default::default()
        };

        let r = publish_and_check(conf).await;
        assert!(
            matches!(r, Err(BuildError::Connect { .. })),
            "publish_and_check failed, expected BuildError::Config, got: {:?}",
            r
        );
    }

    #[tokio::test]
    async fn nats_tls_valid() {
        let subject = format!("test-{}", random_string(10));
        let url = std::env::var("NATS_TLS_ADDRESS")
            .unwrap_or_else(|_| String::from("nats://localhost:4222"));

        let conf = NatsSourceConfig {
            connection_name: "".to_owned(),
            subject: subject.clone(),
            url,
            queue: None,
            tls: Some(TlsEnableableConfig {
                enabled: Some(true),
                options: TlsConfig {
                    ca_file: Some("tests/data/nats/rootCA.pem".into()),
                    ..Default::default()
                },
            }),
            auth: None,
            subject_key_field: default_subject_key_field(),
            ..Default::default()
        };

        let r = publish_and_check(conf).await;
        assert!(
            r.is_ok(),
            "publish_and_check failed, expected Ok(()), got: {:?}",
            r
        );
    }

    #[tokio::test]
    async fn nats_tls_invalid() {
        let subject = format!("test-{}", random_string(10));
        let url = std::env::var("NATS_TLS_ADDRESS")
            .unwrap_or_else(|_| String::from("nats://localhost:4222"));

        let conf = NatsSourceConfig {
            connection_name: "".to_owned(),
            subject: subject.clone(),
            url,
            queue: None,
            tls: None,
            auth: None,
            subject_key_field: default_subject_key_field(),
            ..Default::default()
        };

        let r = publish_and_check(conf).await;
        assert!(
            matches!(r, Err(BuildError::Connect { .. })),
            "publish_and_check failed, expected BuildError::Connect, got: {:?}",
            r
        );
    }

    #[tokio::test]
    async fn nats_tls_client_cert_valid() {
        let subject = format!("test-{}", random_string(10));
        let url = std::env::var("NATS_TLS_CLIENT_CERT_ADDRESS")
            .unwrap_or_else(|_| String::from("nats://localhost:4222"));

        let conf = NatsSourceConfig {
            connection_name: "".to_owned(),
            subject: subject.clone(),
            url,
            queue: None,
            tls: Some(TlsEnableableConfig {
                enabled: Some(true),
                options: TlsConfig {
                    ca_file: Some("tests/data/nats/rootCA.pem".into()),
                    crt_file: Some("tests/data/nats/nats-client.pem".into()),
                    key_file: Some("tests/data/nats/nats-client.key".into()),
                    ..Default::default()
                },
            }),
            auth: None,
            subject_key_field: default_subject_key_field(),
            ..Default::default()
        };

        let r = publish_and_check(conf).await;
        assert!(
            r.is_ok(),
            "publish_and_check failed, expected Ok(()), got: {:?}",
            r
        );
    }

    #[tokio::test]
    async fn nats_tls_client_cert_invalid() {
        let subject = format!("test-{}", random_string(10));
        let url = std::env::var("NATS_TLS_CLIENT_CERT_ADDRESS")
            .unwrap_or_else(|_| String::from("nats://localhost:4222"));

        let conf = NatsSourceConfig {
            connection_name: "".to_owned(),
            subject: subject.clone(),
            url,
            queue: None,
            tls: Some(TlsEnableableConfig {
                enabled: Some(true),
                options: TlsConfig {
                    ca_file: Some("tests/data/nats/rootCA.pem".into()),
                    ..Default::default()
                },
            }),
            auth: None,
            subject_key_field: default_subject_key_field(),
            ..Default::default()
        };

        let r = publish_and_check(conf).await;
        assert!(
            matches!(r, Err(BuildError::Connect { .. })),
            "publish_and_check failed, expected BuildError::Connect, got: {:?}",
            r
        );
    }

    #[tokio::test]
    async fn nats_tls_jwt_auth_valid() {
        let subject = format!("test-{}", random_string(10));
        let url = std::env::var("NATS_JWT_ADDRESS")
            .unwrap_or_else(|_| String::from("nats://localhost:4222"));

        let conf = NatsSourceConfig {
            connection_name: "".to_owned(),
            subject: subject.clone(),
            url,
            queue: None,
            tls: Some(TlsEnableableConfig {
                enabled: Some(true),
                options: TlsConfig {
                    ca_file: Some("tests/data/nats/rootCA.pem".into()),
                    ..Default::default()
                },
            }),
            auth: Some(NatsAuthConfig::CredentialsFile {
                credentials_file: NatsAuthCredentialsFile {
                    path: "tests/data/nats/nats.creds".into(),
                },
            }),
            subject_key_field: default_subject_key_field(),
            ..Default::default()
        };

        let r = publish_and_check(conf).await;
        assert!(
            r.is_ok(),
            "publish_and_check failed, expected Ok(()), got: {:?}",
            r
        );
    }

    #[tokio::test]
    async fn nats_tls_jwt_auth_invalid() {
        let subject = format!("test-{}", random_string(10));
        let url = std::env::var("NATS_JWT_ADDRESS")
            .unwrap_or_else(|_| String::from("nats://localhost:4222"));

        let conf = NatsSourceConfig {
            connection_name: "".to_owned(),
            subject: subject.clone(),
            url,
            queue: None,
            tls: Some(TlsEnableableConfig {
                enabled: Some(true),
                options: TlsConfig {
                    ca_file: Some("tests/data/nats/rootCA.pem".into()),
                    ..Default::default()
                },
            }),
            auth: Some(NatsAuthConfig::CredentialsFile {
                credentials_file: NatsAuthCredentialsFile {
                    path: "tests/data/nats/nats-bad.creds".into(),
                },
            }),
            subject_key_field: default_subject_key_field(),
            ..Default::default()
        };

        let r = publish_and_check(conf).await;
        assert!(
            matches!(r, Err(BuildError::Connect { .. })),
            "publish_and_check failed, expected BuildError::Connect, got: {:?}",
            r
        );
    }
}
