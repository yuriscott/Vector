use std::time::Duration;

use rand::Rng;
use rumqttc::{MqttOptions, TlsConfiguration, Transport};
use snafu::{ResultExt, Snafu};

use vector_lib::{
    codecs::decoding::FramingConfig,
    config::LogNamespace,
    configurable::configurable_component,
    tls::{MaybeTlsSettings, TlsEnableableConfig},
};

use crate::{
    config::{SourceConfig, SourceContext, SourceOutput},
    serde::default_framing_message_based,
};

use super::source::{ConfigurationSnafu, MqttConnector, MqttError, MqttSource, TlsSnafu};

#[derive(Clone, Debug, Eq, PartialEq, Snafu)]
pub enum ConfigurationError {
    #[snafu(display(
        "Client ID must be 1-23 characters long and must consist of only alphanumeric characters."
    ))]
    InvalidClientId,

    #[snafu(display("Username and password must be either both or neither provided."))]
    BadCredentials,
}

/// Shared Group
#[configurable_component]
#[configurable(metadata(docs::advanced))]
#[derive(Clone, Debug, Default, Derivative)]
pub struct Share {
    /// MQTT group setting
    pub enabled: Option<bool>,

    /// MQTT group setting
    #[configurable(derived)]
    #[serde(default = "default_group")]
    #[derivative(Default(value = "default_group()"))]
    pub group: String,

    /// MQTT group setting.
    #[configurable(derived)]
    #[serde(default = "default_concurrency")]
    #[derivative(Default(value = "default_concurrency()"))]
    pub concurrency: u16,
}

impl Share {
    pub fn is_share(&self) -> bool {
        self.enabled.unwrap_or_else(|| false)
    }

    pub fn share_topic(&self, topic: &str) -> String {
        let prefix = format!(
            "$share/{}",
            self.group.trim_start_matches('/').trim_end_matches('/')
        );
        format!("{}/{}", prefix, topic)
    }
}

/// Configuration for the `mqtt` source.
#[configurable_component(source("mqtt", "Collect metrics from MQTT."))]
#[derive(Clone, Debug, Derivative)]
#[derivative(Default)]
#[serde(deny_unknown_fields)]
pub struct MqttSourceConfig {
    /// MQTT server address (The broker’s domain name or IP address).
    #[configurable(metadata(docs::examples = "mqtt.example.com", docs::examples = "127.0.0.1"))]
    pub host: String,

    /// TCP port of the MQTT server to connect to.
    #[configurable(derived)]
    #[serde(default = "default_port")]
    #[derivative(Default(value = "default_port()"))]
    pub port: u16,

    /// MQTT username.
    #[configurable(derived)]
    #[serde(default)]
    pub user: Option<String>,

    /// MQTT password.
    #[configurable(derived)]
    #[serde(default)]
    pub password: Option<String>,

    /// MQTT client ID. If there are multiple
    #[configurable(derived)]
    #[serde(default)]
    pub client_id: Option<String>,

    /// Share Configuration. If source support share subscription
    #[configurable(derived)]
    #[serde(default)]
    pub share: Option<Share>,

    /// Connection keep-alive interval.
    #[configurable(derived)]
    #[serde(default = "default_keep_alive")]
    #[derivative(Default(value = "default_keep_alive()"))]
    pub keep_alive: u16,

    /// MQTT topic from which messages are to be read.
    #[configurable(derived)]
    #[serde(default = "default_topic")]
    #[derivative(Default(value = "default_topic()"))]
    pub topic: String,

    #[configurable(derived)]
    #[serde(default = "max_packet_size")]
    #[derivative(Default(value = "max_packet_size()"))]
    pub max_packet_size: usize,

    #[configurable(derived)]
    #[serde(default = "default_framing_message_based")]
    #[derivative(Default(value = "default_framing_message_based()"))]
    pub framing: FramingConfig,

    #[configurable(derived)]
    pub tls: Option<TlsEnableableConfig>,
}

const fn default_port() -> u16 {
    1883
}

const fn default_keep_alive() -> u16 {
    60
}
const fn max_packet_size() -> usize {
    64 * 1024 * 1024
}
fn default_topic() -> String {
    "vector".to_owned()
}
const fn default_concurrency() -> u16 {
    16
}
fn default_group() -> String {
    String::from("shared")
}

#[async_trait::async_trait]
#[typetag::serde(name = "mqtt")]
impl SourceConfig for MqttSourceConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<crate::sources::Source> {
        let concurrency = self.concurrency();
        let connectors = (0..concurrency)
            .map(|_| self.build_connector().unwrap())
            .collect();

        let source = MqttSource::new(connectors).unwrap();
        Ok(Box::pin(source.run(cx.out, cx.shutdown)))
    }

    fn outputs(&self, _global_log_namespace: LogNamespace) -> Vec<SourceOutput> {
        vec![SourceOutput::new_metrics()]
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}

impl MqttSourceConfig {
    fn build_connector(&self) -> Result<MqttConnector, MqttError> {
        let share_config = self.share.clone().unwrap_or_else(|| Share {
            enabled: Some(false),
            group: String::from("vector"),
            concurrency: 16,
        });

        let client_id: String = match share_config.is_share() {
            true => {
                let client_id = self
                    .client_id
                    .clone()
                    .unwrap_or_else(|| String::from("vector"));
                let hash = rand::thread_rng()
                    .sample_iter(&rand_distr::Alphanumeric)
                    .take(6)
                    .map(char::from)
                    .collect::<String>();
                format!("{client_id}{hash}")
            }
            false => self.client_id.clone().unwrap_or_else(|| {
                let hash = rand::thread_rng()
                    .sample_iter(&rand_distr::Alphanumeric)
                    .take(6)
                    .map(char::from)
                    .collect::<String>();
                format!("vectorSource{hash}")
            }),
        };

        if client_id.is_empty() {
            return Err(ConfigurationError::InvalidClientId).context(ConfigurationSnafu);
        }
        let mut topic = self.topic.clone();
        let tls = MaybeTlsSettings::from_config(self.tls.as_ref(), false).context(TlsSnafu)?;
        let mut options = MqttOptions::new(client_id, &self.host, self.port);
        options.set_keep_alive(Duration::from_secs(self.keep_alive.into()));
        options.set_inflight(65535);
        options.set_max_packet_size(self.max_packet_size, self.max_packet_size);
        if share_config.is_share() {
            options.set_clean_session(true);
            topic = share_config.share_topic(&topic);
        } else {
            options.set_clean_session(false);
        }

        match (&self.user, &self.password) {
            (Some(user), Some(password)) => {
                options.set_credentials(user, password);
            }
            (None, None) => {
                // Credentials were not provided
            }
            _ => {
                // We need either both username and password, or neither. MQTT also allows for providing only password, but rumqttc does not allow that;so we cannot either.
                return Err(ConfigurationError::BadCredentials).context(ConfigurationSnafu);
            }
        }

        if let Some(tls) = tls.tls() {
            let ca = tls.authorities_pem().flatten().collect();
            let client_auth = None;
            let alpn = Some(vec!["mqtt".into()]);
            options.set_transport(Transport::Tls(TlsConfiguration::Simple {
                ca,
                client_auth,
                alpn,
            }));
        }
        MqttConnector::new(options, topic)
    }
    fn share(&self) -> Share {
        self.share.clone().unwrap_or_else(|| Share {
            enabled: Some(false),
            group: String::from("vector"),
            concurrency: 16,
        })
    }
    pub fn concurrency(&self) -> u16 {
        let share = self.share();
        match share.is_share() {
            true => share.concurrency,
            false => 1,
        }
    }
}

impl_generate_config_from_default!(MqttSourceConfig);

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<MqttSourceConfig>();
    }
}
