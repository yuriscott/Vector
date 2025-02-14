//! `NATS-Metrics` sink
//! Publishes data using [NATS](nats.io)(Neural Autonomic Transport System).

use snafu::Snafu;
use crate::event::Metric;
use crate::nats::NatsConfigError;
use crate::sinks::util::buffer::metrics::{MetricNormalize, MetricSet};

mod config;
mod request_builder;
mod service;
mod sink;
mod collector;

#[derive(Debug, Snafu)]
enum NatsError {
    #[snafu(display("invalid encoding: {}", source))]
    Encoding {
        source: vector_lib::codecs::encoding::BuildError,
    },
    #[snafu(display("NATS Config Error: {}", source))]
    Config { source: NatsConfigError },
    #[snafu(display("NATS Connect Error: {}", source))]
    Connect { source: async_nats::ConnectError },
    #[snafu(display("NATS Server Error: {}", source))]
    ServerError { source: async_nats::Error },
    #[snafu(display("NATS Publish Error: {}", source))]
    PublishError { source: async_nats::Error },

}

#[derive(Default)]
pub struct NatsMetricNormalize;

impl MetricNormalize for NatsMetricNormalize {
    fn normalize(&mut self, state: &mut MetricSet, metric: Metric) -> Option<Metric> {
        state.make_absolute(metric)
    }
}



#[derive(Clone, Eq, Hash, PartialEq)]
struct PartitionKey {
    subject: String
}


fn default_histogram_buckets() -> Vec<f64> {
    vec![
        0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    ]
}


fn default_summary_quantiles() -> Vec<f64> {
    vec![0.5, 0.75, 0.9, 0.95, 0.99]
}
