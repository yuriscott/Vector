use super::{
    config::{NatsPublisher, NatsTowerRequestConfigDefaults},
    request_builder::{NatsMetricsEncoder, NatsMetricsRequestBuilder},
    service::{NatsResponse, NatsService},
    NatsError, NatsMetricNormalize, PartitionKey,
};
use crate::event::Metric;
use crate::nats::NatsConfigError;
use crate::sinks::nats_metrics::config::NatsMetricsSinkConfig;
use crate::sinks::prelude::*;
use crate::sinks::util::buffer::metrics::MetricSet;
use futures_util::StreamExt;
use std::sync::Arc;
use vector_lib::stream::batcher::data::BatchData;
use vector_lib::stream::batcher::limiter::ByteSizeOfItemSize;

pub(super) struct NatsMetricsEvent {
    pub(super) metric: Metric,
    pub(super) subject: String,
}

impl Finalizable for NatsMetricsEvent {
    fn take_finalizers(&mut self) -> EventFinalizers {
        self.metric.take_finalizers()
    }
}

impl GetEventCountTags for NatsMetricsEvent {
    fn get_tags(&self) -> TaggedEventsSent {
        self.metric.get_tags()
    }
}

impl EstimatedJsonEncodedSizeOf for NatsMetricsEvent {
    fn estimated_json_encoded_size_of(&self) -> JsonSize {
        self.metric.estimated_json_encoded_size_of()
    }
}

impl ByteSizeOf for NatsMetricsEvent {
    fn allocated_bytes(&self) -> usize {
        self.metric.allocated_bytes()
    }
}

pub(super) struct SubjectPartitioner;

impl Partitioner for SubjectPartitioner {
    type Item = NatsMetricsEvent;
    type Key = PartitionKey;

    fn partition(&self, item: &Self::Item) -> Self::Key {
        PartitionKey {
            subject: item.subject.clone(),
        }
    }
}

pub(super) enum BatchedMetrics {
    Aggregated(MetricSet),
    Unaggregated(Vec<Metric>),
}

impl BatchedMetrics {
    pub(super) fn into_metrics(self) -> Vec<Metric> {
        match self {
            BatchedMetrics::Aggregated(metrics) => metrics.into_metrics(),
            BatchedMetrics::Unaggregated(metrics) => metrics,
        }
    }

    pub(super) fn insert_update(&mut self, metric: Metric) {
        match self {
            BatchedMetrics::Aggregated(metrics) => metrics.insert_update(metric),
            BatchedMetrics::Unaggregated(metrics) => metrics.push(metric),
        }
    }

    pub(super) fn len(&self) -> usize {
        match self {
            BatchedMetrics::Aggregated(metrics) => metrics.len(),
            BatchedMetrics::Unaggregated(metrics) => metrics.len(),
        }
    }
}

pub(super) struct EventCollection {
    pub(super) finalizers: EventFinalizers,
    pub(super) events: BatchedMetrics,
    pub(super) events_byte_size: usize,
    pub(super) events_json_byte_size: GroupedCountByteSize,
}

impl EventCollection {
    /// Creates a new event collection that will either aggregate the incremental metrics
    /// or store all the metrics, depending on the value of the `aggregate` parameter.
    fn new(aggregate: bool) -> Self {
        Self {
            finalizers: Default::default(),
            events: if aggregate {
                BatchedMetrics::Aggregated(Default::default())
            } else {
                BatchedMetrics::Unaggregated(Default::default())
            },
            events_byte_size: Default::default(),
            events_json_byte_size: telemetry().create_request_count_byte_size(),
        }
    }

    const fn is_aggregated(&self) -> bool {
        matches!(self.events, BatchedMetrics::Aggregated(_))
    }
}

impl BatchData<NatsMetricsEvent> for EventCollection {
    type Batch = Self;

    fn len(&self) -> usize {
        self.events.len()
    }

    fn take_batch(&mut self) -> Self::Batch {
        let mut new = Self::new(self.is_aggregated());
        std::mem::swap(self, &mut new);
        new
    }

    fn push_item(&mut self, mut item: NatsMetricsEvent) {
        self.finalizers
            .merge(item.metric.metadata_mut().take_finalizers());
        self.events_byte_size += item.size_of();
        self.events_json_byte_size
            .add_event(&item.metric, item.estimated_json_encoded_size_of());
        self.events.insert_update(item.metric);
    }
}

pub(super) struct NatsMetricsSink {
    request: TowerRequestConfig<NatsTowerRequestConfigDefaults>,
    // encoder: Encoder<()>,
    publisher: Arc<NatsPublisher>,
    subject: Template,
    aggregate: bool,
    default_namespace: Option<String>,
    buckets: Vec<f64>,
    quantiles: Vec<f64>,
    batch_settings: BatcherSettings,
    compression: Compression
}

impl NatsMetricsSink {
    fn make_nats_event(&self, metric: Metric) -> Option<NatsMetricsEvent> {
        let subject = self
            .subject
            .render_string(&metric)
            .map_err(|missing_keys| {
                emit!(TemplateRenderingError {
                    error: missing_keys,
                    field: Some("subject"),
                    drop_event: true,
                });
            })
            .ok()?;
        Some(NatsMetricsEvent { metric, subject })
    }

    pub(super) async fn new(config: NatsMetricsSinkConfig) -> Result<Self, NatsError> {
        let publisher = Arc::new(config.publisher().await?);
        // let transformer = config.encoding.transformer();
        // let serializer = config.encoding.build().context(EncodingSnafu)?;
        // let encoder = Encoder::<()>::new(serializer);
        let request = config.request.clone();
        let subject = config.subject.clone();
        let default_namespace = config.default_namespace.clone();
        let buckets = config.buckets.clone();
        let quantiles = config.quantiles.clone();
        let aggregate = config.batch.aggregate;
        let batch_settings = config
            .batch
            .batch_settings
            .validate()
            .map_err(|_e| NatsError::Config {
                source: NatsConfigError::CommonConfigError {},
            })?
            .into_batcher_settings()
            .map_err(|_e| NatsError::Config {
                source: NatsConfigError::CommonConfigError {},
            })?;
        let compression=config.compression.clone();
        Ok(NatsMetricsSink {
            request,
            // encoder,
            publisher,
            subject,
            aggregate,
            default_namespace,
            buckets,
            quantiles,
            batch_settings,
            compression
        })
    }

    async fn run_inner(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        let request = self.request.into_settings();

        let request_builder = NatsMetricsRequestBuilder {
            encoder: NatsMetricsEncoder {
                default_namespace: self.default_namespace.clone(),
                buckets: self.buckets.clone(),
                quantiles: self.quantiles.clone(),
            },
            compression: self.compression.clone(),
        };

        let batch_settings = self.batch_settings;
        let service = ServiceBuilder::new()
            .settings(request, NatsRetryLogic)
            .service(NatsService {
                publisher: Arc::clone(&self.publisher),
            });
        let aggragate = self.aggregate;
        input
            .filter_map(|event| future::ready(event.try_into_metric()))
            .normalized_with_default::<NatsMetricNormalize>()
            .filter_map(move |event| std::future::ready(self.make_nats_event(event)))
            .batched_partitioned(SubjectPartitioner, || {
                batch_settings
                    .as_reducer_config(ByteSizeOfItemSize, EventCollection::new(aggragate))
            })
            .request_builder(default_request_builder_concurrency_limit(), request_builder)
            .filter_map(|request| async move {
                match request {
                    Err(e) => {
                        error!("Failed to build Nats Metrics request: {:?}.", e);
                        None
                    }
                    Ok(req) => Some(req),
                }
            })
            .into_driver(service)
            .protocol("nats")
            .run()
            .await
    }
}

#[async_trait]
impl StreamSink<Event> for NatsMetricsSink {
    async fn run(mut self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        self.run_inner(input).await
    }
}

#[derive(Debug, Clone)]
pub(super) struct NatsRetryLogic;

impl RetryLogic for NatsRetryLogic {
    type Error = NatsError;
    type Response = NatsResponse;

    fn is_retriable_error(&self, _error: &Self::Error) -> bool {
        true
    }
}
