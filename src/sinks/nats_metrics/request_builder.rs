use std::io;

use super::sink::EventCollection;
use crate::event::Metric;
use crate::sinks::nats_metrics::collector::MetricCollector;
use crate::sinks::nats_metrics::{collector, PartitionKey};
use crate::sinks::prelude::*;
use bytes::{Bytes, BytesMut};
use prost::Message;
use vector_lib::config::telemetry;

pub(super) struct NatsMetricsEncoder {
    pub(super) default_namespace: Option<String>,
    pub(super) buckets: Vec<f64>,
    pub(super) quantiles: Vec<f64>,
}

impl encoding::Encoder<Vec<Metric>> for NatsMetricsEncoder {
    fn encode_input(
        &self,
        input: Vec<Metric>,
        writer: &mut dyn io::Write,
    ) -> io::Result<(usize, GroupedCountByteSize)> {
        let mut byte_size = telemetry().create_request_count_byte_size();

        let mut time_series = collector::TimeSeries::new();
        let len = input.len();
        for metric in input {
            byte_size.add_event(&metric, metric.estimated_json_encoded_size_of());

            time_series.encode_metric(
                self.default_namespace.as_deref(),
                &self.buckets,
                &self.quantiles,
                &metric,
            );
        }
        let request = time_series.finish();

        let mut out = BytesMut::with_capacity(request.encoded_len());
        request.encode(&mut out).expect("Out of memory");
        let body = out.freeze();

        write_all(writer, len, body.as_ref())?;

        Ok((body.len(), byte_size))
    }
}

pub(super) struct NatsMetricsMetadata {
    subject: String,
    finalizers: EventFinalizers,
}

pub(super) struct NatsMetricsRequestBuilder {
    pub(super) encoder: NatsMetricsEncoder,
    pub(super) compression: Compression,
}

#[derive(Clone)]
pub(super) struct NatsMetricsRequest {
    pub(super) bytes: Bytes,
    pub(super) subject: String,
    pub(super) finalizers: EventFinalizers,
    pub(super) metadata: RequestMetadata,
}

impl Finalizable for NatsMetricsRequest {
    fn take_finalizers(&mut self) -> EventFinalizers {
        std::mem::take(&mut self.finalizers)
    }
}

impl MetaDescriptive for NatsMetricsRequest {
    fn get_metadata(&self) -> &RequestMetadata {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut RequestMetadata {
        &mut self.metadata
    }
}

impl RequestBuilder<(PartitionKey, EventCollection)> for NatsMetricsRequestBuilder {
    type Metadata = NatsMetricsMetadata;
    type Events = Vec<Metric>;
    type Encoder = NatsMetricsEncoder;
    type Payload = Bytes;
    type Request = NatsMetricsRequest;
    type Error = io::Error;

    fn compression(&self) -> Compression {
        self.compression
    }

    fn encoder(&self) -> &Self::Encoder {
        &self.encoder
    }

    fn split_input(
        &self,
        input: (PartitionKey, EventCollection),
    ) -> (Self::Metadata, RequestMetadataBuilder, Self::Events) {
        let (key, events) = input;
        let finalizers = events.finalizers;
        let metrics = events.events.into_metrics();
        let builder = RequestMetadataBuilder::from_events(&metrics);

        let metadata = NatsMetricsMetadata {
            subject: key.subject,
            finalizers,
        };

        (metadata, builder, metrics)
    }

    fn build_request(
        &self,
        nats_metadata: Self::Metadata,
        metadata: RequestMetadata,
        payload: EncodeResult<Self::Payload>,
    ) -> Self::Request {
        let body = payload.into_payload();
        NatsMetricsRequest {
            bytes: body,
            subject: nats_metadata.subject,
            finalizers: nats_metadata.finalizers,
            metadata,
        }
    }
}
