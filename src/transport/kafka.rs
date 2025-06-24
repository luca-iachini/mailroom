use async_trait::async_trait;
use rdkafka::{
    message::OwnedHeaders,
    producer::{FutureProducer, FutureRecord},
};
use serde::Serialize;
use std::{marker::PhantomData, time::Duration};

use crate::{
    Envelope,
    transport::{Sender, ToBytes},
};

/// Kafka transport sender.
///
/// This sender publishes messages to Kafka topics using a `FutureProducer`.
/// It supports per-message keys, headers, and topic selection based on
/// envelope headers.
///
/// ## Type Parameters
///
/// - `M`: message payload type (phantom, inferred from `Sender`)
pub struct Kafka<M> {
    /// Kafka producer handle
    producer: FutureProducer,
    /// Timeout for sending messages
    timeout: Duration,
    /// Phantom type for message
    _msg: PhantomData<M>,
}

impl<M> Kafka<M> {
    /// Create a new Kafka sender using the given `FutureProducer`.
    ///
    /// Default timeout is 5 seconds.
    pub fn new(producer: FutureProducer) -> Self {
        Self {
            producer,
            timeout: Duration::from_secs(5),
            _msg: PhantomData,
        }
    }

    /// Set a custom timeout for sending messages.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

#[async_trait]
impl<H, M> Sender<H, M> for Kafka<M>
where
    H: KafkaKey + KafkaHeaders + KafkaTopic + Send + Sync + 'static,
    M: ToBytes + Serialize + Send + Sync,
{
    type Error = rdkafka::error::KafkaError;

    /// Send a message to Kafka.
    ///
    /// Maps the `Envelope` fields as follows:
    /// - `Envelope.headers.topic()` → Kafka topic
    /// - `Envelope.headers.key()` → Kafka message key
    /// - `Envelope.headers.headers()` → Kafka message headers
    /// - `Envelope.message` → message payload
    ///
    /// Uses the configured timeout for the send operation.
    async fn send(&mut self, envelope: Envelope<H, M>) -> Result<(), Self::Error> {
        let record = FutureRecord::to(envelope.headers.topic())
            .payload(envelope.message.to_bytes())
            .key(envelope.headers.key())
            .headers(envelope.headers.headers());

        self.producer
            .send(record, self.timeout)
            .await
            .map_err(|(e, _)| e)?;

        Ok(())
    }
}

/// Provides the message key for Kafka.
///
/// Keys are used for partitioning and ordering in Kafka topics.
pub trait KafkaKey {
    /// Return the key as a byte slice.
    fn key(&self) -> &[u8];
}

/// Provides optional message headers for Kafka.
///
/// Headers can carry metadata that consumers can access.
pub trait KafkaHeaders {
    /// Return the Kafka headers.
    fn headers(&self) -> OwnedHeaders;
}

/// Provides the target Kafka topic.
///
/// Typically, topics are static string literals.
pub trait KafkaTopic {
    /// Return the Kafka topic name.
    fn topic(&self) -> &'static str;
}
