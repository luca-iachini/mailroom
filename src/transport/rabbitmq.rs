use std::sync::Arc;

use async_trait::async_trait;
use lapin::{
    BasicProperties,
    options::BasicPublishOptions,
    types::{AMQPValue, FieldTable, ShortString},
};
use serde::Serialize;
use tokio::sync::Mutex;

use crate::{
    Envelope,
    transport::{Sender, ToBytes},
};

/// RabbitMQ transport sender.
///
/// This sender publishes messages to a RabbitMQ exchange using a shared
/// `lapin::Channel`.
///
/// ## Design
///
/// - Messages are published to a **single exchange**
/// - The routing key is derived from the envelope headers
/// - Envelope headers are mapped to **AMQP message headers**
/// - The message payload is serialized into bytes using `ToBytes`
///
/// The channel is wrapped in `Arc<Mutex<_>>` because:
/// - `lapin::Channel` is not `Sync`
/// - `Sender::send` is async and may be called concurrently
///
/// ## Type Parameters
///
/// - `M`: message payload type (phantom, inferred from `Sender`)
pub struct RabbitMq<M> {
    /// Shared AMQP channel used for publishing.
    channel: Arc<Mutex<lapin::Channel>>,
    /// Target exchange name.
    exchange: String,
    /// Marker for the message type.
    msg: std::marker::PhantomData<M>,
}

#[async_trait]
impl<H, M> Sender<H, M> for RabbitMq<M>
where
    H: RoutingKey + RabbitMqAttributes + Send + Sync + 'static,
    M: ToBytes + Serialize + Send + Sync,
{
    type Error = lapin::Error;

    /// Publish a message to RabbitMQ.
    ///
    /// ## Mapping
    ///
    /// - `Envelope.headers.routing_key()` → AMQP routing key
    /// - `Envelope.headers.attributes()` → AMQP message headers
    /// - `Envelope.message` → message body
    ///
    /// The call waits for both:
    /// - the publish to be sent
    /// - the broker confirmation (publisher confirms)
    async fn send(&mut self, envelope: Envelope<H, M>) -> Result<(), Self::Error> {
        let mut amqp_headers = FieldTable::default();
        for (k, v) in envelope.headers.attributes() {
            amqp_headers.insert(k, v);
        }

        let properties = BasicProperties::default().with_headers(amqp_headers);

        let channel = self.channel.lock().await;
        channel
            .basic_publish(
                &self.exchange,
                envelope.headers.routing_key(),
                BasicPublishOptions::default(),
                envelope.message.to_bytes(),
                properties,
            )
            .await?
            .await?;

        Ok(())
    }
}

/// Provides the routing key used when publishing to RabbitMQ.
///
/// This trait is intentionally minimal to avoid coupling the envelope headers
/// to RabbitMQ-specific types.
pub trait RoutingKey {
    /// Return the routing key for the message.
    fn routing_key(&self) -> &str;
}

/// Provides AMQP-compatible message attributes.
///
/// Implementations should return key-value pairs that can be safely converted
/// into RabbitMQ headers.
///
/// ## Notes
///
/// - Keys must be valid AMQP short strings
/// - Values must be supported `AMQPValue`s
/// - The iterator is consumed during message publishing
pub trait RabbitMqAttributes {
    /// Iterator over AMQP header key-value pairs.
    fn attributes(&self) -> impl Iterator<Item = (ShortString, AMQPValue)>;
}
