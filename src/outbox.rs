//! Outbox abstractions and backend drivers.
//!
//! This module implements the *outbox pattern*, providing a reliable way to
//! persist messages before they are delivered asynchronously by a producer.
//!
//! The outbox is responsible for **durability and ordering**, while delivery
//! concerns are delegated to the producer and transport layers.
//!
//! ## Responsibilities
//!
//! - Persist messages atomically with application state
//! - Stream pending messages for delivery
//! - Remove messages after successful delivery
//!
//! ## Components
//!
//! - [`Outbox`]: High-level façade over an outbox backend
//! - [`InsertMessages`]: Trait for inserting messages
//! - [`StreamMessages`]: Trait for streaming pending messages
//! - [`RemoveMessages`]: Trait for deleting delivered messages
//!
//! Concrete implementations are provided by backend modules such as
//! [`inmemory`] and [`sqlx`] (feature-gated).

pub mod inmemory;
pub mod poller;

#[cfg(feature = "sqlx")]
pub mod sqlx;

use futures_core::stream::BoxStream;
use tokio_util::sync::CancellationToken;
use tracing::instrument;
use tracing_error::SpanTrace;

use crate::Envelope;

/// Error returned by outbox operations.
///
/// Wraps the underlying backend error and captures a tracing span backtrace
/// for improved diagnostics.
#[derive(Debug)]
pub struct OutboxError {
    context: SpanTrace,
    source: tower::BoxError,
}

impl OutboxError {
    /// Create a backend-related outbox error.
    fn backend(err: Box<dyn std::error::Error + Send + Sync>) -> Self {
        Self {
            context: SpanTrace::capture(),
            source: err,
        }
    }
}

impl std::fmt::Display for OutboxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Backend error: {}", self.source)?;
        self.context.fmt(f)
    }
}

impl std::error::Error for OutboxError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

/// Message stored in the outbox.
///
/// Each outbox message consists of:
/// - A backend-generated identifier
/// - The original [`Envelope`] payload
#[derive(Debug, PartialEq)]
pub struct OutboxMessage<ID, H, M> {
    pub(crate) id: ID,
    pub(crate) envelope: Envelope<H, M>,
}

/// High-level façade over an outbox backend.
///
/// `Outbox` provides a stable, ergonomic API for publishing messages while
/// delegating persistence and streaming behavior to the underlying backend.
pub struct Outbox<D>(D);

impl<D> Outbox<D>
where
    D: Clone,
{
    /// Create a new outbox backed by the given backend implementation.
    pub fn new(driver: D) -> Self {
        Self(driver)
    }

    /// Publish messages into the outbox.
    ///
    /// Messages are inserted into the outbox but **not** sent immediately.
    /// Delivery is handled asynchronously by a producer.
    ///
    /// This method is typically called within the same transaction that
    /// mutates application state.
    #[instrument(skip(self, msgs, tx))]
    pub async fn publish_messages<H, M>(
        &self,
        msgs: impl IntoIterator<Item = impl Into<Envelope<H, M>>>,
        tx: &mut D::Transaction<'_>,
    ) -> Result<(), OutboxError>
    where
        D: InsertMessages<H, M>,
        <D as InsertMessages<H, M>>::Error: Into<tower::BoxError>,
    {
        let msgs: Vec<Envelope<H, M>> = msgs.into_iter().map(Into::into).collect();

        self.0
            .insert_messages(msgs, tx)
            .await
            .map_err(|e| OutboxError::backend(e.into()))
    }
}

/// Trait for inserting messages into the outbox.
///
/// Implementations must ensure durability and transactional guarantees.
#[async_trait::async_trait]
pub trait InsertMessages<H, M> {
    /// Backend-specific error type.
    type Error;
    /// Identifier type assigned to stored messages.
    type ID;
    /// Transaction type used for atomic insertion.
    type Transaction<'a>;

    /// Insert a batch of messages into the outbox.
    async fn insert_messages(
        &self,
        msgs: Vec<Envelope<H, M>>,
        tx: &mut Self::Transaction<'_>,
    ) -> Result<(), Self::Error>;
}

/// Trait for removing delivered messages from the outbox.
#[async_trait::async_trait]
pub trait RemoveMessages<H, M> {
    /// Backend-specific error type.
    type Error;
    /// Identifier type for stored messages.
    type ID;

    /// Remove messages that have been successfully delivered.
    async fn remove_messages(
        &self,
        msg: Vec<OutboxMessage<Self::ID, H, M>>,
    ) -> Result<(), Self::Error>;
}

/// Trait for streaming pending messages from the outbox.
///
/// The returned stream should:
/// - Yield messages in delivery order
/// - Respect cancellation via the provided [`CancellationToken`]
#[async_trait::async_trait]
pub trait StreamMessages<H, M> {
    /// Backend-specific error type.
    type Error;
    /// Identifier type for stored messages.
    type ID;

    /// Stream pending messages until exhaustion or cancellation.
    async fn messages(
        &self,
        cancel: CancellationToken,
    ) -> Result<BoxStream<'_, Result<OutboxMessage<Self::ID, H, M>, Self::Error>>, Self::Error>;
}
