//! Transport abstractions and sender backends.
//!
//! This module defines a Tower-compatible transport layer used to send
//! [`Envelope`]s through different backends (e.g. in-memory, Kafka, RabbitMQ).
//!
//! The transport is built around Tower’s `Service` abstraction, enabling
//! middleware composition (retries, tracing, buffering, etc.) while keeping
//! sender implementations backend-agnostic.
//!
//! ## Key components
//!
//! - [`Transport`]: Public-facing wrapper implementing `tower::Service`
//! - [`SenderService`]: Adapter from a [`Sender`] to a Tower service
//! - [`Sender`]: Trait implemented by concrete sender backends
//! - [`TransportError`]: Unified error type with tracing context

mod inmemory;

#[cfg(feature = "kafka")]
pub mod kafka;

pub mod layers;

#[cfg(feature = "rabbitmq")]
pub mod rabbitmq;

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use tower::Service;
use tracing_error::SpanTrace;

use crate::Envelope;

pub use inmemory::InMemory;

/// Generic Tower-compatible transport wrapper.
///
/// `Transport` is the main entry point for sending envelopes. It wraps an
/// underlying Tower `Service` and:
///
/// - Normalizes errors into [`TransportError`]
/// - Supports Tower middleware via layers
/// - Provides a convenience [`send`](Transport::send) API
///
/// Typically constructed from a concrete [`Sender`] implementation.
#[derive(Clone)]
pub struct Transport<S> {
    service: S,
}

impl<D> Transport<SenderService<D>> {
    /// Create a new transport from a concrete sender backend.
    ///
    /// The sender will be wrapped in a [`SenderService`] to make it
    /// Tower-compatible.
    pub fn new(driver: D) -> Self {
        Self {
            service: SenderService::new(driver),
        }
    }
}

impl<S> Transport<S> {
    /// Apply a Tower layer to the transport.
    ///
    /// This enables composition with middleware such as retries, timeouts,
    /// buffering, or tracing.
    pub fn layer<L>(self, layer: L) -> Transport<L::Service>
    where
        L: tower::Layer<S>,
    {
        Transport {
            service: layer.layer(self.service),
        }
    }
}

/// Tower `Service` implementation for `Transport`.
///
/// Delegates readiness and request handling to the inner service while mapping
/// all errors into [`TransportError`].
impl<R, S> Service<R> for Transport<S>
where
    S: Service<R> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<tower::BoxError>,
    R: Send + 'static,
{
    type Response = ();
    type Error = TransportError;
    type Future = Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service
            .poll_ready(cx)
            .map_err(|e| TransportError::sender(e.into()))
    }

    fn call(&mut self, req: R) -> Self::Future {
        let mut service = self.service.clone();

        Box::pin(async move {
            service
                .call(req)
                .await
                .map_err(|e| TransportError::sender(e.into()))?;
            Ok(())
        })
    }
}

impl<S> Transport<S> {
    /// Send an [`Envelope`] through the transport.
    ///
    /// This is a convenience method for users that do not need direct access
    /// to the `tower::Service` API.
    pub async fn send<H, M>(&mut self, envelope: Envelope<H, M>) -> Result<(), TransportError>
    where
        M: Clone + Send + 'static,
        S: Service<Envelope<H, M>> + Clone + Send + 'static,
        S::Future: Send + 'static,
        S::Error: Into<tower::BoxError>,
    {
        let mut service = self.service.clone();
        service
            .call(envelope)
            .await
            .map_err(|e| TransportError::sender(e.into()))?;
        Ok(())
    }
}

/// Error returned by transport operations.
///
/// Each error captures:
/// - The underlying error kind
/// - A tracing span backtrace for improved diagnostics
#[derive(Debug)]
pub struct TransportError {
    context: SpanTrace,
    kind: TransportErrorKind,
}

/// Transport errors kind.
#[derive(Debug)]
pub enum TransportErrorKind {
    /// Errors originating from the sender backend.
    Sender(tower::BoxError),
    /// Errors related to serialization or deserialization.
    Serde(tower::BoxError),
}

impl TransportError {
    /// Create a sender-related transport error.
    pub fn sender(err: Box<dyn std::error::Error + Send + Sync>) -> Self {
        Self {
            context: SpanTrace::capture(),
            kind: TransportErrorKind::Sender(err),
        }
    }

    /// Create a serialization-related transport error.
    pub fn serde(err: Box<dyn std::error::Error + Send + Sync>) -> Self {
        Self {
            context: SpanTrace::capture(),
            kind: TransportErrorKind::Serde(err),
        }
    }
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            TransportErrorKind::Sender(err) => writeln!(f, "Sender error: {err}"),
            TransportErrorKind::Serde(err) => writeln!(f, "Serde error: {err}"),
        }?;
        self.context.fmt(f)
    }
}

impl std::error::Error for TransportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.kind {
            TransportErrorKind::Sender(err) => Some(err.as_ref()),
            TransportErrorKind::Serde(err) => Some(err.as_ref()),
        }
    }
}

/// Tower service adapter for a [`Sender`] backend.
///
/// This type bridges the [`Sender`] trait with Tower’s `Service` abstraction.
#[derive(Clone)]
pub struct SenderService<D> {
    sender: D,
}

impl<D> SenderService<D> {
    /// Create a new sender service from a backend.
    pub fn new(sender: D) -> Self {
        Self { sender }
    }
}

/// `tower::Service` implementation delegating to a [`Sender`].
impl<H, M, D> Service<Envelope<H, M>> for SenderService<D>
where
    H: Clone + Send + 'static,
    M: Clone + Send + 'static,
    D: Sender<H, M> + Clone + Send + 'static,
{
    type Response = ();
    type Error = tower::BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Envelope<H, M>) -> Self::Future {
        let mut sender = self.sender.clone();
        Box::pin(async move {
            sender.send(req).await.map_err(Into::into)?;
            Ok(())
        })
    }
}

/// Trait implemented by concrete sender backends.
///
/// A sender is responsible for delivering an [`Envelope`] to an external
/// system (e.g. Kafka, RabbitMQ, or an in-memory channel).
#[async_trait::async_trait]
pub trait Sender<H, M> {
    /// Backend-specific error type.
    type Error: Into<tower::BoxError>;

    /// Send an envelope using the underlying transport.
    async fn send(&mut self, envelope: Envelope<H, M>) -> Result<(), Self::Error>;
}

/// Wrapper type for raw byte payloads.
#[derive(Debug, Clone)]
pub struct RawPayload(Vec<u8>);

impl From<Vec<u8>> for RawPayload {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

/// Trait for values that can be viewed as raw bytes.
///
/// This abstraction avoids unnecessary allocations when handling payloads.
pub trait ToBytes {
    /// Convert the value into a byte slice.
    fn to_bytes(&self) -> &[u8];
}

impl ToBytes for [u8] {
    fn to_bytes(&self) -> &[u8] {
        self
    }
}

impl ToBytes for str {
    fn to_bytes(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl ToBytes for Vec<u8> {
    fn to_bytes(&self) -> &[u8] {
        self.as_slice()
    }
}

impl ToBytes for String {
    fn to_bytes(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl<T: ToBytes> ToBytes for &T {
    fn to_bytes(&self) -> &[u8] {
        (*self).to_bytes()
    }
}

impl ToBytes for () {
    fn to_bytes(&self) -> &[u8] {
        &[]
    }
}

impl<const N: usize> ToBytes for [u8; N] {
    fn to_bytes(&self) -> &[u8] {
        self
    }
}

impl ToBytes for RawPayload {
    fn to_bytes(&self) -> &[u8] {
        self.0.to_bytes()
    }
}
