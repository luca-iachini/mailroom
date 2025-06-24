//! Producer loop for delivering outbox messages through a transport.
//!
//! This module implements a generic *outbox producer* that:
//!
//! - Streams messages from an outbox
//! - Sends them through a [`transport::Transport`]
//! - Removes successfully delivered messages from the outbox
//! - Exposes lifecycle hooks for observability and customization
//!
//! The producer runs until:
//! - The outbox stream ends
//! - A fatal error occurs
//! - A [`CancellationToken`] is triggered

use std::marker::PhantomData;

use tokio_stream::StreamExt as _;
use tokio_util::sync::CancellationToken;
use tower::Service;

use crate::{Envelope, outbox, transport};

/// Outbox producer.
///
/// The `Producer` continuously pulls messages from an outbox and delivers them
/// using a transport. On successful delivery, messages are removed from the
/// outbox.
///
/// Generic parameters:
/// - `H`: Envelope header type
/// - `M`: Envelope message type
/// - `D`: Outbox implementation
/// - `HK`: Hook implementation for lifecycle events
/// - `T`: Transport service type
pub struct Producer<H, M, D, HK, T> {
    outbox: D,
    transport: transport::Transport<T>,
    hook: HK,
    message_marker: PhantomData<M>,
    header_marker: PhantomData<H>,
}

impl<H, M, D, T> Producer<H, M, D, DefaultProducerHook, T>
where
    D: outbox::RemoveMessages<H, M> + outbox::StreamMessages<H, M> + Send,
    T: Service<Envelope<H, M>>,
{
    /// Create a new producer with the default hook implementation.
    pub fn new(outbox: D, transport: transport::Transport<T>) -> Self {
        Self {
            outbox,
            hook: DefaultProducerHook,
            transport,
            message_marker: PhantomData,
            header_marker: PhantomData,
        }
    }
}

impl<H, M, D, HK, T> Producer<H, M, D, HK, T>
where
    M: Clone + Send + 'static,
    H: Clone + Send + 'static,
    D: outbox::RemoveMessages<H, M> + outbox::StreamMessages<H, M> + Send,
    <D as outbox::RemoveMessages<H, M>>::Error: Into<tower::BoxError>,
    <D as outbox::StreamMessages<H, M>>::Error: Into<tower::BoxError>,
    D: outbox::RemoveMessages<H, M, ID = <D as outbox::StreamMessages<H, M>>::ID>,
    HK: ProducerHook<H, M>,
    T: Service<Envelope<H, M>> + Send + Clone + 'static,
    <T as Service<Envelope<H, M>>>::Error: Into<tower::BoxError>,
    <T as Service<Envelope<H, M>>>::Future: Send,
{
    /// Replace the producer hook while keeping all other generics unchanged.
    ///
    /// This allows customizing behavior (logging, metrics, retries, etc.)
    /// without rebuilding the producer.
    pub fn with_hook<HK2: ProducerHook<H, M>>(self, hook: HK2) -> Producer<H, M, D, HK2, T> {
        Producer {
            outbox: self.outbox,
            hook,
            transport: self.transport,
            header_marker: self.header_marker,
            message_marker: self.message_marker,
        }
    }

    /// Run the producer loop.
    ///
    /// The producer:
    /// - Subscribes to the outbox message stream
    /// - Sends each message through the transport
    /// - Removes messages after successful delivery
    /// - Stops on cancellation, stream end, or fatal error
    ///
    /// The loop can be terminated gracefully using the provided
    /// [`CancellationToken`].
    #[tracing::instrument(skip(self))]
    pub async fn run(mut self, cancel: CancellationToken) -> Result<(), ProducerRunError> {
        self.hook.on_startup();

        let mut message_stream = self
            .outbox
            .messages(cancel.clone())
            .await
            .map_err(|e| ProducerRunError::outbox(e.into()))?;

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    self.hook.on_shutdown();
                    break;
                }
                message = message_stream.next() => {
                    match message {
                        Some(Ok(msg)) => {
                            self.hook.on_next_message(&msg.envelope);

                            match self.transport.send(msg.envelope.clone()).await {
                                Ok(_) => {
                                    self.hook.on_message_delivered(&msg.envelope);
                                    if let Err(e) = self.outbox.remove_messages(vec![msg]).await {
                                        self.hook.on_outbox_remove_messages_error(e.into().as_ref());
                                    }
                                },
                                Err(e) => {
                                    self.hook.on_transport_send_error(&e);
                                    return Err(ProducerRunError::transport(e));
                                }
                            }
                        }
                        Some(Err(err)) => {
                            let err = err.into();
                            self.hook.on_message_receive_error(err.as_ref());
                            return Err(ProducerRunError::outbox(err));
                        }
                        None => {
                            self.hook.on_outbox_stream_end();
                            return Ok(());
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

/// Error returned when the producer loop fails.
#[derive(Debug)]
pub struct ProducerRunError {
    context: tracing_error::SpanTrace,
    kind: ProducerRunErrorKind,
}

impl ProducerRunError {
    fn transport(error: crate::transport::TransportError) -> Self {
        Self {
            context: tracing_error::SpanTrace::capture(),
            kind: ProducerRunErrorKind::Transport(error),
        }
    }

    fn outbox(error: tower::BoxError) -> Self {
        ProducerRunError {
            context: tracing_error::SpanTrace::capture(),
            kind: ProducerRunErrorKind::Outbox(error),
        }
    }
}

/// Classification of producer runtime errors.
#[derive(Debug)]
pub enum ProducerRunErrorKind {
    /// Errors originating from the outbox.
    Outbox(tower::BoxError),
    /// Errors originating from the transport.
    Transport(crate::transport::TransportError),
}

impl std::fmt::Display for ProducerRunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            ProducerRunErrorKind::Outbox(err) => writeln!(f, "Outbox error: {}", err),
            ProducerRunErrorKind::Transport(err) => writeln!(f, "Transport error: {}", err),
        }?;
        self.context.fmt(f)
    }
}

impl std::error::Error for ProducerRunError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.kind {
            ProducerRunErrorKind::Outbox(err) => Some(err.as_ref()),
            ProducerRunErrorKind::Transport(err) => Some(err),
        }
    }
}

impl From<crate::transport::TransportError> for ProducerRunError {
    fn from(err: crate::transport::TransportError) -> Self {
        ProducerRunError {
            context: tracing_error::SpanTrace::capture(),
            kind: ProducerRunErrorKind::Transport(err),
        }
    }
}

/// Hook trait for observing producer lifecycle events.
///
/// Hooks are invoked synchronously and should avoid heavy or blocking work.
/// Typical use cases include logging, metrics, and tracing integration.
pub trait ProducerHook<H, M>: Send + Sync {
    fn on_startup(&self);
    fn on_shutdown(&self);
    fn on_next_message(&self, message: &Envelope<H, M>);
    fn on_message_receive_error(&self, error: &dyn std::error::Error);
    fn on_transport_send_error(&self, error: &dyn std::error::Error);
    fn on_message_delivered(&self, message: &Envelope<H, M>);
    fn on_outbox_remove_messages_error(&self, error: &dyn std::error::Error);
    fn on_outbox_stream_end(&self);
}

/// Default producer hook implementation.
///
/// Logs lifecycle events using `tracing`.
pub struct DefaultProducerHook;

impl<H, M> ProducerHook<H, M> for DefaultProducerHook {
    fn on_startup(&self) {
        tracing::info!("Producer is starting up");
    }

    fn on_shutdown(&self) {
        tracing::info!("Producer is shutting down");
    }

    fn on_next_message(&self, _message: &Envelope<H, M>) {
        tracing::debug!("Message received");
    }

    fn on_message_receive_error(&self, error: &dyn std::error::Error) {
        tracing::error!(?error, "Error receiving message");
    }

    fn on_transport_send_error(&self, error: &dyn std::error::Error) {
        tracing::error!(?error, "Error sending message");
    }

    fn on_message_delivered(&self, _message: &Envelope<H, M>) {
        tracing::info!("Message delivered successfully");
    }

    fn on_outbox_remove_messages_error(&self, error: &dyn std::error::Error) {
        tracing::error!(?error, "Failed to remove message from outbox");
    }

    fn on_outbox_stream_end(&self) {
        tracing::info!("Outbox stream ended");
    }
}
