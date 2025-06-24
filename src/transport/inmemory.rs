use std::{marker::PhantomData, sync::Arc};

use tokio::sync::Mutex;

use crate::{Envelope, transport::Sender};

/// In-memory transport for testing or local pipelines.
///
/// This transport stores messages in memory in a shared queue and implements
/// the `Sender` trait. It is useful for:
/// - Unit and integration testing
/// - Simulating message delivery without a real broker
/// - Debugging message flows
///
/// ## Type Parameters
///
/// - `H`: type of the message headers
/// - `M`: type of the message payload
pub struct InMemory<H, M> {
    /// Shared message queue
    msg_queue: Arc<Mutex<Vec<Envelope<H, M>>>>,
    /// Marker for headers
    _header_marker: PhantomData<H>,
    /// Marker for message payload
    _message_marker: PhantomData<M>,
}

impl<H, M> InMemory<H, M> {
    /// Return all messages that have been "sent" and clears the internal queue.
    ///
    /// This consumes the internal queue and is primarily intended for testing
    /// purposes.
    pub async fn sent_messages(self) -> Vec<Envelope<H, M>> {
        let mut queue = self.msg_queue.lock_owned().await;
        std::mem::take(&mut *queue)
    }
}

impl<M, H> Clone for InMemory<H, M> {
    fn clone(&self) -> Self {
        Self {
            msg_queue: Arc::clone(&self.msg_queue),
            _header_marker: self._header_marker,
            _message_marker: self._message_marker,
        }
    }
}

impl<H, M> Default for InMemory<H, M> {
    /// Create a new empty in-memory transport.
    fn default() -> Self {
        Self {
            msg_queue: Arc::new(Mutex::new(Vec::new())),
            _header_marker: PhantomData,
            _message_marker: PhantomData,
        }
    }
}

#[async_trait::async_trait]
impl<H, M> Sender<H, M> for InMemory<H, M>
where
    H: Clone + std::fmt::Debug + Send,
    M: Clone + std::fmt::Debug + Send,
{
    type Error = std::io::Error;

    /// "Send" a message by appending it to the in-memory queue.
    ///
    /// Logs the headers and payload using `tracing::info`.
    #[tracing::instrument(skip_all)]
    async fn send(&mut self, envelope: Envelope<H, M>) -> Result<(), Self::Error> {
        let mut queue = self.msg_queue.lock().await;
        queue.push(envelope.clone());
        tracing::info!(
            headers = ?envelope.headers,
            msg = ?envelope.message,
            "Message sent to in-memory queue",
        );
        Ok(())
    }
}
