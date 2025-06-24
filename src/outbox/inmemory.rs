use async_trait::async_trait;
use futures_core::stream::BoxStream;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::outbox::{Envelope, InsertMessages, OutboxMessage, RemoveMessages, StreamMessages};

/// An in-memory outbox for testing or local usage.
///
/// Stores messages in a `HashMap` and supports insert, remove, and stream operations.
#[derive(Clone)]
pub struct InMemoryOutbox<H, M> {
    messages: Arc<Mutex<HashMap<String, Envelope<H, M>>>>,
}

impl<H, M> Default for InMemoryOutbox<H, M> {
    fn default() -> Self {
        Self {
            messages: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl<H, M> InsertMessages<H, M> for InMemoryOutbox<H, M>
where
    H: Send + Clone + MessageId,
    M: Send + Clone,
{
    type Error = InMemoryOutboxError;
    type Transaction<'a> = ();
    type ID = String;

    /// Insert messages into the in-memory store.
    async fn insert_messages(
        &self,
        envelopes: Vec<Envelope<H, M>>,
        _tx: &mut Self::Transaction<'_>,
    ) -> Result<(), InMemoryOutboxError> {
        for e in envelopes {
            let id = e.headers.id();
            self.messages.lock().await.insert(id.to_owned(), e);
        }
        Ok(())
    }
}

#[async_trait]
impl<H, M> RemoveMessages<H, M> for InMemoryOutbox<H, M>
where
    H: Send,
    M: Send,
{
    type Error = InMemoryOutboxError;
    type ID = String;

    /// Remove messages from the in-memory store by ID.
    async fn remove_messages(
        &self,
        messages: Vec<OutboxMessage<Self::ID, H, M>>,
    ) -> Result<(), InMemoryOutboxError> {
        for msg in messages {
            self.messages
                .lock()
                .await
                .remove(&msg.id)
                .ok_or(InMemoryOutboxError::not_found())?;
        }
        Ok(())
    }
}

#[async_trait]
impl<H, M> StreamMessages<H, M> for InMemoryOutbox<H, M>
where
    H: Send + Clone,
    M: Send + Clone,
{
    type Error = InMemoryOutboxError;
    type ID = String;

    /// Stream all messages currently in the in-memory store.
    ///
    /// Messages are sorted by ID.
    async fn messages(
        &self,
        _cancel: CancellationToken,
    ) -> Result<BoxStream<'_, Result<OutboxMessage<Self::ID, H, M>, Self::Error>>, Self::Error>
    {
        let messages = self.messages.lock().await;
        let mut messages: Vec<_> = messages
            .iter()
            .map(|(k, e)| OutboxMessage {
                id: k.clone(),
                envelope: e.clone(),
            })
            .collect();
        messages.sort_by_key(|m| m.id.clone());
        let messages: Vec<_> = messages.into_iter().map(Ok).collect();
        Ok(Box::pin(tokio_stream::iter(messages)))
    }
}

/// Error type for `InMemoryOutbox` operations.
#[derive(Debug)]
pub struct InMemoryOutboxError {
    kind: InMemoryOutboxErrorKind,
}

impl InMemoryOutboxError {
    fn not_found() -> Self {
        Self {
            kind: InMemoryOutboxErrorKind::NotFound,
        }
    }
}

impl std::fmt::Display for InMemoryOutboxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            InMemoryOutboxErrorKind::NotFound => write!(f, "Message not found in in-memory driver"),
        }
    }
}

impl std::error::Error for InMemoryOutboxError {}

#[derive(Debug)]
enum InMemoryOutboxErrorKind {
    NotFound,
}

/// Trait for extracting a unique message ID from headers.
pub trait MessageId {
    fn id(&self) -> String;
}

impl MessageId for String {
    fn id(&self) -> String {
        self.clone()
    }
}

impl MessageId for &str {
    fn id(&self) -> String {
        self.to_string()
    }
}
