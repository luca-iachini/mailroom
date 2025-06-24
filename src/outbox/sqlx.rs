use ahash::AHasher;
use async_stream::stream;
use async_trait::async_trait;
use futures_core::stream::BoxStream;
use serde::{Serialize, de::DeserializeOwned};
use sqlx::{Database, Pool, Row};
use std::hash::Hasher;
use std::{marker::PhantomData, time::Duration};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::outbox::{
    Envelope, InsertMessages, OutboxMessage, RemoveMessages, StreamMessages, poller::PollerBuilder,
};

/// Type alias for Postgres SqlxOutbox.
pub type PgSqlxOutbox<H, M> = SqlxOutbox<H, M, sqlx::Postgres>;
/// Type alias for Postgres SqlxOutbox poller.
pub type PgSqlxOutboxPoller<H, M> = SqlxOutboxPoller<H, M, sqlx::Postgres>;

/// SQLx-based outbox driver.
pub struct SqlxOutbox<H, M, DB>
where
    DB: Database,
{
    pool: Pool<DB>,
    _header_marker: PhantomData<H>,
    _message_marker: PhantomData<M>,
}

impl<H, M, DB> Clone for SqlxOutbox<H, M, DB>
where
    DB: Database,
{
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            _header_marker: PhantomData,
            _message_marker: PhantomData,
        }
    }
}

impl<H, M, DB> SqlxOutbox<H, M, DB>
where
    DB: Database,
{
    /// Creates a new uninitialized outbox with a SQLx pool.
    pub fn new_uninitialized(pool: Pool<DB>) -> Self {
        Self {
            pool,
            _header_marker: PhantomData,
            _message_marker: PhantomData,
        }
    }
}

impl<H, M> SqlxOutbox<H, M, sqlx::Postgres> {
    /// Creates a new Postgres outbox and ensures the table exists.
    #[tracing::instrument(skip_all)]
    pub async fn try_new(pool: sqlx::Pool<sqlx::Postgres>) -> Result<Self, Error> {
        create_table(&pool).await?;
        Ok(Self::new_uninitialized(pool))
    }
}

/// SQLx `InsertMessages` driver implementation.
#[async_trait]
impl<H, M> InsertMessages<H, M> for SqlxOutbox<H, M, sqlx::Postgres>
where
    H: Serialize + PartitionKey + Send + Sync + 'static,
    M: Serialize + Send + Sync + 'static,
{
    type Error = tower::BoxError;
    type Transaction<'a> = sqlx::PgTransaction<'a>;
    type ID = i64;

    #[tracing::instrument(skip_all)]
    async fn insert_messages(
        &self,
        envelopes: Vec<Envelope<H, M>>,
        tx: &mut Self::Transaction<'_>,
    ) -> Result<(), Self::Error> {
        for envelope in envelopes {
            let key = envelope.headers.partition_key();
            let partition = calculate_partition(&key);

            let message = serde_json::to_value(&envelope.message)?;
            let headers = serde_json::to_value(&envelope.headers)?;

            sqlx::query("INSERT INTO outbox (partition, message, headers) VALUES ($1, $2, $3)")
                .bind(partition)
                .bind(message)
                .bind(headers)
                .execute(&mut **tx)
                .await?;
        }
        Ok(())
    }
}

/// Represents a partition configuration.
#[derive(Debug, Clone, Copy)]
pub struct Partition {
    pub id: u32,
    pub total: u32,
}

/// SQLx-based poller to fetch outbox messages from the database.
pub struct SqlxOutboxPoller<H, M, DB>
where
    DB: Database,
{
    pool: Pool<DB>,
    poll_interval: Duration,
    fetch_size: usize,
    logical_delete: bool,
    partition: Partition,
    _header_marker: PhantomData<H>,
    _message_marker: PhantomData<M>,
}

impl<H, M, DB> SqlxOutboxPoller<H, M, DB>
where
    DB: Database,
{
    /// Creates a new uninitialized poller.
    pub fn new_uninitialized(pool: Pool<DB>, poll_interval: Duration) -> Self {
        Self {
            pool,
            poll_interval,
            fetch_size: 1000,
            logical_delete: true,
            partition: Partition { id: 0, total: 1 },
            _header_marker: PhantomData,
            _message_marker: PhantomData,
        }
    }

    /// Sets the fetch size for batch queries.
    pub fn with_fetch_size(mut self, size: usize) -> Self {
        self.fetch_size = size;
        self
    }

    /// Uses permanent deletion instead of logical deletion.
    pub fn with_permanent_delete(mut self) -> Self {
        self.logical_delete = false;
        self
    }

    /// Configures the poller partitioning.
    pub fn with_partitions(mut self, total: u32, partition_id: u32) -> Self {
        self.partition = Partition {
            total,
            id: partition_id,
        };
        self
    }
}

impl<H, M> SqlxOutboxPoller<H, M, sqlx::Postgres> {
    #[tracing::instrument(skip_all)]
    pub async fn try_new(
        pool: sqlx::Pool<sqlx::Postgres>,
        poller_interval: Duration,
    ) -> Result<Self, Error> {
        create_table(&pool).await?;
        Ok(Self::new_uninitialized(pool, poller_interval))
    }
}

impl<H, M, DB> Clone for SqlxOutboxPoller<H, M, DB>
where
    DB: Database,
{
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            poll_interval: self.poll_interval,
            fetch_size: self.fetch_size,
            logical_delete: self.logical_delete,
            partition: self.partition,
            _header_marker: PhantomData,
            _message_marker: PhantomData,
        }
    }
}

/// Fetches messages from the database according to partitioning.
impl<H, M> SqlxOutboxPoller<H, M, sqlx::Postgres>
where
    H: DeserializeOwned + Send + Sync + 'static,
    M: DeserializeOwned + Send + Sync + 'static,
{
    #[tracing::instrument(skip_all, fields(partition = ?self.partition))]
    async fn fetch_from_db(
        &self,
        fetch_size: usize,
    ) -> Result<Vec<OutboxMessage<i64, H, M>>, Error> {
        let mut rows = sqlx::query(
            "SELECT outbox_id, message, headers FROM outbox WHERE deleted = FALSE AND (partition % $1 = $2) ORDER BY outbox_id LIMIT $3",
        )
        .bind(self.partition.total as i32)
        .bind(self.partition.id as i32)
        .bind(fetch_size as i64)
        .fetch(&self.pool);

        let mut out = Vec::new();

        while let Some(row) = rows.try_next().await? {
            let id: i64 = row.try_get("outbox_id")?;
            let message_json: serde_json::Value = row.try_get("message")?;
            let headers_json: serde_json::Value = row.try_get("headers")?;

            let message = serde_json::from_value(message_json)?;
            let headers = serde_json::from_value(headers_json)?;

            out.push(OutboxMessage {
                id,
                envelope: Envelope { headers, message },
            });
        }

        Ok(out)
    }
}

/// Stream messages continuously using the poller.
#[async_trait::async_trait]
impl<H, M> StreamMessages<H, M> for SqlxOutboxPoller<H, M, sqlx::Postgres>
where
    H: Serialize + DeserializeOwned + Send + Sync + 'static,
    M: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    type Error = tower::BoxError;
    type ID = i64;

    #[tracing::instrument(skip_all)]
    async fn messages(
        &self,
        cancel: CancellationToken,
    ) -> Result<BoxStream<'_, Result<OutboxMessage<Self::ID, H, M>, Self::Error>>, Self::Error>
    {
        let this = self.clone();
        let stream = stream! {
            let mut poller = PollerBuilder::new(this.poll_interval)
                .channel_size(this.fetch_size)
                .start(cancel, move || {
                    let this = this.clone();
                    async move {
                            this.fetch_from_db(this.fetch_size)
                            .await
                            .map_err(|e| e.into())
                    }
                });

            while let Some(item) = poller.receiver.recv().await {
                yield item;
            }
        };

        Ok(Box::pin(stream))
    }
}

/// Remove messages from the database, either logically or permanently.
#[async_trait]
impl<H, M> RemoveMessages<H, M> for SqlxOutboxPoller<H, M, sqlx::Postgres>
where
    H: Send + Sync + 'static,
    M: Send + Sync + 'static,
{
    type Error = tower::BoxError;
    type ID = i64;

    async fn remove_messages(
        &self,
        msgs: Vec<OutboxMessage<Self::ID, H, M>>,
    ) -> Result<(), Self::Error> {
        let mut tx = self.pool.begin().await?;
        let query = if self.logical_delete {
            "UPDATE outbox SET deleted = true, deleted_at = NOW() WHERE outbox_id = $1"
        } else {
            "DELETE FROM outbox WHERE outbox_id = $1"
        };
        for msg in msgs {
            sqlx::query(query).bind(msg.id).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }
}

/// Ensures the outbox table exists.
async fn create_table(pool: &sqlx::PgPool) -> Result<(), Error> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS outbox (
            outbox_id BIGSERIAL PRIMARY KEY,
            partition INT DEFAULT 0,
            message JSONB NOT NULL,
            headers JSONB NOT NULL,
            deleted BOOL DEFAULT FALSE,
            deleted_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )",
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Sqlx outbox errors.
#[derive(Debug)]
pub struct Error {
    context: tracing_error::SpanTrace,
    kind: SqlxDriverErrorKind,
}

/// Kinds of SQLx outbox errors.
#[derive(Debug)]
pub enum SqlxDriverErrorKind {
    Database(sqlx::Error),
    Serde(serde_json::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            SqlxDriverErrorKind::Database(err) => writeln!(f, "Database error: {}", err),
            SqlxDriverErrorKind::Serde(err) => writeln!(f, "Serde error: {}", err),
        }?;
        self.context.fmt(f)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.kind {
            SqlxDriverErrorKind::Database(err) => Some(err),
            SqlxDriverErrorKind::Serde(err) => Some(err),
        }
    }
}

impl From<sqlx::Error> for Error {
    fn from(err: sqlx::Error) -> Self {
        Self {
            context: tracing_error::SpanTrace::capture(),
            kind: SqlxDriverErrorKind::Database(err),
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Self {
            context: tracing_error::SpanTrace::capture(),
            kind: SqlxDriverErrorKind::Serde(err),
        }
    }
}

/// Provides a key used to calculate message partition.
pub trait PartitionKey {
    fn partition_key(&self) -> Vec<u8>;
}

/// Calculates a partition from a key using a hash function.
#[tracing::instrument(skip_all)]
fn calculate_partition<K: std::hash::Hash>(key: &K) -> i32 {
    let mut hasher = AHasher::default();
    key.hash(&mut hasher);
    (hasher.finish() % i32::MAX as u64) as i32
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use sqlx::PgPool;
    use teststack::stack;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestHeaders {
        id: u64,
    }

    impl PartitionKey for TestHeaders {
        fn partition_key(&self) -> Vec<u8> {
            self.id.to_be_bytes().to_vec()
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestMessage {
        value: String,
    }

    #[stack(postgres(random_db_name))]
    #[sqlx::test]
    async fn insert_and_fetch_messages_in_order(pool: PgPool) {
        let outbox = SqlxOutbox::try_new(pool.clone()).await.unwrap();

        let messages = vec![
            (TestHeaders { id: 1 }, TestMessage { value: "a".into() }).into(),
            (TestHeaders { id: 2 }, TestMessage { value: "b".into() }).into(),
        ];
        let mut tx = pool.begin().await.unwrap();
        outbox
            .insert_messages(messages.clone(), &mut tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let poller = PgSqlxOutboxPoller::<TestHeaders, TestMessage>::try_new(
            pool,
            Duration::from_millis(10),
        )
        .await
        .unwrap();

        let fetched = poller.fetch_from_db(2).await.unwrap();

        assert_eq!(
            fetched,
            vec![
                OutboxMessage {
                    id: 1,
                    envelope: messages[0].clone(),
                },
                OutboxMessage {
                    id: 2,
                    envelope: messages[1].clone(),
                }
            ]
        );
    }

    #[stack(postgres(random_db_name))]
    #[sqlx::test]
    async fn logical_delete_hides_messages(pool: PgPool) {
        let outbox = SqlxOutbox::try_new(pool.clone()).await.unwrap();

        let mut tx = pool.begin().await.unwrap();
        outbox
            .insert_messages(
                vec![
                    (
                        TestHeaders { id: 1 },
                        TestMessage {
                            value: "hello".into(),
                        },
                    )
                        .into(),
                ],
                &mut tx,
            )
            .await
            .unwrap();

        tx.commit().await.unwrap();

        let poller = PgSqlxOutboxPoller::<TestHeaders, TestMessage>::try_new(
            pool,
            Duration::from_millis(10),
        )
        .await
        .unwrap();

        let msgs = poller.fetch_from_db(1).await.unwrap();
        assert_eq!(msgs.len(), 1);

        poller.remove_messages(msgs).await.unwrap();

        let after = poller.fetch_from_db(1).await.unwrap();
        assert!(after.is_empty());
    }

    #[stack(postgres(random_db_name))]
    #[sqlx::test]
    async fn permanent_delete_removes_rows(pool: PgPool) {
        let outbox = SqlxOutbox::try_new(pool.clone()).await.unwrap();

        let mut tx = pool.begin().await.unwrap();

        outbox
            .insert_messages(
                vec![
                    (
                        TestHeaders { id: 42 },
                        TestMessage {
                            value: "hello".into(),
                        },
                    )
                        .into(),
                ],
                &mut tx,
            )
            .await
            .unwrap();

        tx.commit().await.unwrap();

        let poller = PgSqlxOutboxPoller::<TestHeaders, TestMessage>::try_new(
            pool.clone(),
            Duration::from_millis(10),
        )
        .await
        .unwrap()
        .with_permanent_delete();

        let msgs = poller.fetch_from_db(1).await.unwrap();
        poller.remove_messages(msgs).await.unwrap();

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox")
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(count, 0);
    }

    #[stack(postgres(random_db_name))]
    #[sqlx::test]
    async fn stream_messages_emits_and_stops_on_cancel(pool: PgPool) {
        let outbox = SqlxOutbox::try_new(pool.clone()).await.unwrap();

        let mut tx = pool.begin().await.unwrap();
        outbox
            .insert_messages(
                vec![Envelope {
                    headers: TestHeaders { id: 1 },
                    message: TestMessage {
                        value: "streamed".into(),
                    },
                }],
                &mut tx,
            )
            .await
            .unwrap();

        tx.commit().await.unwrap();

        let poller = PgSqlxOutboxPoller::<TestHeaders, TestMessage>::try_new(
            pool,
            Duration::from_millis(20),
        )
        .await
        .unwrap();

        let cancel = CancellationToken::new();
        let mut stream = poller.messages(cancel.clone()).await.unwrap();

        let msg = stream.next().await.unwrap().unwrap();
        assert_eq!(msg.envelope.message.value, "streamed");

        cancel.cancel();
        assert!(stream.next().await.is_none());
    }
}
