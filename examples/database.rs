use std::time::Duration;

use postoffice::outbox::{self, Outbox};
use postoffice::transport::layers::JsonLayer;
use postoffice::{Envelope, transport};
use postoffice::{Producer, transport::Transport};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use sqlx::postgres::PgConnectOptions;
use tokio_util::sync::CancellationToken;
use tracing_error::ErrorLayer;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::prelude::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Msg(MsgHeaders, MsgContent);

#[derive(Clone, Serialize, Deserialize, Debug)]
struct MsgHeaders {
    id: i32,
    version: String,
    subject: String,
    r#type: String,
}

impl MsgHeaders {
    pub fn new(id: i32) -> Self {
        Self {
            id,
            version: "1.0.0".to_string(),
            subject: "hello.msg".to_string(),
            r#type: "hello".to_string(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct MsgContent {
    message: String,
}

impl From<&str> for MsgContent {
    fn from(value: &str) -> Self {
        Self {
            message: value.to_owned(),
        }
    }
}

impl From<Msg> for Envelope<MsgHeaders, MsgContent> {
    fn from(value: Msg) -> Self {
        Envelope {
            headers: value.0,
            message: value.1,
        }
    }
}

impl outbox::sqlx::PartitionKey for MsgHeaders {
    fn partition_key(&self) -> Vec<u8> {
        self.id.to_be_bytes().to_vec()
    }
}

#[tokio::main]
async fn main() {
    dotenv::dotenv().unwrap();

    let fmt_layer = tracing_subscriber::fmt::layer().with_target(false);
    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(EnvFilter::from_default_env())
        .with(ErrorLayer::default())
        .init();

    // Postgres connection
    let pool = PgPool::connect_with(PgConnectOptions::new()).await.unwrap();

    let cancel = CancellationToken::new();
    let cancel_signal = cancel.clone();
    let cancel_handle = tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
        cancel_signal.cancel();
    });

    let pool_clone = pool.clone();
    let cancel_outbox = cancel.clone();
    let outbox_handle = tokio::spawn(async move {
        let outbox = outbox::sqlx::PgSqlxOutbox::try_new(pool_clone.clone())
            .await
            .unwrap();
        let outbox = Outbox::new(outbox);

        let mut id = 0;
        loop {
            let mut tx = pool_clone.begin().await.unwrap();
            outbox
                .publish_messages([Msg(MsgHeaders::new(id), "Hello".into())], &mut tx)
                .await
                .expect("Failed to insert message");
            tx.commit().await.unwrap();
            id += 1;
            tokio::time::sleep(Duration::from_millis(200)).await;
            if cancel_outbox.is_cancelled() {
                break;
            }
        }
    });

    let inmemory_transport: transport::InMemory<MsgHeaders, _> = transport::InMemory::default();
    let transport = Transport::new(inmemory_transport).layer(JsonLayer);
    let outbox: outbox::sqlx::PgSqlxOutboxPoller<MsgHeaders, MsgContent> =
        outbox::sqlx::PgSqlxOutboxPoller::try_new(pool.clone(), Duration::from_secs(1))
            .await
            .unwrap();

    let producer_handle = tokio::spawn(async move {
        Producer::new(outbox, transport).run(cancel).await.unwrap();
    });

    tokio::try_join!(cancel_handle, outbox_handle, producer_handle).unwrap();
}
