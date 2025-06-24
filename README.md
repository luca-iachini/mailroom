# Postoffice

Postoffice is a Rust library implementing the Outbox Pattern for reliable message delivery in distributed microservices.
It provides an async-first API for storing, partitioning, and dispatching messages with pluggable transports and middleware layers, ensuring transaction-safe publishing and high concurrency.

## Features

- Async-first Rust API for message storage and streaming.

- Supports `Postgres` via `SqlxOutbox` as the default storage driver.

- Partitioned polling with `SqlxOutboxPoller` for parallel consumption within the same application.

- Pluggable transports for delivering messages (e.g., RabbitMQ, Kafka, in-memory for tests).

- Middleware support for `JSON` or `CloudEvents` encoding.

- Supports logical deletion or physical removal after successful delivery.

- Provides an in-memory outbox for testing or small-scale use cases.

> ⚠️ **Important:** Postoffice does not provide distributed coordination across multiple application instances. Partitioning only enables parallelism within a single process.

## Installation

Add Postoffice to your `Cargo.toml`:

```toml
[dependencies]
postoffice = "0.1" # replace with the latest version
serde = { version = "1", features = ["derive"] }
sqlx = { version = "0.7", features = ["postgres", "runtime-tokio-native-tls"] }
tokio = { version = "1", features = ["full"] }
tracing = "0.1"
```

## Quick Start

### Publishing Messages

```rust,no_run
use postoffice::{Outbox, outbox, outbox::sqlx::PartitionKey};
use sqlx::PgPool;
use serde::{Serialize, Deserialize};

#[derive(Clone, Serialize, Deserialize)]
struct MsgHeaders {
    id: i32,
    subject: String,
}

impl PartitionKey for MsgHeaders {
    fn partition_key(&self) -> Vec<u8> {
        self.id.to_be_bytes().to_vec()
    }
}

#[tokio::main]
async fn main() {
    let pool = PgPool::connect("postgres://user:password@localhost/db").await.unwrap();
    let outbox = outbox::sqlx::PgSqlxOutbox::try_new(pool.clone())
        .await
        .unwrap();
    let outbox = Outbox::new(outbox);

    let mut tx = pool.begin().await.unwrap();
    outbox.publish_messages(
        [(MsgHeaders { id: 1, subject: "hello".to_string() }, "Hello, world!".to_string())],
        &mut tx,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
}
```

### Send Messages with a Producer

```rust,no_run
use postoffice::{transport, outbox, Transport, Producer};
use sqlx::PgPool;
use serde::{Serialize, Deserialize};
use tokio_util::sync::CancellationToken;
use std::time::Duration;

#[derive(Clone, Serialize, Deserialize, Debug)]
struct MsgHeaders {
    id: i32,
    subject: String,
}

#[tokio::main]
async fn main() {
    let pool = PgPool::connect("postgres://user:password@localhost/db").await.unwrap();
    let cancel_token = CancellationToken::new();

    let outbox: outbox::sqlx::PgSqlxOutboxPoller<MsgHeaders, String> =
        outbox::sqlx::PgSqlxOutboxPoller::try_new(pool.clone(), Duration::from_secs(1))
        .await
        .unwrap();

    let transport = Transport::new(transport::InMemory::default());
    Producer::new(outbox, transport)
        .run(cancel_token)
        .await
        .unwrap();
}
```

## How It Works

- Durable message storage – messages are inserted transactionally into an outbox table to prevent loss.

- Partitioned processing – messages can be split across multiple producers for concurrent delivery.

- Pluggable transport – messages are dispatched via any transport implementing the Sender trait, with optional middleware (JSON serialization, timeouts, logging).

- Async consumption – messages are consumed asynchronously with support for graceful shutdown using `CancellationToken`.

## Transports

Postoffice provides pluggable transports that implement the Sender trait:

- RabbitMQ (RabbitMq): Send messages to AMQP exchanges.

- Kafka (Kafka): Publish messages to Kafka topics.

- In-Memory (InMemory): Useful for testing or development.

Middleware layers are available for transforming messages:

- `JsonLayer`: Serialize messages as JSON.

- `JsonCloudEventsLayer`: Serialize messages as `CloudEvents` `JSON`.

## Scaling Notes

Single-process scaling only: `Postoffice` does not handle distributed locks or coordination across multiple instances.
Ensure that multiple application instances do not read the same partitions unless you implement external coordination.

## License

Postoffice is licensed under the MIT License. See LICENSE for details.

