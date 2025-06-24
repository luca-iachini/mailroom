#![doc = include_str!("../README.md")]

pub mod envelope;
pub mod outbox;
mod producer;
pub mod transport;

#[doc(inline)]
pub use envelope::Envelope;

#[doc(inline)]
pub use outbox::{Outbox, OutboxError, OutboxMessage};

#[doc(inline)]
pub use transport::{Transport, TransportError, TransportErrorKind};

#[doc(inline)]
pub use producer::{
    DefaultProducerHook, Producer, ProducerHook, ProducerRunError, ProducerRunErrorKind,
};
