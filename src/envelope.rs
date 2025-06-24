/// Message container used by the outbox and delivery pipeline.
///
/// `Envelope` bundles a message payload together with its associated headers.
/// It is intentionally generic and transport-agnostic.
///
/// ## Design
///
/// - `H` represents message metadata (e.g. routing keys, correlation IDs,
///   timestamps, or tracing information)
/// - `M` represents the actual message payload
///
/// Keeping headers and payload separated makes it easier to:
/// - Serialize and persist messages
/// - Attach transport-specific metadata
/// - Reuse the same payload across different delivery mechanisms
///
/// ## Conversion
///
/// `Envelope` implements `From<(H, M)>` for ergonomic construction when headers
/// and payload are already available as a tuple.
///
/// ## Example
///
/// ```rust
/// use mailroom::Envelope;
///
/// let envelope = Envelope {
///     headers: "user.created",
///     message: 42,
/// };
///
/// // or, equivalently
/// let envelope: Envelope<_, _> = ("user.created", 42).into();
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct Envelope<H, M> {
    /// Message metadata.
    pub headers: H,
    /// Message payload.
    pub message: M,
}

impl<H, M> From<(H, M)> for Envelope<H, M> {
    fn from(value: (H, M)) -> Self {
        Envelope {
            headers: value.0,
            message: value.1,
        }
    }
}
