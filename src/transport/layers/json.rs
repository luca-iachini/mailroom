use crate::{Envelope, transport::RawPayload};
use std::{future::Future, pin::Pin};
use tower::{Layer, Service};

/// Tower `Service` wrapper that serializes messages to JSON.
///
/// This service converts any message type `M` that implements `serde::Serialize`
/// into a `RawPayload` containing the serialized JSON bytes before passing
/// it to the inner service. Useful for pipelines where the transport expects
/// raw bytes instead of structured types.
#[derive(Clone)]
pub struct JsonService<T> {
    inner: T,
}

impl<T, H, M> Service<Envelope<H, M>> for JsonService<T>
where
    H: Send + 'static,
    M: serde::Serialize + Send + 'static,
    T: Service<Envelope<H, RawPayload>> + Clone + Send + 'static,
    <T as Service<Envelope<H, RawPayload>>>::Error: Into<tower::BoxError>,
    T::Future: Send + 'static,
{
    type Response = T::Response;
    type Error = tower::BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: Envelope<H, M>) -> Self::Future {
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let bytes = serde_json::to_vec(&req.message).map_err(Box::new)?;
            let envelope = Envelope {
                headers: req.headers,
                message: RawPayload(bytes),
            };

            inner.call(envelope).await.map_err(Into::into)
        })
    }
}

/// Tower `Layer` that applies `JsonService` to a service stack.
///
/// Wraps an existing service so that all outgoing messages are serialized
/// to JSON automatically.
pub struct JsonLayer;

impl<S> Layer<S> for JsonLayer {
    type Service = JsonService<S>;

    fn layer(&self, service: S) -> Self::Service {
        JsonService { inner: service }
    }
}
