use chrono::{DateTime, Utc};
use serde::Serialize;
use std::pin::Pin;
use tower::{Layer, Service};

use crate::{Envelope, transport::RawPayload};

/// Tower `Service` wrapper that converts messages into JSON CloudEvents format.
///
/// This service transforms an envelope with generic headers `H` and message `M`
/// into an envelope with the same headers and a `RawPayload` containing a serialized
/// CloudEvents JSON message. It can be used as a middleware in a Tower service stack.
#[derive(Clone)]
pub struct JsonCloudEventsService<T> {
    inner: T,
    source: String,
}

impl<T, H, M> Service<Envelope<H, M>> for JsonCloudEventsService<T>
where
    H: ToCloudEventsHeaders + Send + 'static,
    M: serde::Serialize + Send + 'static,
    T: Service<Envelope<H, RawPayload>> + Clone + Send + 'static,
    <T as Service<Envelope<H, RawPayload>>>::Error: Into<tower::BoxError>,
    T::Future: Send + 'static,
{
    type Response = T::Response;
    type Error = tower::BoxError;
    type Future =
        Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: Envelope<H, M>) -> Self::Future {
        let source = self.source.clone();
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let headers = req.headers.to_cloud_events_headers();
            let message = CloudEventsMessage {
                specversion: headers.spec_version,
                source,
                subject: headers.subject,
                id: headers.id,
                r#type: headers.r#type,
                datacontenttype: "application/json".to_owned(),
                data: serde_json::to_value(&req.message).map_err(Box::new)?,
                time: headers.time.to_rfc3339(),
            };

            let bytes = serde_json::to_vec(&message).map_err(Box::new)?;
            let envelope = Envelope {
                headers: req.headers,
                message: RawPayload(bytes),
            };

            inner.call(envelope).await.map_err(Into::into)
        })
    }
}

/// Tower `Layer` that applies `JsonCloudEventsService` to a service stack.
///
/// This layer is used to wrap an existing service so that all outgoing messages
/// are transformed into CloudEvents JSON before reaching the inner transport.
pub struct JsonCloudEventsLayer {
    source: String,
}

impl JsonCloudEventsLayer {
    /// Create a new layer specifying the CloudEvents `source` field.
    pub fn new(source: impl ToString) -> Self {
        Self {
            source: source.to_string(),
        }
    }
}

impl<S> Layer<S> for JsonCloudEventsLayer {
    type Service = JsonCloudEventsService<S>;

    fn layer(&self, service: S) -> Self::Service {
        JsonCloudEventsService {
            source: self.source.clone(),
            inner: service,
        }
    }
}

/// Trait for converting generic headers into CloudEvents-compatible headers.
pub trait ToCloudEventsHeaders {
    fn to_cloud_events_headers(&self) -> CloudEventsHeaders;
}

/// Standard CloudEvents headers.
#[derive(Clone, Debug, Serialize)]
pub struct CloudEventsHeaders {
    pub spec_version: String,
    pub subject: String,
    pub id: String,
    pub r#type: String,
    pub time: DateTime<Utc>,
}

impl ToCloudEventsHeaders for CloudEventsHeaders {
    fn to_cloud_events_headers(&self) -> CloudEventsHeaders {
        self.clone()
    }
}

/// JSON CloudEvents message structure.
///
/// This is the format that is serialized and sent as `RawPayload`.
#[derive(Clone, Debug, Serialize)]
pub struct CloudEventsMessage {
    pub specversion: String,
    pub source: String,
    pub subject: String,
    pub id: String,
    pub r#type: String,
    pub datacontenttype: String,
    pub data: serde_json::Value,
    pub time: String,
}
