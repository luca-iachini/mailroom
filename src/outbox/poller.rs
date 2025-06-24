use std::{marker::PhantomData, time::Duration};

use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

/// A continuously running background poller that produces a stream of results.
///
/// This struct holds the receiver for polled items and keeps the spawned background
/// task alive. Each polled item is sent to `receiver` as `Ok(T)`; errors are sent as `Err(BoxError)`.
pub struct Poller<T> {
    /// Receiver that yields polled items or errors.
    pub receiver: mpsc::Receiver<Result<T, tower::BoxError>>,

    /// Handle to the background task. Kept private to ensure the task is alive as long as
    /// the `Poller` exists.
    _handle: JoinHandle<()>,

    _marker: PhantomData<T>,
}

/// Builder for creating a `Poller`.
///
/// Configures the polling interval, channel buffer size, and the polling function.
pub struct PollerBuilder<T> {
    interval: Duration,
    channel_size: usize,
    _marker: PhantomData<T>,
}

impl<T> PollerBuilder<T> {
    /// Create a new `PollerBuilder` with the specified polling interval.
    ///
    /// # Arguments
    ///
    /// * `interval` - Duration between polling attempts.
    pub fn new(interval: Duration) -> Self {
        Self {
            interval,
            channel_size: 100, // default channel size
            _marker: PhantomData,
        }
    }

    /// Set the channel buffer size for sending polled items.
    ///
    /// # Arguments
    ///
    /// * `size` - The capacity of the internal mpsc channel.
    pub fn channel_size(mut self, size: usize) -> Self {
        self.channel_size = size;
        self
    }

    /// Start the poller in the background.
    ///
    /// # Arguments
    ///
    /// * `cancel` - A `CancellationToken` that can be used to stop the poller.
    /// * `poll_fn` - An async function or closure returning a `Vec<T>` or a `BoxError`.
    ///
    /// # Returns
    ///
    /// A `Poller<T>` containing a receiver for the polled items and errors.
    pub fn start<F, Fut>(self, cancel: CancellationToken, mut poll_fn: F) -> Poller<T>
    where
        T: Send + 'static,
        F: FnMut() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<Vec<T>, tower::BoxError>> + Send,
    {
        let (tx, receiver) = mpsc::channel(self.channel_size);
        let interval = self.interval;

        // Spawn a background task that polls at the configured interval
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        match poll_fn().await {
                            Ok(items) => {
                                for item in items {
                                    if tx.send(Ok(item)).await.is_err() {
                                        // Receiver dropped, stop polling
                                        return;
                                    }
                                }
                            }
                            Err(err) => {
                                if tx.send(Err(err)).await.is_err() {
                                    // Receiver dropped, stop polling
                                    return;
                                }
                            }
                        }
                    }
                    _ = cancel.cancelled() => return,
                }
            }
        });

        Poller {
            receiver,
            _handle: handle,
            _marker: PhantomData,
        }
    }
}
