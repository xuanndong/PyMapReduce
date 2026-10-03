use crate::types::error::FrameworkError;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::oneshot;

pub struct JobHandle<T> {
    receiver: oneshot::Receiver<Result<Vec<T>, FrameworkError>>,
}

impl<T> JobHandle<T> {
    pub fn new(receiver: oneshot::Receiver<Result<Vec<T>, FrameworkError>>) -> Self {
        Self { receiver }
    }
}

impl<T> Future for JobHandle<T> {
    type Output = Result<Vec<T>, FrameworkError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.receiver).poll(cx) {
            Poll::Ready(Ok(res)) => Poll::Ready(res),
            Poll::Ready(Err(e)) => Poll::Ready(Err(FrameworkError::Other(e.to_string()))),
            Poll::Pending => Poll::Pending,
        }
    }
}
