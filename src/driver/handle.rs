use crate::driver::DriverCommand;
use crate::types::error::FrameworkError;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

pub struct JobHandle<T> {
    pub job_id: Uuid,
    receiver: oneshot::Receiver<Result<Vec<T>, FrameworkError>>,
    cancel_tx: Option<mpsc::Sender<DriverCommand>>,
    completed: bool,
}

impl<T> JobHandle<T> {
    pub fn new(
        job_id: Uuid,
        receiver: oneshot::Receiver<Result<Vec<T>, FrameworkError>>,
        cancel_tx: mpsc::Sender<DriverCommand>,
    ) -> Self {
        Self {
            job_id,
            receiver,
            cancel_tx: Some(cancel_tx),
            completed: false,
        }
    }

    pub async fn cancel(&mut self) -> Result<(), FrameworkError> {
        if !self.completed {
            if let Some(tx) = self.cancel_tx.take() {
                self.completed = true;
                let (resp_tx, resp_rx) = oneshot::channel();
                let _ = tx.send(DriverCommand::CancelJob {
                    job_id: self.job_id,
                    resp: Some(resp_tx),
                }).await;
                return resp_rx.await.unwrap_or(Ok(()));
            }
        }
        Ok(())
    }
}

impl<T> Future for JobHandle<T> {
    type Output = Result<Vec<T>, FrameworkError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.receiver).poll(cx) {
            Poll::Ready(Ok(res)) => {
                self.completed = true;
                Poll::Ready(res)
            }
            Poll::Ready(Err(e)) => {
                self.completed = true;
                Poll::Ready(Err(FrameworkError::Other(e.to_string())))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T> Drop for JobHandle<T> {
    fn drop(&mut self) {
        if !self.completed {
            if let Some(tx) = self.cancel_tx.take() {
                let _ = tx.try_send(DriverCommand::CancelJob {
                    job_id: self.job_id,
                    resp: None,
                });
            }
        }
    }
}

