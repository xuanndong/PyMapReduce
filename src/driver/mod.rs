pub mod config;
pub mod handle;

use crate::cluster::handle::ClusterHandle;
use crate::driver::config::JobConfig;
use crate::driver::handle::JobHandle;
use crate::protocol::codec::MessageCodec;
use crate::protocol::message::{Message, ObjectId};
use crate::types::error::FrameworkError;
use crate::types::task::Task;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::Framed;
use uuid::Uuid;

pub struct Driver {
    tx: mpsc::Sender<DriverCommand>,
}

enum DriverCommand {
    SubmitJob {
        config: JobConfig,
        resp: oneshot::Sender<Result<JobHandle<Vec<u8>>, FrameworkError>>,
    },
    PutObject {
        object_id: ObjectId,
        data: Vec<u8>,
        resp: oneshot::Sender<Result<(), FrameworkError>>,
    },
    GetObject {
        object_id: ObjectId,
        offset: u64,
        len: u64,
        resp: oneshot::Sender<Result<Vec<u8>, FrameworkError>>,
    },
    GetJobStatus {
        job_id: crate::types::job::JobId,
        resp: oneshot::Sender<Result<String, FrameworkError>>,
    },
    Shutdown,
    DeleteObject {
        object_id: ObjectId,
        resp: oneshot::Sender<Result<(), FrameworkError>>,
    },
}

impl Driver {
    pub async fn connect(
        head_node: &str,
        _workers: Option<Vec<String>>,
    ) -> Result<Self, FrameworkError> {
        let stream = TcpStream::connect(head_node)
            .await
            .map_err(|e| FrameworkError::Network(e.to_string()))?;
        let mut framed = Framed::new(stream, MessageCodec::new());

        let (tx, mut rx) = mpsc::channel::<DriverCommand>(100);
        let head_addr = head_node.to_string();

        tokio::spawn(async move {
            let mut pending_jobs: std::collections::HashMap<Uuid, oneshot::Sender<Result<Vec<Vec<u8>>, FrameworkError>>> = std::collections::HashMap::new();
            let mut pending_gets: std::collections::HashMap<ObjectId, Vec<oneshot::Sender<Result<Vec<u8>, FrameworkError>>>> = std::collections::HashMap::new();
            let mut pending_status: std::collections::HashMap<Uuid, oneshot::Sender<Result<String, FrameworkError>>> = std::collections::HashMap::new();

            loop {
                tokio::select! {
                    cmd = rx.recv() => {
                        let Some(command) = cmd else { break; };
                        match command {
                            DriverCommand::SubmitJob { config, resp } => {
                                let job_id = Uuid::new_v4();
                                let mut tasks = Vec::new();
                                for t in config.tasks {
                                    let task = Task {
                                        id: Uuid::new_v4(),
                                        job_id,
                                        kind: t.kind,
                                        payload: t.payload,
                                        runtime_env: config.runtime_env,
                                        dependencies: t.dependencies,
                                    };
                                    tasks.push(bincode::serialize(&task).unwrap());
                                }

                                let msg = Message::SubmitJob {
                                    job_id,
                                    tasks,
                                    max_time_secs: config.max_time.map(|d| d.as_secs()),
                                };

                                if framed.send(msg).await.is_ok() {
                                    let (job_tx, job_rx) = oneshot::channel();
                                    let _ = resp.send(Ok(JobHandle::new(job_rx)));
                                    pending_jobs.insert(job_id, job_tx);
                                } else {
                                    let _ = resp.send(Err(FrameworkError::Network("Failed to send".into())));
                                }
                            }
                            DriverCommand::PutObject { object_id, data, resp } => {
                                let msg = Message::PutObject { object_id, data };
                                let res = if framed.send(msg).await.is_ok() {
                                    Ok(())
                                } else {
                                    Err(FrameworkError::Network("Failed to send".into()))
                                };
                                let _ = resp.send(res);
                            }
                            DriverCommand::GetObject { object_id, offset, len, resp } => {
                                let msg = Message::GetObject { object_id, offset, len };
                                if framed.send(msg).await.is_ok() {
                                    pending_gets.entry(object_id).or_default().push(resp);
                                } else {
                                    let _ = resp.send(Err(FrameworkError::Network("Failed to send".into())));
                                }
                            }
                            DriverCommand::GetJobStatus { job_id, resp } => {
                                let msg = Message::GetJobStatus { job_id };
                                if framed.send(msg).await.is_ok() {
                                    pending_status.insert(job_id, resp);
                                } else {
                                    let _ = resp.send(Err(FrameworkError::Network("Failed to send".into())));
                                }
                            }
                            DriverCommand::DeleteObject { object_id, resp } => {
                                let msg = Message::DeleteObject { object_id };
                                let res = if framed.send(msg).await.is_ok() {
                                    Ok(())
                                } else {
                                    Err(FrameworkError::Network("Failed to send".into()))
                                };
                                let _ = resp.send(res);
                            }
                            DriverCommand::Shutdown => {
                                let _ = framed.send(Message::Shutdown).await;
                                break;
                            }
                        }
                    }
                    msg = framed.next() => {
                        match msg {
                            Some(Ok(Message::JobComplete { job_id, results })) => {
                                if let Some(resp) = pending_jobs.remove(&job_id) {
                                    let _ = resp.send(Ok(results));
                                }
                            }
                            Some(Ok(Message::JobFailed { job_id, reason })) => {
                                if let Some(resp) = pending_jobs.remove(&job_id) {
                                    let _ = resp.send(Err(FrameworkError::Other(reason)));
                                }
                            }
                            Some(Ok(Message::JobProgress { job_id, completed, total })) => {
                                if let Some(resp) = pending_status.remove(&job_id) {
                                    let _ = resp.send(Ok(format!("{}/{} completed", completed, total)));
                                }
                            }
                            Some(Ok(Message::ObjectData { object_id, chunk, .. })) => {
                                if let Some(senders) = pending_gets.remove(&object_id) {
                                    for resp in senders {
                                        let _ = resp.send(Ok(chunk.clone()));
                                    }
                                }
                            }
                            Some(Ok(_)) => {}
                            _ => {
                                // Socket closed or error: attempt auto-reconnection with 6 retries (total 30s)
                                let backoffs = [1, 2, 4, 6, 8, 9];
                                let mut reconnected = false;

                                for delay in backoffs {
                                    tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
                                    if let Ok(new_stream) = TcpStream::connect(&head_addr).await {
                                        framed = Framed::new(new_stream, MessageCodec::new());
                                        reconnected = true;
                                        break;
                                    }
                                }

                                if reconnected {
                                    // Re-attach all in-flight jobs
                                    for &job_id in pending_jobs.keys() {
                                        let _ = framed.send(Message::AttachJob { job_id }).await;
                                    }
                                    // Re-request pending gets
                                    for &obj_id in pending_gets.keys() {
                                        let _ = framed.send(Message::GetObject {
                                            object_id: obj_id,
                                            offset: 0,
                                            len: u64::MAX,
                                        }).await;
                                    }
                                } else {
                                    // Fail all pending requests after retries exhausted (30s)
                                    for (_, resp) in pending_jobs.drain() {
                                        let _ = resp.send(Err(FrameworkError::Network(
                                            "HeadNode unavailable after 6 retries (30s total)".into(),
                                        )));
                                    }
                                    for (_, senders) in pending_gets.drain() {
                                        for resp in senders {
                                            let _ = resp.send(Err(FrameworkError::Network(
                                                "HeadNode unavailable after 6 retries (30s total)".into(),
                                            )));
                                        }
                                    }
                                    for (_, resp) in pending_status.drain() {
                                        let _ = resp.send(Err(FrameworkError::Network(
                                            "HeadNode unavailable after 6 retries (30s total)".into(),
                                        )));
                                    }
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        });

        Ok(Self { tx })
    }

    pub async fn delete_object(&self, id: ObjectId) -> Result<(), FrameworkError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(DriverCommand::DeleteObject {
                object_id: id,
                resp: resp_tx,
            })
            .await
            .map_err(|_| FrameworkError::Other("Driver task died".into()))?;
        resp_rx
            .await
            .map_err(|_| FrameworkError::Other("Driver task died before responding".into()))?
    }

    pub async fn shutdown(&self) -> Result<(), FrameworkError> {
        self.tx
            .send(DriverCommand::Shutdown)
            .await
            .map_err(|_| FrameworkError::Other("Driver task died".into()))?;
        Ok(())
    }

    pub async fn init(
        address: Option<String>,
        strategy: Option<String>,
    ) -> Result<ClusterHandle, FrameworkError> {
        use crate::cluster::head::HeadNode;
        use crate::fault::wal::WriteAheadLog;
        use crate::gcs::store::Gcs;
        use crate::scheduler::dispatcher::Dispatcher;
        use crate::scheduler::strategy::SchedulerType;
        use std::sync::Arc;

        let gcs = Arc::new(Gcs::new());
        let strategy_enum = match strategy.as_deref() {
            Some("WeightedCapacity") => SchedulerType::WeightedCapacity,
            Some("LeastLoad") => SchedulerType::LeastLoad,
            Some("LocalityFirst") => SchedulerType::LocalityFirst,
            Some("Adaptive") => SchedulerType::Adaptive,
            _ => SchedulerType::RoundRobin,
        };
        let dispatcher = Arc::new(Dispatcher::new(strategy_enum.into_strategy()));

        let wal = WriteAheadLog::new(std::path::PathBuf::from("/tmp/wal.log")).await?;
        
        let addr = address.unwrap_or_else(|| "127.0.0.1:7777".to_string());

        let head = HeadNode::new(
            gcs.clone(),
            dispatcher.clone(),
            wal,
            addr,
        );

        tokio::spawn(async move {
            let _ = head.run().await;
        });

        Ok(ClusterHandle::new())
    }

    pub async fn submit(&self, config: JobConfig) -> Result<JobHandle<Vec<u8>>, FrameworkError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(DriverCommand::SubmitJob {
                config,
                resp: resp_tx,
            })
            .await
            .map_err(|_| FrameworkError::Other("Driver task died".into()))?;
        resp_rx
            .await
            .map_err(|_| FrameworkError::Other("Response channel dropped".into()))?
    }

    pub async fn put(&self, object_id: ObjectId, data: Vec<u8>) -> Result<(), FrameworkError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(DriverCommand::PutObject {
                object_id,
                data,
                resp: resp_tx,
            })
            .await
            .map_err(|_| FrameworkError::Other("Driver task died".into()))?;
        resp_rx
            .await
            .map_err(|_| FrameworkError::Other("Response channel dropped".into()))?
    }

    pub async fn get(
        &self,
        object_id: ObjectId,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>, FrameworkError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(DriverCommand::GetObject {
                object_id,
                offset,
                len,
                resp: resp_tx,
            })
            .await
            .map_err(|_| FrameworkError::Other("Driver task died".into()))?;
        resp_rx
            .await
            .map_err(|_| FrameworkError::Other("Response channel dropped".into()))?
    }

    pub async fn get_job_status(&self, job_id: crate::types::job::JobId) -> Result<String, FrameworkError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(DriverCommand::GetJobStatus {
                job_id,
                resp: resp_tx,
            })
            .await
            .map_err(|_| FrameworkError::Other("Driver task died".into()))?;
        resp_rx
            .await
            .map_err(|_| FrameworkError::Other("Response channel dropped".into()))?
    }
}
