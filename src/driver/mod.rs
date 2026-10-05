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
use std::collections::HashMap;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::Framed;
use uuid::Uuid;

type JobResultSender = oneshot::Sender<Result<Vec<Vec<u8>>, FrameworkError>>;
type ObjectDataSender = oneshot::Sender<Result<Vec<u8>, FrameworkError>>;
type JobStatusSender = oneshot::Sender<Result<String, FrameworkError>>;

pub enum DriverCommand {
    SubmitJob {
        config: JobConfig,
        resp: oneshot::Sender<Result<JobHandle<Vec<u8>>, FrameworkError>>,
    },
    CancelJob {
        job_id: Uuid,
        resp: Option<oneshot::Sender<Result<(), FrameworkError>>>,
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
        resp: ObjectDataSender,
    },
    GetJobStatus {
        job_id: crate::types::job::JobId,
        resp: JobStatusSender,
    },
    Shutdown,
    DeleteObject {
        object_id: ObjectId,
        resp: oneshot::Sender<Result<(), FrameworkError>>,
    },
}

/// Tracks in-flight requests that are awaiting responses from the HeadNode.
#[derive(Default)]
struct PendingRequests {
    jobs: HashMap<Uuid, JobResultSender>,
    gets: HashMap<ObjectId, Vec<ObjectDataSender>>,
    status: HashMap<Uuid, JobStatusSender>,
}

impl PendingRequests {
    fn insert_job(&mut self, job_id: Uuid, resp: JobResultSender) {
        self.jobs.insert(job_id, resp);
    }

    fn remove_job(&mut self, job_id: &Uuid) {
        self.jobs.remove(job_id);
    }

    fn complete_job(&mut self, job_id: &Uuid, result: Result<Vec<Vec<u8>>, FrameworkError>) {
        if let Some(resp) = self.jobs.remove(job_id) {
            let _ = resp.send(result);
        }
    }

    fn insert_get(&mut self, object_id: ObjectId, resp: ObjectDataSender) {
        self.gets.entry(object_id).or_default().push(resp);
    }

    fn complete_get(&mut self, object_id: &ObjectId, chunk: Vec<u8>) {
        if let Some(senders) = self.gets.remove(object_id) {
            for resp in senders {
                let _ = resp.send(Ok(chunk.clone()));
            }
        }
    }

    fn insert_status(&mut self, job_id: Uuid, resp: JobStatusSender) {
        self.status.insert(job_id, resp);
    }

    fn complete_status(&mut self, job_id: &Uuid, status: Result<String, FrameworkError>) {
        if let Some(resp) = self.status.remove(job_id) {
            let _ = resp.send(status);
        }
    }

    fn job_ids(&self) -> Vec<Uuid> {
        self.jobs.keys().copied().collect()
    }

    fn get_object_ids(&self) -> Vec<ObjectId> {
        self.gets.keys().copied().collect()
    }

    fn fail_all(&mut self, reason: &str) {
        for (_, resp) in self.jobs.drain() {
            let _ = resp.send(Err(FrameworkError::Network(reason.to_string())));
        }
        for (_, senders) in self.gets.drain() {
            for resp in senders {
                let _ = resp.send(Err(FrameworkError::Network(reason.to_string())));
            }
        }
        for (_, resp) in self.status.drain() {
            let _ = resp.send(Err(FrameworkError::Network(reason.to_string())));
        }
    }
}

/// Actor managing the connection to the HeadNode and processing commands/messages.
struct DriverActor {
    framed: Framed<TcpStream, MessageCodec>,
    cmd_rx: mpsc::Receiver<DriverCommand>,
    cmd_tx: mpsc::Sender<DriverCommand>,
    head_addr: String,
    pending: PendingRequests,
}

impl DriverActor {
    fn new(
        stream: TcpStream,
        cmd_rx: mpsc::Receiver<DriverCommand>,
        cmd_tx: mpsc::Sender<DriverCommand>,
        head_addr: String,
    ) -> Self {
        Self {
            framed: Framed::new(stream, MessageCodec::new()),
            cmd_rx,
            cmd_tx,
            head_addr,
            pending: PendingRequests::default(),
        }
    }

    async fn run(mut self) {
        loop {
            tokio::select! {
                cmd = self.cmd_rx.recv() => {
                    let Some(command) = cmd else { break; };
                    if !self.handle_command(command).await {
                        break;
                    }
                }
                msg = self.framed.next() => {
                    match msg {
                        Some(Ok(message)) => {
                            self.handle_message(message);
                        }
                        _ => {
                            if !self.handle_disconnect().await {
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    async fn handle_command(&mut self, command: DriverCommand) -> bool {
        match command {
            DriverCommand::SubmitJob { config, resp } => {
                self.handle_submit_job(config, resp).await;
            }
            DriverCommand::CancelJob { job_id, resp } => {
                self.handle_cancel_job(job_id, resp).await;
            }
            DriverCommand::PutObject { object_id, data, resp } => {
                self.handle_put_object(object_id, data, resp).await;
            }
            DriverCommand::GetObject { object_id, offset, len, resp } => {
                self.handle_get_object(object_id, offset, len, resp).await;
            }
            DriverCommand::GetJobStatus { job_id, resp } => {
                self.handle_get_job_status(job_id, resp).await;
            }
            DriverCommand::DeleteObject { object_id, resp } => {
                self.handle_delete_object(object_id, resp).await;
            }
            DriverCommand::Shutdown => {
                let _ = self.framed.send(Message::Shutdown).await;
                return false;
            }
        }
        true
    }

    async fn handle_submit_job(
        &mut self,
        config: JobConfig,
        resp: oneshot::Sender<Result<JobHandle<Vec<u8>>, FrameworkError>>,
    ) {
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
            tasks.push(bincode::serialize(&task).unwrap_or_default());
        }

        let msg = Message::SubmitJob {
            job_id,
            tasks,
            max_time_secs: config.max_time.map(|d| d.as_secs()),
        };

        if self.framed.send(msg).await.is_ok() {
            let (job_tx, job_rx) = oneshot::channel();
            let _ = resp.send(Ok(JobHandle::new(job_id, job_rx, self.cmd_tx.clone())));
            self.pending.insert_job(job_id, job_tx);
        } else {
            let _ = resp.send(Err(FrameworkError::Network("Failed to send SubmitJob".into())));
        }
    }

    async fn handle_cancel_job(
        &mut self,
        job_id: Uuid,
        resp: Option<oneshot::Sender<Result<(), FrameworkError>>>,
    ) {
        self.pending.remove_job(&job_id);
        let msg = Message::CancelJob { job_id };
        let res = if self.framed.send(msg).await.is_ok() {
            Ok(())
        } else {
            Err(FrameworkError::Network("Failed to send CancelJob".into()))
        };
        if let Some(r) = resp {
            let _ = r.send(res);
        }
    }

    async fn handle_put_object(
        &mut self,
        object_id: ObjectId,
        data: Vec<u8>,
        resp: oneshot::Sender<Result<(), FrameworkError>>,
    ) {
        let msg = Message::PutObject { object_id, data };
        let res = if self.framed.send(msg).await.is_ok() {
            Ok(())
        } else {
            Err(FrameworkError::Network("Failed to send PutObject".into()))
        };
        let _ = resp.send(res);
    }

    async fn handle_get_object(
        &mut self,
        object_id: ObjectId,
        offset: u64,
        len: u64,
        resp: ObjectDataSender,
    ) {
        let msg = Message::GetObject { object_id, offset, len };
        if self.framed.send(msg).await.is_ok() {
            self.pending.insert_get(object_id, resp);
        } else {
            let _ = resp.send(Err(FrameworkError::Network("Failed to send GetObject".into())));
        }
    }

    async fn handle_get_job_status(&mut self, job_id: Uuid, resp: JobStatusSender) {
        let msg = Message::GetJobStatus { job_id };
        if self.framed.send(msg).await.is_ok() {
            self.pending.insert_status(job_id, resp);
        } else {
            let _ = resp.send(Err(FrameworkError::Network("Failed to send GetJobStatus".into())));
        }
    }

    async fn handle_delete_object(
        &mut self,
        object_id: ObjectId,
        resp: oneshot::Sender<Result<(), FrameworkError>>,
    ) {
        let msg = Message::DeleteObject { object_id };
        let res = if self.framed.send(msg).await.is_ok() {
            Ok(())
        } else {
            Err(FrameworkError::Network("Failed to send DeleteObject".into()))
        };
        let _ = resp.send(res);
    }

    fn handle_message(&mut self, message: Message) {
        match message {
            Message::JobComplete { job_id, results } => {
                self.pending.complete_job(&job_id, Ok(results));
            }
            Message::JobFailed { job_id, reason } => {
                self.pending.complete_job(&job_id, Err(FrameworkError::Other(reason)));
            }
            Message::JobProgress { job_id, completed, total } => {
                let status = format!("{}/{} completed", completed, total);
                self.pending.complete_status(&job_id, Ok(status));
            }
            Message::ObjectData { object_id, chunk, .. } => {
                self.pending.complete_get(&object_id, chunk);
            }
            _ => {}
        }
    }

    async fn handle_disconnect(&mut self) -> bool {
        const BACKOFFS: [u64; 6] = [1, 2, 4, 6, 8, 9];
        let mut reconnected = false;

        for delay in BACKOFFS {
            tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
            if let Ok(new_stream) = TcpStream::connect(&self.head_addr).await {
                self.framed = Framed::new(new_stream, MessageCodec::new());
                reconnected = true;
                break;
            }
        }

        if reconnected {
            for job_id in self.pending.job_ids() {
                let _ = self.framed.send(Message::AttachJob { job_id }).await;
            }
            for obj_id in self.pending.get_object_ids() {
                let _ = self
                    .framed
                    .send(Message::GetObject {
                        object_id: obj_id,
                        offset: 0,
                        len: u64::MAX,
                    })
                    .await;
            }
            true
        } else {
            self.pending.fail_all("HeadNode unavailable after 6 retries (30s total)");
            false
        }
    }
}

/// Client handle for interacting with the cluster.
pub struct Driver {
    tx: mpsc::Sender<DriverCommand>,
}

impl Driver {
    pub async fn connect(
        head_node: &str,
        _workers: Option<Vec<String>>,
    ) -> Result<Self, FrameworkError> {
        let stream = TcpStream::connect(head_node)
            .await
            .map_err(|e| FrameworkError::Network(e.to_string()))?;

        let (tx, rx) = mpsc::channel::<DriverCommand>(100);
        let actor = DriverActor::new(stream, rx, tx.clone(), head_node.to_string());
        tokio::spawn(actor.run());

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

    pub async fn cancel_job(&self, job_id: Uuid) -> Result<(), FrameworkError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(DriverCommand::CancelJob {
                job_id,
                resp: Some(resp_tx),
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

        let head = HeadNode::new(gcs.clone(), dispatcher.clone(), wal, addr);

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
