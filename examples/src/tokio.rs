use std::{process, time::Duration};

use airflow_common::{
    datetime::{StdTimeProvider, TimeProvider},
    models::{TaskInstanceKey, TaskInstanceLike},
};
use airflow_edge_sdk::{
    api::EdgeJobFetched,
    worker::{EdgeJob, Intercom, IntercomMessage, WorkerRuntime, WorkerState},
};
use airflow_task_sdk::{
    api::{ExecutionApiClient, ExecutionApiClientFactory},
    definitions::DagBag,
    execution::{
        ExecutionError, ServiceResult, StartupDetails, SupervisorComms, SupervisorCommsError,
        TaskHandle, TaskRunner, TaskRuntime, ToSupervisor, ToTask, supervise,
    },
};
use log::{debug, error, info};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

// TODO where should this be defined?
static EXECUTION_API_SERVER_URL: &str = "http://localhost:28080/execution";

#[derive(Debug)]
pub struct TokioEdgeJob {
    ti_key: TaskInstanceKey,
    handle: Option<JoinHandle<bool>>,
    result: bool,
    concurrency_slots: usize,
}

impl EdgeJob for TokioEdgeJob {
    fn ti_key(&self) -> &TaskInstanceKey {
        &self.ti_key
    }

    fn concurrency_slots(&self) -> usize {
        self.concurrency_slots
    }

    fn is_running(&self) -> bool {
        match &self.handle {
            Some(h) => !h.is_finished(),
            None => false,
        }
    }

    fn abort(&mut self) {
        if let Some(h) = self.handle.take() {
            h.abort();
        };
    }

    async fn is_success(&mut self) -> bool {
        if let Some(h) = self.handle.take() {
            self.result = h.await.unwrap_or_default()
        };
        self.result
    }
}

#[derive(Debug, Clone)]
pub struct TokioIntercom(mpsc::Sender<IntercomMessage>);

impl Intercom for TokioIntercom {
    type SendError = mpsc::error::SendError<IntercomMessage>;

    async fn send(&self, msg: IntercomMessage) -> Result<(), Self::SendError> {
        self.0.send(msg).await
    }
}

pub struct TokioRuntime<F: ExecutionApiClientFactory> {
    concurrency: usize,
    recv: mpsc::Receiver<IntercomMessage>,
    send: mpsc::Sender<IntercomMessage>,
    hostname: String,
    client_factory: F,
}

impl<F: ExecutionApiClientFactory> TokioRuntime<F> {
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }
}

impl<F: ExecutionApiClientFactory + Default> Default for TokioRuntime<F> {
    fn default() -> Self {
        let (send, recv) = mpsc::channel(1);
        let hostname = whoami::fallible::hostname().expect("Could not get hostname");
        TokioRuntime {
            recv,
            send,
            concurrency: 1,
            hostname,
            client_factory: F::default(),
        }
    }
}

impl<F> WorkerRuntime for TokioRuntime<F>
where
    F: ExecutionApiClientFactory + Clone + 'static,
    F::Client: Clone + Sync,
    <F::Client as ExecutionApiClient>::Error: Send,
{
    type Job = TokioEdgeJob;
    type Intercom = TokioIntercom;

    async fn sleep(&mut self, duration: Duration) -> Option<IntercomMessage> {
        debug!("Sleeping for {} seconds", duration.as_secs());
        tokio::select! {
            v = self.recv.recv() =>  v,
            _ = tokio::time::sleep(duration) => None,
        }
    }

    fn intercom(&self) -> Self::Intercom {
        TokioIntercom(self.send.clone())
    }

    fn launch(&self, job: EdgeJobFetched, dag_bag: &'static DagBag) -> Self::Job {
        let ti_key = job.ti_key();
        let intercom = self.intercom();
        let client_factory = self.client_factory.clone();

        let handle = tokio::spawn(async move {
            let key = job.command.ti().ti_key();
            info!("{}: Worker task launched.", key);

            // TODO should the time provider be passed from somewhere?
            let time_provider = StdTimeProvider;
            let runtime = TokioTaskRuntime::default();

            supervise(
                job.command,
                client_factory,
                time_provider,
                dag_bag,
                &runtime,
                EXECUTION_API_SERVER_URL,
            )
            .await;
            intercom.send(IntercomMessage::JobCompleted(key)).await.ok();
            true
        });
        TokioEdgeJob {
            ti_key,
            handle: Some(handle),
            result: false,
            concurrency_slots: job.concurrency_slots,
        }
    }

    fn concurrency(&self) -> usize {
        self.concurrency
    }

    async fn on_update(&mut self, _state: &WorkerState) -> () {}

    fn hostname(&self) -> &str {
        &self.hostname
    }
}

pub struct TokioTaskHandle {
    handle: JoinHandle<Result<(), ExecutionError>>,
    recv: mpsc::Receiver<(
        ToSupervisor,
        oneshot::Sender<Result<ToTask, SupervisorCommsError>>,
    )>,
    send: Option<oneshot::Sender<Result<ToTask, SupervisorCommsError>>>,
}

impl TaskHandle for TokioTaskHandle {
    fn abort(&self) {
        self.handle.abort();
    }

    async fn service(&mut self, timeout: Duration) -> ServiceResult {
        let timeout = tokio::time::Duration::from_secs(timeout.as_secs());

        tokio::select! {
            // handle incoming messages from the task
            msg = self.recv.recv(), if !self.recv.is_closed() => {
                match msg {
                    Some((msg, send)) => {
                        self.send = Some(send);
                        ServiceResult::Comms(msg)
                    },
                    // Channel closed.
                    None => ServiceResult::None
                }
            },
            // task runner has terminated
            r = &mut self.handle => match r {
                Ok(r) => ServiceResult::Terminated(r),
                Err(e) => {
                    if e.is_panic() {
                        ExecutionError::Panicked.into()
                    } else {
                        ExecutionError::Cancelled.into()
                    }
                }
            },
            _ = tokio::time::sleep(timeout) => ServiceResult::None
        }
    }

    async fn respond(&mut self, msg: Result<ToTask, SupervisorCommsError>) {
        match self.send.take() {
            Some(sender) => {
                if sender.send(msg).is_err() {
                    error!("Failed to respond to task. Task is gone.");
                }
            }
            None => panic!("No sender available to send message to task"),
        }
    }
}

pub struct TokioTaskRuntime {
    hostname: String,
    unixname: String,
}

impl Default for TokioTaskRuntime {
    fn default() -> Self {
        let hostname = whoami::fallible::hostname().expect("Could not get hostname");
        let unixname = whoami::fallible::username().expect("Could not get username");
        TokioTaskRuntime { hostname, unixname }
    }
}

impl<T> TaskRuntime<T> for TokioTaskRuntime
where
    T: TimeProvider + Send + Sync + 'static,
{
    type TaskHandle = TokioTaskHandle;
    type Instant = std::time::Instant;

    fn hostname(&self) -> &str {
        &self.hostname
    }

    fn unixname(&self) -> &str {
        &self.unixname
    }

    fn pid(&self) -> u32 {
        process::id()
    }

    fn now(&self) -> Self::Instant {
        Self::Instant::now()
    }

    fn elapsed(&self, start: Self::Instant) -> Duration {
        start.elapsed()
    }

    fn start(
        &self,
        time_provider: T,
        details: StartupDetails,
        dag_bag: &'static DagBag,
    ) -> Self::TaskHandle {
        let (send, recv) = mpsc::channel(1);
        let comms = TokioSupervisorComms(send);
        let task_runner = TaskRunner::new(comms, time_provider);
        let handle = tokio::spawn(task_runner.main(details, dag_bag));
        TokioTaskHandle {
            handle,
            recv,
            send: None,
        }
    }
}

pub struct TokioSupervisorComms(
    mpsc::Sender<(
        ToSupervisor,
        oneshot::Sender<Result<ToTask, SupervisorCommsError>>,
    )>,
);

impl SupervisorComms for TokioSupervisorComms {
    async fn send(&self, msg: ToSupervisor) -> Result<ToTask, SupervisorCommsError> {
        let (send, recv) = oneshot::channel();
        self.0
            .send((msg, send))
            .await
            .map_err(|e| SupervisorCommsError::Send(e.to_string()))?;
        match recv.await {
            Ok(r) => r,
            Err(e) => Err(SupervisorCommsError::SupervisorGone(e.to_string())),
        }
    }
}
