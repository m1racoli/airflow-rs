use std::{
    pin::Pin,
    process,
    task::{Context, Poll},
    time::Duration,
};

use airflow_common::{
    datetime::{StdTimeProvider, TimeProvider},
    executors::ExecuteTask,
    models::{TaskInstanceKey, TaskInstanceLike},
};
use airflow_edge_sdk::{
    api::EdgeJobFetched,
    worker::{EdgeJob, Intercom, IntercomMessage, WorkerRuntime, WorkerState},
};
use airflow_task_sdk::{
    api::ExecutionApiClient,
    definitions::DagBag,
    execution::{
        ExecutionError, ExecutionResultTIState, StartupDetails, TaskHandle, TaskRunner,
        TaskRuntime, supervise,
    },
};
use log::debug;
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::{error, info};

use crate::StdExecutionApiClientFactory;

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

pub struct TokioRuntime {
    concurrency: usize,
    recv: mpsc::Receiver<IntercomMessage>,
    send: mpsc::Sender<IntercomMessage>,
    hostname: String,
}

impl TokioRuntime {
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }
}

impl Default for TokioRuntime {
    fn default() -> Self {
        let (send, recv) = mpsc::channel(1);
        let hostname = whoami::fallible::hostname().expect("Could not get hostname");
        TokioRuntime {
            recv,
            send,
            concurrency: 1,
            hostname,
        }
    }
}

impl WorkerRuntime for TokioRuntime {
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
        async fn _launch(
            task: ExecuteTask,
            intercom: TokioIntercom,
            dag_bag: &'static DagBag,
        ) -> bool {
            let key = task.ti().ti_key();
            info!(ti_key = key.to_string(), "Worker task launched.");

            // TODO should supervise instantiate the client? we need a client factory?
            // TODO should this be passed from somewhere?
            let time_provider = StdTimeProvider;
            let runtime = TokioTaskRuntime::default();
            let client_factory = StdExecutionApiClientFactory::default();

            supervise(task, client_factory, time_provider, dag_bag, &runtime).await;
            intercom.send(IntercomMessage::JobCompleted(key)).await.ok();
            true
        }
        let ti_key = job.ti_key();
        let intercom = self.intercom();
        let handle = tokio::spawn(_launch(job.command, intercom, dag_bag));
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

pub struct TokioTaskHandle<C: ExecutionApiClient>(
    JoinHandle<Result<ExecutionResultTIState, ExecutionError<C>>>,
);

impl<C> TaskHandle<C> for TokioTaskHandle<C>
where
    C: ExecutionApiClient,
    C::Error: Send,
{
    fn abort(&self) {
        self.0.abort();
    }
}

impl<C> Future for TokioTaskHandle<C>
where
    C: ExecutionApiClient,
    C::Error: Send,
{
    type Output = Result<ExecutionResultTIState, ExecutionError<C>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let pinned = Pin::new(&mut this.0);
        match pinned.poll(cx) {
            Poll::Ready(result) => match result {
                Ok(res) => Poll::Ready(res),
                Err(e) => {
                    if e.is_panic() {
                        error!("Activity task panicked: {}", e);
                    }
                    Poll::Ready(Ok(ExecutionResultTIState::Failed))
                }
            },
            Poll::Pending => Poll::Pending,
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

impl<C, T> TaskRuntime<C, T> for TokioTaskRuntime
where
    C: ExecutionApiClient + Send + Sync + 'static,
    C::Error: Send,
    T: TimeProvider + Send + Sync + 'static,
{
    type ActivityHandle = TokioTaskHandle<C>;
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
        client: C,
        time_provider: T,
        details: StartupDetails,
        dag_bag: &'static DagBag,
    ) -> Self::ActivityHandle {
        let task_runner = TaskRunner::new(client, time_provider);
        let handle = tokio::spawn(task_runner.main(details, dag_bag));
        TokioTaskHandle(handle)
    }

    async fn wait(
        &self,
        handle: &mut Self::ActivityHandle,
        duration: Duration,
    ) -> Option<Result<ExecutionResultTIState, ExecutionError<C>>> {
        let max_wait_time = duration.as_secs();
        tokio::select! {
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(max_wait_time)) => {
                None
            }
            r = handle => {
                Some(r)
            }
        }
    }
}
