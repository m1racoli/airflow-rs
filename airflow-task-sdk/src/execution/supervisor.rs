cfg_if::cfg_if! {
    if #[cfg(feature = "std")] {
        use std::cmp;
        use std::time;
    } else {
        extern crate alloc;
        use alloc::string::ToString;
        use core::cmp;
        use core::time;
    }
}

use crate::{
    api::{
        ExecutionApiClient, ExecutionApiClientFactory, ExecutionApiError, TaskInstanceApiClient,
        TaskInstanceApiError,
    },
    definitions::DagBag,
    execution::{ExecutionResultTIState, LocalTaskHandle, LocalTaskRuntime, StartupDetails},
};
use airflow_common::{
    datetime::TimeProvider,
    executors::{ExecuteTask, TaskInstance},
};
use log::{debug, error, info, warn};

// TODO conf
static HEARTBEAT_TIMEOUT: u64 = 300; // seconds
static MIN_HEARTBEAT_INTERVAL: u64 = 5; // seconds
static MAX_FAILED_HEARTBEATS: usize = 3;
static EXECUTION_API_SERVER_URL: &str = "http://localhost:28080/execution";

pub async fn supervise<F, T, R>(
    task: ExecuteTask,
    client_factory: F,
    time_provider: T,
    dag_bag: &'static DagBag,
    runtime: &R,
) -> bool
where
    F: ExecutionApiClientFactory,
    F::Client: Clone + 'static,
    T: TimeProvider + Clone + 'static,
    R: LocalTaskRuntime<F::Client, T>,
{
    let ti = task.ti();
    debug!("Supervising task: {:?}", ti);
    let start = runtime.now();

    // logging setup?
    // init secrets backend?

    let client = match client_factory.create(EXECUTION_API_SERVER_URL, task.token()) {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to create execution API client: {}", e);
            return false;
        }
    };

    let active_task = match ActivityTask::start(ti, client, dag_bag, time_provider, runtime).await {
        Ok(task) => task,
        Err(e) => {
            error!("Failed to start task: {}", e);
            return false;
        }
    };

    let state = match active_task.wait().await {
        Ok(state) => state.to_string(),
        Err(e) => match e {
            ActivityError::ServerTerminated => "SERVER_TERMINATED".to_string(),
            e => {
                error!("Failed to complete task: {}", e);
                return false;
            }
        },
    };

    let duration = runtime.elapsed(start);
    info!(
        "Task finished after {} seconds with state={}",
        duration.as_secs_f64(),
        state,
    );
    true
}

#[derive(thiserror::Error, Debug)]
enum ActivityError<C: ExecutionApiClient> {
    #[error("Server terminated")]
    ServerTerminated,
    #[error("Execution API error: {0}")]
    ExecutionApi(#[from] ExecutionApiError<C::Error>),
}

/// This is an equivalent of ActiveSubprocess in original Airflow, but async.
struct ActivityTask<'a, C: ExecutionApiClient, T: TimeProvider, R: LocalTaskRuntime<C, T>> {
    ti: &'a TaskInstance,
    client: C,
    time_provider: T,
    handle: R::ActivityHandle,
    last_successful_heartbeat: R::Instant,
    last_heartbeat_attempt: R::Instant,
    failed_heartbeats: usize,
    server_terminated: bool,
    runtime: &'a R,
}

impl<'a, C, T, R> ActivityTask<'a, C, T, R>
where
    C: ExecutionApiClient + Clone + 'static,
    T: TimeProvider + Clone + 'static,
    R: LocalTaskRuntime<C, T>,
{
    async fn start(
        what: &'a TaskInstance,
        client: C,
        dag_bag: &'static DagBag,
        time_provider: T,
        runtime: &'a R,
    ) -> Result<Self, ExecutionApiError<C::Error>> {
        let id = what.id();
        let start = time_provider.now();

        let ti_context = client
            .task_instances()
            .start(
                &id,
                runtime.hostname(),
                runtime.unixname(),
                runtime.pid(),
                &start,
            )
            .await?;

        let last_successful_heartbeat = runtime.now();
        let details = StartupDetails {
            ti: what.clone(),
            start_date: start,
            ti_context,
        };

        let handle = runtime.start(client.clone(), time_provider.clone(), details, dag_bag);

        Ok(Self {
            ti: what,
            client,
            time_provider,
            handle,
            last_successful_heartbeat,
            last_heartbeat_attempt: last_successful_heartbeat,
            failed_heartbeats: 0,
            server_terminated: false,
            runtime,
        })
    }

    async fn wait(mut self) -> Result<ExecutionResultTIState, ActivityError<C>> {
        let result = loop {
            let max_wait_time = cmp::max(
                0,
                cmp::min(
                    (HEARTBEAT_TIMEOUT as f64 * 0.75) as u64
                        - self
                            .runtime
                            .elapsed(self.last_successful_heartbeat)
                            .as_secs(),
                    MIN_HEARTBEAT_INTERVAL,
                ),
            );

            let max_wait_time = time::Duration::from_secs(max_wait_time);

            if let Some(r) = self.runtime.wait(&mut self.handle, max_wait_time).await {
                break r;
            }

            self.send_heartbeat_if_needed().await?;
            if self.server_terminated {
                return Err(ActivityError::ServerTerminated);
            }
        };

        let state = match result {
            Ok(state) => state,
            Err(e) => {
                error!("Task execution failed: {e}");
                ExecutionResultTIState::Failed
            }
        };

        self.update_task_state_if_needed(state).await?;
        Ok(state)
    }

    async fn send_heartbeat_if_needed(&mut self) -> Result<(), ExecutionApiError<C::Error>> {
        // TODO don't heartbeat if task has completed
        if self.runtime.elapsed(self.last_heartbeat_attempt).as_secs() < MIN_HEARTBEAT_INTERVAL {
            return Ok(());
        }

        self.last_heartbeat_attempt = self.runtime.now();
        debug!("{}: Sending heartbeat", self.ti.id());
        match self
            .client
            .task_instances()
            .heartbeat(&self.ti.id(), self.runtime.hostname(), self.runtime.pid())
            .await
        {
            Ok(()) => {
                self.last_successful_heartbeat = self.runtime.now();
                self.failed_heartbeats = 0;
                Ok(())
            }
            Err(TaskInstanceApiError::NotFound(detail)) => {
                info!(
                    "{}: Server indicated the task shouldn't be running anymore; terminating task: {detail}",
                    self.ti.id()
                );
                self.handle.abort();
                self.server_terminated = true;
                Ok(())
            }
            Err(TaskInstanceApiError::Conflict(detail)) => {
                info!(
                    "{}: Server indicated the task shouldn't be running anymore; terminating task: {detail}",
                    self.ti.id()
                );
                self.handle.abort();
                self.server_terminated = true;
                Ok(())
            }
            Err(e) => {
                // error!("Error sending heartbeat: {}", e);
                self.failed_heartbeats += 1;
                if self.failed_heartbeats >= MAX_FAILED_HEARTBEATS {
                    error!("{}: {e}", self.ti.id());
                    error!(
                        "{}: Too many failed heartbeats; terminating task",
                        self.ti.id()
                    );
                    self.handle.abort();
                    Err(ExecutionApiError::TaskInstance(e))
                } else {
                    warn!(
                        "{}: Failed to send heartbeat ({} of {}): {e}",
                        self.ti.id(),
                        self.failed_heartbeats,
                        MAX_FAILED_HEARTBEATS
                    );
                    Ok(())
                }
            }
        }
    }

    async fn update_task_state_if_needed(
        &self,
        state: ExecutionResultTIState,
    ) -> Result<(), ExecutionApiError<C::Error>> {
        let id = self.ti.id();
        let end = self.time_provider.now();

        match state.into() {
            Some(state) => {
                self.client
                    .task_instances()
                    .finish(&id, state, &end, None)
                    .await?;
                Ok(())
            }
            None => Ok(()),
        }
    }
}
