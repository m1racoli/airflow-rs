extern crate alloc;
use alloc::string::ToString;
use core::cmp;
use core::time;

use airflow_common::{
    datetime::TimeProvider,
    executors::{ExecuteTask, TaskInstance},
};
use log::{debug, error, info, warn};

use crate::{
    api::{ExecutionApiError, LocalExecutionApiClient, LocalExecutionApiClientFactory},
    definitions::DagBag,
    execution::{
        ExecutionError, ExecutionResultTIState, StartupDetails, SupervisorCommsError, TaskRuntime,
        ToSupervisor, ToTask,
        runtime::{LocalTaskHandle, ServiceResult},
    },
};

// TODO conf
static HEARTBEAT_TIMEOUT: u64 = 300; // seconds
static MIN_HEARTBEAT_INTERVAL: u64 = 5; // seconds
static MAX_FAILED_HEARTBEATS: usize = 3;

pub async fn supervise<F, R>(
    task: ExecuteTask,
    client_factory: F,
    dag_bag: &'static DagBag<R>,
    runtime: &R,
    server: &str,
) -> bool
where
    F: LocalExecutionApiClientFactory + 'static,
    R: TaskRuntime,
{
    let ti = task.ti();
    debug!("Supervising task: {:?}", ti);
    let start = runtime.now();

    // logging setup?
    // init secrets backend?

    let client = match client_factory.create(server, task.token()) {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to create execution API client: {}", e);
            return false;
        }
    };

    let active_task = match ActivityTask::start(ti, client, dag_bag, runtime).await {
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
enum ActivityError<C: LocalExecutionApiClient> {
    #[error("Server terminated")]
    ServerTerminated,
    #[error("Execution API error: {0}")]
    ExecutionApi(#[from] ExecutionApiError<C::Error>),
}

/// This is an equivalent of ActiveSubprocess in original Airflow, but async.
struct ActivityTask<'a, C: LocalExecutionApiClient, R: TaskRuntime> {
    ti: &'a TaskInstance,
    client: C,
    handle: R::TaskHandle,
    last_successful_heartbeat: R::Instant,
    last_heartbeat_attempt: R::Instant,
    failed_heartbeats: usize,
    server_terminated: bool,
    terminal_state: Option<ExecutionResultTIState>,
    result: Option<Result<(), ExecutionError>>,
    runtime: &'a R,
    task_end_time: Option<R::Instant>,
}

impl<'a, C, R> ActivityTask<'a, C, R>
where
    C: LocalExecutionApiClient + 'static,
    R: TaskRuntime,
{
    async fn start(
        what: &'a TaskInstance,
        mut client: C,
        dag_bag: &'static DagBag<R>,
        runtime: &'a R,
    ) -> Result<Self, ExecutionApiError<C::Error>> {
        let id = what.id();
        let start = runtime.time_provider().now();

        let ti_context = client
            .task_instances_start(
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

        let handle = runtime.start(details, dag_bag);

        Ok(Self {
            ti: what,
            client,
            handle,
            last_successful_heartbeat,
            last_heartbeat_attempt: last_successful_heartbeat,
            failed_heartbeats: 0,
            server_terminated: false,
            runtime,
            // has_error: ,
            terminal_state: None,
            result: None,
            task_end_time: None,
        })
    }

    async fn wait(mut self) -> Result<ExecutionResultTIState, ActivityError<C>> {
        self.monitor().await?;
        self.update_task_state_if_needed().await?;
        Ok(self.final_state())
    }

    async fn monitor(&mut self) -> Result<(), ActivityError<C>> {
        loop {
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

            match self.handle.service(max_wait_time).await {
                ServiceResult::Comms(msg) => {
                    let response = self.handle_message(msg).await;
                    self.handle.respond(response).await;
                }
                ServiceResult::None => {}
                ServiceResult::Terminated(r) => {
                    self.result = Some(r);
                    break;
                }
            }

            self.send_heartbeat_if_needed().await?;
            if self.server_terminated {
                return Err(ActivityError::ServerTerminated);
            }

            self.handle_process_overtime_if_needed().await?;
        }
        Ok(())
    }

    async fn handle_message(&mut self, msg: ToSupervisor) -> Result<ToTask, SupervisorCommsError> {
        match msg {
            ToSupervisor::SucceedTask(payload) => {
                self.terminal_state = Some(ExecutionResultTIState::Success);
                self.task_end_time = Some(self.runtime.now());
                self.client
                    .task_instances_succeed(
                        &self.ti.id(),
                        &self.runtime.time_provider().now(),
                        &payload.task_outlets,
                        &payload.outlet_events,
                        None,
                    )
                    .await?;
                Ok(ToTask::Empty)
            }
            ToSupervisor::GetXCom {
                key,
                dag_id,
                run_id,
                task_id,
                map_index,
                include_prior_dates,
            } => {
                let response = self
                    .client
                    .xcoms_get(
                        &dag_id,
                        &run_id,
                        &task_id,
                        &key,
                        map_index,
                        include_prior_dates,
                    )
                    .await?;
                Ok(ToTask::XCom(response))
            }
            ToSupervisor::GetXComCount {
                key,
                dag_id,
                run_id,
                task_id,
            } => {
                let count = self
                    .client
                    .xcoms_head(&dag_id, &run_id, &task_id, &key)
                    .await?;
                Ok(ToTask::XComCount(count))
            }
            ToSupervisor::SetXCom {
                key,
                value,
                dag_id,
                run_id,
                task_id,
                map_index,
                mapped_length,
            } => {
                self.client
                    .xcoms_set(
                        &dag_id,
                        &run_id,
                        &task_id,
                        &key,
                        &value,
                        map_index,
                        mapped_length,
                    )
                    .await?;
                Ok(ToTask::Empty)
            }
            ToSupervisor::DeleteXCom {
                key,
                dag_id,
                run_id,
                task_id,
                map_index,
            } => {
                self.client
                    .xcoms_delete(&dag_id, &run_id, &task_id, &key, map_index)
                    .await?;
                Ok(ToTask::Empty)
            }
        }
    }

    fn final_state(&self) -> ExecutionResultTIState {
        match &self.result {
            Some(r) => match r {
                Err(_) => ExecutionResultTIState::Failed,
                Ok(_) => match self.terminal_state {
                    Some(state) => state,
                    None => ExecutionResultTIState::Success,
                },
            },
            None => panic!("final_state called before task finished"),
        }
    }

    async fn send_heartbeat_if_needed(&mut self) -> Result<(), ExecutionApiError<C::Error>> {
        if self.runtime.elapsed(self.last_heartbeat_attempt).as_secs() < MIN_HEARTBEAT_INTERVAL {
            return Ok(());
        }

        if self.terminal_state.is_some() {
            // If the task has finished, and we are in "overtime" (running OL listeners etc) we shouldn't heartbeat
            return Ok(());
        }

        self.last_heartbeat_attempt = self.runtime.now();
        debug!("{}: Sending heartbeat", self.ti.id());
        match self
            .client
            .task_instances_heartbeat(&self.ti.id(), self.runtime.hostname(), self.runtime.pid())
            .await
        {
            Ok(()) => {
                self.last_successful_heartbeat = self.runtime.now();
                self.failed_heartbeats = 0;
                Ok(())
            }
            Err(ExecutionApiError::NotFound(detail)) => {
                info!(
                    "{}: Server indicated the task shouldn't be running anymore; terminating task: {detail}",
                    self.ti.id()
                );
                self.handle.abort();
                self.server_terminated = true;
                Ok(())
            }
            Err(ExecutionApiError::Conflict(detail)) => {
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
                    Err(e)
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

    async fn handle_process_overtime_if_needed(
        &mut self,
    ) -> Result<(), ExecutionApiError<C::Error>> {
        if self.terminal_state.is_none() {
            return Ok(());
        }
        // TODO handle over time
        Ok(())
    }

    async fn update_task_state_if_needed(&mut self) -> Result<(), ExecutionApiError<C::Error>> {
        let id = self.ti.id();
        let end = self.runtime.time_provider().now();

        match self.final_state().into() {
            Some(state) => {
                self.client
                    .task_instances_finish(&id, state, &end, None)
                    .await?;
                Ok(())
            }
            None => Ok(()),
        }
    }
}
