cfg_if::cfg_if! {
    if #[cfg(feature = "std")] {
        use std::time;
    } else {
        extern crate alloc;
        use alloc::string::String;
        use alloc::string::ToString;
        use alloc::vec::Vec;
        use core::time;
    }
}

use crate::api::{EdgeApiError, EdgeJobFetched, LocalEdgeApiClient};
use crate::models::{EdgeWorkerState, SysInfo};
use crate::worker::{IntercomMessage, LocalEdgeJob, LocalWorkerRuntime};
use airflow_common::datetime::{MIN_UTC, TimeProvider, UtcDateTime};
use airflow_common::models::{TaskInstanceKey, TaskInstanceLike};
use airflow_common::utils::TaskInstanceState;
use airflow_task_sdk::definitions::DagBag;
use tracing::{debug, error, info};

pub static EDGE_HEARTBEAT_INTERVAL: u64 = 30; // seconds
pub static EDGE_JOB_POLL_INTERVAL: u64 = 5; // seconds

#[derive(Debug, Clone)]
pub struct WorkerState {
    concurrency: usize,
    used_concurrency: usize,
    last_heartbeat: UtcDateTime,
    drain: bool,
    maintenance_comments: Option<String>,
    maintenance_mode: bool,
    airflow_version: &'static str,
    edge_provider_version: &'static str,
}

impl WorkerState {
    pub fn concurrency(&self) -> usize {
        self.concurrency
    }

    pub fn free_concurrency(&self) -> usize {
        self.concurrency - self.used_concurrency
    }

    pub fn used_concurrency(&self) -> usize {
        self.used_concurrency
    }

    pub fn last_heartbeat(&self) -> UtcDateTime {
        self.last_heartbeat
    }

    /// Get the system information of the edge worker.
    pub fn sys_info(&self) -> SysInfo {
        SysInfo {
            airflow_version: self.airflow_version.to_string(),
            edge_provider_version: self.edge_provider_version.to_string(),
            concurrency: self.concurrency(),
            free_concurrency: self.free_concurrency(),
        }
    }

    pub fn get_state(&self) -> EdgeWorkerState {
        if self.last_heartbeat == MIN_UTC {
            return EdgeWorkerState::Starting;
        } else if self.used_concurrency > 0 {
            if self.drain {
                return EdgeWorkerState::Terminating;
            }
            if self.maintenance_mode {
                return EdgeWorkerState::MaintenancePending;
            }
            return EdgeWorkerState::Running;
        }
        if self.drain {
            if self.maintenance_mode {
                return EdgeWorkerState::OfflineMaintenance;
            }
            return EdgeWorkerState::Offline;
        }
        if self.maintenance_mode {
            return EdgeWorkerState::MaintenanceMode;
        }
        EdgeWorkerState::Idle
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EdgeWorkerError<C: LocalEdgeApiClient> {
    #[error(transparent)]
    EdgeApi(EdgeApiError<C::Error>),
}

pub struct EdgeWorker<C: LocalEdgeApiClient, T: TimeProvider, R: LocalWorkerRuntime> {
    client: C,
    state_changed: bool,
    jobs: Vec<R::Job>,
    queues: Option<Vec<String>>,
    time_provider: T,
    runtime: R,
    state: WorkerState,
    dag_bag: &'static DagBag<R::TaskRuntime>,
}

impl<C: LocalEdgeApiClient, T: TimeProvider, R: LocalWorkerRuntime> EdgeWorker<C, T, R> {
    pub fn new(
        client: C,
        time_provider: T,
        runtime: R,
        dag_bag: &'static DagBag<R::TaskRuntime>,
    ) -> Self {
        // TODO a better way to set these for the API server
        // https://github.com/m1racoli/airflow-rs/issues/1
        let airflow_version = env!("AIRFLOW_VERSION");
        let edge_provider_version = env!("EDGE_PROVIDER_VERSION");

        let state = WorkerState {
            used_concurrency: 0,
            concurrency: runtime.concurrency(),
            last_heartbeat: MIN_UTC,
            drain: false,
            maintenance_comments: None,
            maintenance_mode: false,
            airflow_version,
            edge_provider_version,
        };
        EdgeWorker {
            client,
            state,
            state_changed: false,
            jobs: Vec::new(),
            queues: None,
            time_provider,
            runtime,
            dag_bag,
        }
    }

    pub fn with_queues(mut self, queues: Option<Vec<String>>) -> Self {
        self.queues = queues;
        self
    }

    pub async fn start(mut self) -> Result<(), EdgeWorkerError<C>> {
        self.runtime.on_update(&self.state).await;
        info!("Starting worker {} ...", self.runtime.hostname());

        let registration_response = self
            .client
            .worker_register(
                self.runtime.hostname(),
                EdgeWorkerState::Starting,
                self.queues.as_ref(),
                &self.state.sys_info(),
            )
            .await
            .map_err(EdgeWorkerError::EdgeApi)?;
        self.state.last_heartbeat = registration_response.last_update;
        info!("Worker registered.");

        self.state_changed = self.heartbeat().await?;

        while !self.state.drain || !self.jobs.is_empty() {
            self.do_loop().await?;
        }

        self.runtime.on_update(&self.state).await;
        info!("Stopping worker.");
        self.client
            .worker_set_state(
                self.runtime.hostname(),
                if self.state.maintenance_mode {
                    EdgeWorkerState::OfflineMaintenance
                } else {
                    EdgeWorkerState::Offline
                },
                0,
                self.queues.as_ref(),
                &self.state.sys_info(),
                self.state.maintenance_comments.as_deref(),
            )
            .await
            .map_err(EdgeWorkerError::EdgeApi)?;
        // TODO handle version mismatch
        info!("Worker stopped.");
        Ok(())
    }

    async fn do_loop(&mut self) -> Result<(), EdgeWorkerError<C>> {
        let mut new_job = false;
        let jobs_was_empty = self.jobs.is_empty();
        let was_full = self.state.free_concurrency() == 0;

        if !(self.state.drain || self.state.maintenance_mode) && self.state.free_concurrency() > 0 {
            new_job = self.fetch_job().await?;
        }
        self.check_running_jobs().await?;

        if self.state.drain
            || (self.time_provider.now() - self.state.last_heartbeat).num_seconds()
                > EDGE_HEARTBEAT_INTERVAL as i64
            || self.state_changed
            || jobs_was_empty != self.jobs.is_empty()
        {
            self.state_changed = self.heartbeat().await?;
        }

        // TODO we also receive messages here, should we skip that?
        // sleep if there was no new job and no free slots we made available in this iteration
        if !new_job && (!was_full || self.state.free_concurrency() == 0) {
            self.sleep().await;
        }

        Ok(())
    }

    async fn check_running_jobs(&mut self) -> Result<(), EdgeWorkerError<C>> {
        let mut used_concurrency: usize = 0;

        //TODO probably there's a better way to check for running/success, send result and retain jobs
        let mut results: Vec<(TaskInstanceKey, TaskInstanceState)> = Vec::new();

        for job in self.jobs.iter_mut() {
            debug!("Checking job: {}", job.ti_key());
            if job.is_running() {
                used_concurrency += job.concurrency_slots();
            } else {
                let state = if job.is_success().await {
                    info!("Job finished: {}", job.ti_key());
                    TaskInstanceState::Success
                } else {
                    error!("Job failed: {}", job.ti_key());
                    TaskInstanceState::Failed
                };
                results.push((job.ti_key().clone(), state));
            }
        }

        for result in results.iter() {
            self.client
                .jobs_set_state(&result.0, result.1)
                .await
                .map_err(EdgeWorkerError::EdgeApi)?;
        }

        self.jobs.retain(|job| job.is_running());
        self.state.used_concurrency = used_concurrency;
        self.runtime.on_update(&self.state).await;
        Ok(())
    }

    async fn fetch_job(&mut self) -> Result<bool, EdgeWorkerError<C>> {
        debug!("Attempting to fetch a new job...");
        let edge_job = self
            .client
            .jobs_fetch(
                self.runtime.hostname(),
                self.queues.as_ref(),
                self.state.free_concurrency(),
            )
            .await
            .map_err(EdgeWorkerError::EdgeApi)?;

        match edge_job {
            Some(job) => {
                let ti_key = job.ti_key();
                info!("Received job: {}", ti_key);
                self.launch_job(job).await;
                self.client
                    .jobs_set_state(&ti_key, TaskInstanceState::Running)
                    .await
                    .map_err(EdgeWorkerError::EdgeApi)?;
                Ok(true)
            }
            None => {
                debug!("No new job to process, {} jobs running", self.jobs.len());
                Ok(false)
            }
        }
    }

    async fn launch_job(&mut self, job: EdgeJobFetched) -> () {
        let job = self.runtime.launch(job, self.dag_bag);
        self.jobs.push(job);
    }

    async fn sleep(&mut self) -> () {
        if let Some(msg) = self
            .runtime
            .sleep(time::Duration::from_secs(EDGE_JOB_POLL_INTERVAL))
            .await
        {
            debug!("Received intercom message: {:?}", msg);
            match msg {
                IntercomMessage::Shutdown => {
                    info!(
                        "Request to shut down Edge Worker received, waiting for jobs to complete."
                    );
                    self.state.drain = true;
                }
                IntercomMessage::JobCompleted(key) => {
                    debug!("Received job completed for {}", key);
                }
                IntercomMessage::Terminate => {
                    info!("Request to terminate Edge Worker received, stopping immediately.");
                    self.state.drain = true;
                    self.jobs.iter_mut().for_each(|j| j.abort());
                }
            }
            self.runtime.on_update(&self.state).await;
        }
    }

    async fn heartbeat(&mut self) -> Result<bool, EdgeWorkerError<C>> {
        debug!("Sending heartbeat");
        let state = self.state.get_state();

        let worker_info = match self
            .client
            .worker_set_state(
                self.runtime.hostname(),
                state,
                self.jobs.len(),
                self.queues.as_ref(),
                &self.state.sys_info(),
                self.state.maintenance_comments.as_deref(),
            )
            .await
        {
            Ok(info) => info,
            Err(EdgeApiError::VersionMismatch(e)) => {
                error!("Worker version mismatch, exiting");
                error!("{}", e);
                self.state.drain = true;
                return Ok(false);
            }
            Err(e) => return Err(EdgeWorkerError::EdgeApi(e)),
        };

        self.queues = worker_info.queues;

        if worker_info.state == EdgeWorkerState::MaintenanceRequest {
            info!("Maintenance mode requested!");
            self.state.maintenance_mode = true;
        } else if (worker_info.state == EdgeWorkerState::Idle
            || worker_info.state == EdgeWorkerState::Running)
            && self.state.maintenance_mode
        {
            info!("Maintenance mode exit requested!");
            self.state.maintenance_mode = false;
        }

        if self.state.maintenance_mode {
            self.state.maintenance_comments = worker_info.maintenance_comments;
        } else {
            self.state.maintenance_comments = None;
        }

        info!("Heartbeat sent, state: {:?}", worker_info.state);
        self.state.last_heartbeat = self.time_provider.now();
        self.runtime.on_update(&self.state).await;
        Ok(worker_info.state != state)
    }
}
