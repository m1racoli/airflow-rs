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
use crate::worker::{IntercomMessage, LocalEdgeJob, LocalRuntime};
use airflow_common::datetime::{MIN_UTC, TimeProvider, UtcDateTime};
use airflow_common::models::{TaskInstanceKey, TaskInstanceLike};
use airflow_common::utils::TaskInstanceState;
use log::{debug, error, info};

pub static EDGE_HEARTBEAT_INTERVAL: u64 = 30; // seconds
pub static EDGE_JOB_POLL_INTERVAL: u64 = 5; // seconds

#[derive(Debug, thiserror::Error)]
pub enum EdgeWorkerError<C: LocalEdgeApiClient> {
    #[error(transparent)]
    EdgeApi(EdgeApiError<C::Error>),
}

#[derive(Debug)]
pub struct EdgeWorker<'a, C: LocalEdgeApiClient, T: TimeProvider, R: LocalRuntime> {
    hostname: &'a str,
    client: C,
    free_concurrency: usize,
    last_heartbeat: UtcDateTime,
    state_changed: bool,
    drain: bool,
    jobs: Vec<R::Job>,
    maintenance_comments: Option<String>,
    maintenance_mode: bool,
    queues: Option<Vec<String>>,
    time_provider: T,
    runtime: R,
}

impl<'a, C: LocalEdgeApiClient, T: TimeProvider, R: LocalRuntime> EdgeWorker<'a, C, T, R> {
    pub fn new(hostname: &'a str, client: C, time_provider: T, runtime: R) -> Self {
        EdgeWorker {
            hostname,
            client,
            free_concurrency: runtime.concurrency(),
            last_heartbeat: MIN_UTC,
            state_changed: false,
            drain: false,
            jobs: Vec::new(),
            maintenance_comments: None,
            maintenance_mode: false,
            queues: None,
            time_provider,
            runtime,
        }
    }

    pub fn with_queues(mut self, queues: Option<Vec<String>>) -> Self {
        self.queues = queues;
        self
    }

    pub async fn start(mut self) -> Result<(), EdgeWorkerError<C>> {
        info!("Starting worker {} ...", self.hostname);

        let registration_response = self
            .client
            .worker_register(
                self.hostname,
                EdgeWorkerState::Starting,
                self.queues.as_ref(),
                &self.sys_info(),
            )
            .await
            .map_err(EdgeWorkerError::EdgeApi)?;
        info!("Worker registered.");

        self.last_heartbeat = registration_response.last_update;

        self.state_changed = self.heartbeat().await?;
        self.last_heartbeat = self.time_provider.now();

        while !self.drain || !self.jobs.is_empty() {
            self.do_loop().await?;
        }

        info!("Stopping worker.");
        self.client
            .worker_set_state(
                self.hostname,
                if self.maintenance_mode {
                    EdgeWorkerState::OfflineMaintenance
                } else {
                    EdgeWorkerState::Offline
                },
                0,
                self.queues.as_ref(),
                &self.sys_info(),
                self.maintenance_comments.as_deref(),
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
        let was_full = self.free_concurrency == 0;

        if !(self.drain || self.maintenance_mode) && self.free_concurrency > 0 {
            new_job = self.fetch_job().await?;
        }
        self.check_running_jobs().await?;

        if self.drain
            || (self.time_provider.now() - self.last_heartbeat).num_seconds()
                > EDGE_HEARTBEAT_INTERVAL as i64
            || self.state_changed
            || jobs_was_empty != self.jobs.is_empty()
        {
            self.state_changed = self.heartbeat().await?;
            self.last_heartbeat = self.time_provider.now();
        }

        // sleep if there was no new job and no free slots we made available in this iteration
        if !new_job && (!was_full || self.free_concurrency == 0) {
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
        self.free_concurrency = self.runtime.concurrency() - used_concurrency;
        Ok(())
    }

    async fn fetch_job(&mut self) -> Result<bool, EdgeWorkerError<C>> {
        debug!("Attempting to fetch a new job...");
        let edge_job = self
            .client
            .jobs_fetch(self.hostname, self.queues.as_ref(), self.free_concurrency)
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
        let job = self.runtime.launch(job);
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
                    self.drain = true;
                }
                IntercomMessage::JobCompleted(key) => {
                    debug!("Received job completed for {}", key);
                }
                IntercomMessage::Terminate => {
                    info!("Request to terminate Edge Worker received, stopping immediately.");
                    self.drain = true;
                    self.jobs.iter_mut().for_each(|j| j.abort());
                }
            }
        }
    }

    async fn heartbeat(&mut self) -> Result<bool, EdgeWorkerError<C>> {
        debug!("Sending heartbeat");
        let state = self.get_state();

        let worker_info = match self
            .client
            .worker_set_state(
                self.hostname,
                state,
                self.jobs.len(),
                self.queues.as_ref(),
                &self.sys_info(),
                self.maintenance_comments.as_deref(),
            )
            .await
        {
            Ok(info) => info,
            Err(EdgeApiError::VersionMismatch(e)) => {
                error!("Worker version mismatch, exiting");
                error!("{}", e);
                self.drain = true;
                return Ok(false);
            }
            Err(e) => return Err(EdgeWorkerError::EdgeApi(e)),
        };

        self.queues = worker_info.queues;

        if worker_info.state == EdgeWorkerState::MaintenanceRequest {
            info!("Maintenance mode requested!");
            self.maintenance_mode = true;
        } else if (worker_info.state == EdgeWorkerState::Idle
            || worker_info.state == EdgeWorkerState::Running)
            && self.maintenance_mode
        {
            info!("Maintenance mode exit requested!");
            self.maintenance_mode = false;
        }

        if self.maintenance_mode {
            self.maintenance_comments = worker_info.maintenance_comments;
        } else {
            self.maintenance_comments = None;
        }

        info!("Heartbeat sent, state: {:?}", worker_info.state);
        Ok(worker_info.state != state)
    }

    /// Get the system information of the edge worker.
    fn sys_info(&self) -> SysInfo {
        SysInfo {
            airflow_version: "3.1.0".to_string(),
            edge_provider_version: "1.1.3".to_string(),
            concurrency: self.runtime.concurrency(),
            free_concurrency: self.free_concurrency,
        }
    }

    fn get_state(&self) -> EdgeWorkerState {
        if !self.jobs.is_empty() {
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
