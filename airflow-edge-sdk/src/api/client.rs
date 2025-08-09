cfg_if::cfg_if! {
    if #[cfg(feature = "std")] {
        use std::error;
    } else {
        extern crate alloc;
        use alloc::string::String;
        use alloc::vec::Vec;
        use core::error;
    }
}

use airflow_common::{datetime::UtcDateTime, models::TaskInstanceKey, utils::TaskInstanceState};

use crate::models::{EdgeWorkerState, SysInfo};

use super::{EdgeJobFetched, HealthReturn, WorkerRegistrationReturn, WorkerSetStateReturn};

#[trait_variant::make(EdgeApiClient: Send)]
pub trait LocalEdgeApiClient {
    type Error: error::Error;

    /// Health check of the Edge API.
    async fn health(&mut self) -> Result<HealthReturn, Self::Error>;

    /// Register worker with the Edge API.
    async fn worker_register(
        &mut self,
        hostname: &str,
        state: EdgeWorkerState,
        queues: Option<&Vec<String>>,
        sysinfo: &SysInfo,
    ) -> Result<WorkerRegistrationReturn, Self::Error>;

    /// Update the state of the worker in the central site and thereby implicitly heartbeat.
    async fn worker_set_state(
        &mut self,
        hostname: &str,
        state: EdgeWorkerState,
        jobs_active: usize,
        queues: Option<&Vec<String>>,
        sysinfo: &SysInfo,
        maintenance_comments: Option<&str>,
    ) -> Result<WorkerSetStateReturn, Self::Error>;

    /// Fetch a job to execute on the edge worker.
    async fn jobs_fetch(
        &mut self,
        hostname: &str,
        queues: Option<&Vec<String>>,
        free_concurrency: usize,
    ) -> Result<Option<EdgeJobFetched>, Self::Error>;

    /// Set the state of a job.
    async fn jobs_set_state(
        &mut self,
        key: &TaskInstanceKey,
        state: TaskInstanceState,
    ) -> Result<(), Self::Error>;

    /// Elaborate the path and filename to expect from task execution.
    async fn logs_logfile_path(&mut self, key: &TaskInstanceKey) -> Result<String, Self::Error>;

    /// Push an incremental log chunk from Edge Worker to central site.
    async fn logs_push(
        &mut self,
        key: &TaskInstanceKey,
        log_chunk_time: &UtcDateTime,
        log_chunk_data: &str,
    ) -> Result<(), Self::Error>;
}
