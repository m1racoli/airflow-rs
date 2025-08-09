cfg_if::cfg_if! {
    if #[cfg(feature = "std")] {
        use std::error;
        use std::future;
    } else {
        extern crate alloc;
        use alloc::string::String;
        use alloc::vec::Vec;
        use core::error;
        use core::future;
    }
}

use airflow_common::{datetime::UtcDateTime, models::TaskInstanceKey, utils::TaskInstanceState};

use crate::models::{EdgeWorkerState, SysInfo};

use super::{EdgeJobFetched, HealthReturn, WorkerRegistrationReturn, WorkerSetStateReturn};

pub trait EdgeApiClient {
    type Error: error::Error;

    /// Health check of the Edge API.
    fn health(&mut self) -> impl future::Future<Output = Result<HealthReturn, Self::Error>> + Send;

    /// Register worker with the Edge API.
    fn worker_register(
        &mut self,
        hostname: &str,
        state: EdgeWorkerState,
        queues: Option<&Vec<String>>,
        sysinfo: &SysInfo,
    ) -> impl future::Future<Output = Result<WorkerRegistrationReturn, Self::Error>> + Send;

    /// Update the state of the worker in the central site and thereby implicitly heartbeat.
    fn worker_set_state(
        &mut self,
        hostname: &str,
        state: EdgeWorkerState,
        jobs_active: usize,
        queues: Option<&Vec<String>>,
        sysinfo: &SysInfo,
        maintenance_comments: Option<&str>,
    ) -> impl future::Future<Output = Result<WorkerSetStateReturn, Self::Error>> + Send;

    /// Fetch a job to execute on the edge worker.
    fn jobs_fetch(
        &mut self,
        hostname: &str,
        queues: Option<&Vec<String>>,
        free_concurrency: usize,
    ) -> impl future::Future<Output = Result<Option<EdgeJobFetched>, Self::Error>> + Send;

    /// Set the state of a job.
    fn jobs_set_state(
        &mut self,
        key: &TaskInstanceKey,
        state: TaskInstanceState,
    ) -> impl future::Future<Output = Result<(), Self::Error>> + Send;

    /// Elaborate the path and filename to expect from task execution.
    fn logs_logfile_path(
        &mut self,
        key: &TaskInstanceKey,
    ) -> impl future::Future<Output = Result<String, Self::Error>> + Send;

    /// Push an incremental log chunk from Edge Worker to central site.
    fn logs_push(
        &mut self,
        key: &TaskInstanceKey,
        log_chunk_time: &UtcDateTime,
        log_chunk_data: &str,
    ) -> impl future::Future<Output = Result<(), Self::Error>> + Send;
}
