use airflow_common::{datetime::DateTime, models::TaskInstanceKey, utils::TaskInstanceState};

use crate::models::{EdgeWorkerState, SysInfo};

use super::{EdgeJobFetched, WorkerRegistrationReturn, WorkerSetStateReturn};

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

pub trait EdgeApiClient {
    type Error: error::Error;

    fn worker_register(
        &self,
        hostname: &str,
        state: EdgeWorkerState,
        queues: Option<&Vec<String>>,
        sysinfo: &SysInfo,
    ) -> impl future::Future<Output = Result<WorkerRegistrationReturn, Self::Error>> + Send;

    fn worker_set_state(
        &self,
        hostname: &str,
        state: EdgeWorkerState,
        jobs_active: usize,
        queues: Option<&Vec<String>>,
        sysinfo: &SysInfo,
        maintenance_comments: Option<&str>,
    ) -> impl future::Future<Output = Result<WorkerSetStateReturn, Self::Error>> + Send;

    fn jobs_fetch(
        &self,
        hostname: &str,
        queues: Option<&Vec<String>>,
        free_concurrency: usize,
    ) -> impl future::Future<Output = Result<Option<EdgeJobFetched>, Self::Error>> + Send;

    fn jobs_set_state(
        &self,
        key: &TaskInstanceKey,
        state: TaskInstanceState,
    ) -> impl future::Future<Output = Result<(), Self::Error>> + Send;

    fn logs_push(
        &self,
        key: &TaskInstanceKey,
        log_chunk_time: &DateTime,
        log_chunk_data: &str,
    ) -> impl future::Future<Output = Result<(), Self::Error>> + Send;
}
