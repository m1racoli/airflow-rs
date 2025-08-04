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

use airflow_common::{
    datetime::UtcDateTime,
    executors::UniqueTaskInstanceId,
    utils::{MapIndex, TaskInstanceState, TerminalTIStateNonSuccess},
};
use serde::Serialize;

use crate::api::{
    TIRunContext,
    datamodels::{
        AssetProfile, InactiveAssetsResponse, PrevSuccessfulDagRunResponse, TICount,
        TaskRescheduleStartDate, TaskStatesResponse,
    },
};

pub trait ExecutionApiClient {
    type Error: error::Error;

    fn task_instances(&self) -> impl TaskInstanceApiClient<Error = Self::Error> + Send;
}

/// An umbrella error type for the Execution API.
#[derive(thiserror::Error, Debug)]
pub enum ExecutionApiError<E: error::Error> {
    #[error(transparent)]
    TaskInstance(#[from] TaskInstanceApiError<E>),
}

/// An error which can occur when interacting with the TaskInstance API.
#[derive(thiserror::Error, Debug)]
pub enum TaskInstanceApiError<E: error::Error> {
    #[error("Not Found: {0}")]
    NotFound(String), // TODO do we want to retrieve detailed reason and message from error response?
    #[error("Conflict: {0}")]
    Conflict(String),
    #[error(transparent)]
    Other(#[from] E),
}

pub trait TaskInstanceApiClient {
    type Error: error::Error;

    /// Tell the API server that this TI has started running.
    fn start(
        &self,
        id: &UniqueTaskInstanceId,
        hostname: &str,
        unixname: &str,
        pid: u32,
        when: &UtcDateTime,
    ) -> impl future::Future<Output = Result<TIRunContext, TaskInstanceApiError<Self::Error>>> + Send;

    /// Tell the API server that this TI has reached a terminal state.
    fn finish(
        &self,
        id: &UniqueTaskInstanceId,
        state: TerminalTIStateNonSuccess,
        when: &UtcDateTime,
        rendered_map_index: Option<&str>,
    ) -> impl future::Future<Output = Result<(), TaskInstanceApiError<Self::Error>>> + Send;

    /// Tell the API server that this TI has failed and reached a up_for_retry state.
    fn retry(
        &self,
        id: &UniqueTaskInstanceId,
        when: &UtcDateTime,
        rendered_map_index: Option<&str>,
    ) -> impl future::Future<Output = Result<(), TaskInstanceApiError<Self::Error>>> + Send;

    /// Tell the API server that this TI has succeeded.
    fn succeed(
        &self,
        id: &UniqueTaskInstanceId,
        when: &UtcDateTime,
        task_outlets: &[AssetProfile],
        outlet_events: &[()], // TODO outlet events
        rendered_map_index: Option<&str>,
    ) -> impl future::Future<Output = Result<(), TaskInstanceApiError<Self::Error>>> + Send;

    /// Tell the API server that this TI has been deferred.
    #[allow(clippy::too_many_arguments)]
    fn defer<T: Serialize + Sync, N: Serialize + Sync>(
        &self,
        id: &UniqueTaskInstanceId,
        classpath: &str,
        trigger_kwargs: &T,
        trigger_timeout: u64,
        next_method: &str,
        next_kwargs: &N,
        rendered_map_index: Option<&str>,
    ) -> impl future::Future<Output = Result<(), TaskInstanceApiError<Self::Error>>> + Send;

    /// Tell the API server that this TI has been rescheduled.
    fn reschedule(
        &self,
        id: &UniqueTaskInstanceId,
        reschedule_date: &UtcDateTime,
        end_date: &UtcDateTime,
    ) -> impl future::Future<Output = Result<(), TaskInstanceApiError<Self::Error>>> + Send;

    /// Tell the API server that this TI is still running and send a heartbeat.
    /// Also, updates the auth token if the server returns a new one.
    fn heartbeat(
        &self,
        id: &UniqueTaskInstanceId,
        hostname: &str,
        pid: u32,
    ) -> impl future::Future<Output = Result<(), TaskInstanceApiError<Self::Error>>> + Send;

    /// Tell the API server to skip the downstream tasks of this TI.
    fn skip_downstream_tasks(
        &self,
        id: &UniqueTaskInstanceId,
        tasks: &[(String, MapIndex)], // TODO enum for mapped and non-mapped tasks?
    ) -> impl future::Future<Output = Result<(), TaskInstanceApiError<Self::Error>>> + Send;

    /// Set Rendered Task Instance Fields via the API server.
    fn set_rtif<F: Serialize + Sync>(
        &self,
        id: &UniqueTaskInstanceId,
        fields: &F,
    ) -> impl future::Future<Output = Result<(), TaskInstanceApiError<Self::Error>>> + Send;

    /// Get the previous successful dag run for a given task instance.
    ///
    /// The data from it is used to get values for Task Context.
    fn get_previous_successful_dagrun(
        &self,
        id: &UniqueTaskInstanceId,
    ) -> impl future::Future<
        Output = Result<PrevSuccessfulDagRunResponse, TaskInstanceApiError<Self::Error>>,
    > + Send;

    /// Get the start date of a task reschedule via the API server.
    fn get_reschedule_start_date(
        &self,
        id: &UniqueTaskInstanceId,
        try_number: usize,
    ) -> impl future::Future<
        Output = Result<TaskRescheduleStartDate, TaskInstanceApiError<Self::Error>>,
    > + Send;

    /// Get count of task instances matching the given criteria.
    #[allow(clippy::too_many_arguments)]
    fn get_count(
        &self,
        dag_id: &str,
        map_index: Option<MapIndex>,
        task_ids: Option<&Vec<String>>,
        task_group_id: Option<&str>,
        logical_dates: Option<&Vec<UtcDateTime>>,
        run_ids: Option<&Vec<String>>,
        states: Option<&Vec<TaskInstanceState>>,
    ) -> impl future::Future<Output = Result<TICount, TaskInstanceApiError<Self::Error>>> + Send;

    /// Get task states given criteria.
    fn get_task_states(
        &self,
        dag_id: &str,
        map_index: Option<MapIndex>,
        task_ids: Option<&Vec<String>>,
        task_group_id: Option<&str>,
        logical_dates: Option<&Vec<UtcDateTime>>,
        run_ids: Option<&Vec<String>>,
    ) -> impl future::Future<Output = Result<TaskStatesResponse, TaskInstanceApiError<Self::Error>>> + Send;

    /// Validate whether there're inactive assets in inlets and outlets of a given task instance.
    fn validate_inlets_and_outlets(
        &self,
        id: &UniqueTaskInstanceId,
    ) -> impl future::Future<
        Output = Result<InactiveAssetsResponse, TaskInstanceApiError<Self::Error>>,
    > + Send;
}
