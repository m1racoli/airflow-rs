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

use airflow_common::{
    datetime::UtcDateTime,
    executors::UniqueTaskInstanceId,
    utils::{MapIndex, SecretString, TaskInstanceState, TerminalTIStateNonSuccess},
};
use serde::Serialize;

use crate::api::{
    TIRunContext,
    datamodels::{
        AssetProfile, InactiveAssetsResponse, PrevSuccessfulDagRunResponse, TICount,
        TaskRescheduleStartDate, TaskStatesResponse,
    },
};

#[trait_variant::make(Send)]
pub trait ExecutionApiClient {
    type Error: error::Error;

    /// Tell the API server that this TI has started running.
    async fn task_instances_start(
        &self,
        id: &UniqueTaskInstanceId,
        hostname: &str,
        unixname: &str,
        pid: u32,
        when: &UtcDateTime,
    ) -> Result<TIRunContext, TaskInstanceApiError<Self::Error>>;

    /// Tell the API server that this TI has reached a terminal state.
    async fn task_instances_finish(
        &self,
        id: &UniqueTaskInstanceId,
        state: TerminalTIStateNonSuccess,
        when: &UtcDateTime,
        rendered_map_index: Option<&str>,
    ) -> Result<(), TaskInstanceApiError<Self::Error>>;

    /// Tell the API server that this TI has failed and reached a up_for_retry state.
    async fn task_instances_retry(
        &self,
        id: &UniqueTaskInstanceId,
        when: &UtcDateTime,
        rendered_map_index: Option<&str>,
    ) -> Result<(), TaskInstanceApiError<Self::Error>>;

    /// Tell the API server that this TI has succeeded.
    async fn task_instances_succeed(
        &self,
        id: &UniqueTaskInstanceId,
        when: &UtcDateTime,
        task_outlets: &[AssetProfile],
        outlet_events: &[()], // TODO outlet events
        rendered_map_index: Option<&str>,
    ) -> Result<(), TaskInstanceApiError<Self::Error>>;

    /// Tell the API server that this TI has been deferred.
    #[allow(clippy::too_many_arguments)]
    async fn task_instances_defer<T: Serialize + Sync, N: Serialize + Sync>(
        &self,
        id: &UniqueTaskInstanceId,
        classpath: &str,
        trigger_kwargs: &T,
        trigger_timeout: u64,
        next_method: &str,
        next_kwargs: &N,
        rendered_map_index: Option<&str>,
    ) -> Result<(), TaskInstanceApiError<Self::Error>>;

    /// Tell the API server that this TI has been rescheduled.
    async fn task_instances_reschedule(
        &self,
        id: &UniqueTaskInstanceId,
        reschedule_date: &UtcDateTime,
        end_date: &UtcDateTime,
    ) -> Result<(), TaskInstanceApiError<Self::Error>>;

    /// Tell the API server that this TI is still running and send a heartbeat.
    /// Also, updates the auth token if the server returns a new one.
    async fn task_instances_heartbeat(
        &self,
        id: &UniqueTaskInstanceId,
        hostname: &str,
        pid: u32,
    ) -> Result<(), TaskInstanceApiError<Self::Error>>;

    /// Tell the API server to skip the downstream tasks of this TI.
    async fn task_instances_skip_downstream_tasks(
        &self,
        id: &UniqueTaskInstanceId,
        tasks: &[(String, MapIndex)], // TODO enum for mapped and non-mapped tasks?
    ) -> Result<(), TaskInstanceApiError<Self::Error>>;

    /// Set Rendered Task Instance Fields via the API server.
    async fn task_instances_set_rtif<F: Serialize + Sync>(
        &self,
        id: &UniqueTaskInstanceId,
        fields: &F,
    ) -> Result<(), TaskInstanceApiError<Self::Error>>;

    /// Get the previous successful dag run for a given task instance.
    ///
    /// The data from it is used to get values for Task Context.
    async fn task_instances_get_previous_successful_dagrun(
        &self,
        id: &UniqueTaskInstanceId,
    ) -> Result<PrevSuccessfulDagRunResponse, TaskInstanceApiError<Self::Error>>;

    /// Get the start date of a task reschedule via the API server.
    async fn task_instances_get_reschedule_start_date(
        &self,
        id: &UniqueTaskInstanceId,
        try_number: usize,
    ) -> Result<TaskRescheduleStartDate, TaskInstanceApiError<Self::Error>>;

    /// Get count of task instances matching the given criteria.
    #[allow(clippy::too_many_arguments)]
    async fn task_instances_get_count(
        &self,
        dag_id: &str,
        map_index: Option<MapIndex>,
        task_ids: Option<&Vec<String>>,
        task_group_id: Option<&str>,
        logical_dates: Option<&Vec<UtcDateTime>>,
        run_ids: Option<&Vec<String>>,
        states: Option<&Vec<TaskInstanceState>>,
    ) -> Result<TICount, TaskInstanceApiError<Self::Error>>;

    /// Get task states given criteria.
    async fn task_instances_get_task_states(
        &self,
        dag_id: &str,
        map_index: Option<MapIndex>,
        task_ids: Option<&Vec<String>>,
        task_group_id: Option<&str>,
        logical_dates: Option<&Vec<UtcDateTime>>,
        run_ids: Option<&Vec<String>>,
    ) -> Result<TaskStatesResponse, TaskInstanceApiError<Self::Error>>;

    /// Validate whether there're inactive assets in inlets and outlets of a given task instance.
    async fn task_instances_validate_inlets_and_outlets(
        &self,
        id: &UniqueTaskInstanceId,
    ) -> Result<InactiveAssetsResponse, TaskInstanceApiError<Self::Error>>;
}

/// A factory which builds an execution API client for the given base URL and token.
pub trait ExecutionApiClientFactory {
    type Client: ExecutionApiClient;
    type Error: error::Error;

    fn create(&self, base_url: &str, token: &SecretString) -> Result<Self::Client, Self::Error>;
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

#[trait_variant::make(Send)]
pub trait TaskInstanceApiClient {
    type Error: error::Error;
}
