extern crate alloc;
use alloc::string::String;
use alloc::vec::Vec;
use core::error;

use airflow_common::{
    datetime::UtcDateTime,
    executors::UniqueTaskInstanceId,
    utils::{MapIndex, TaskInstanceState, TerminalTIStateNonSuccess},
};
use serde::Serialize;

use crate::{api::datamodels::*, definitions::serde::JsonValue};

/// An error which can occur when interacting with the TaskInstance API.
#[derive(thiserror::Error, Debug)]
pub enum ExecutionApiError<E: error::Error> {
    #[error("Not Found: {0}")]
    NotFound(String), // TODO do we want to retrieve detailed reason and message from error response?
    #[error("Conflict: {0}")]
    Conflict(String),
    #[error(transparent)]
    Client(#[from] E),
    #[error("{0}")]
    Other(String),
}

#[trait_variant::make(ExecutionApiClient: Send)]
pub trait LocalExecutionApiClient {
    type Error: error::Error;

    /// Tell the API server that this TI has started running.
    async fn task_instances_start(
        &mut self,
        id: &UniqueTaskInstanceId,
        hostname: &str,
        unixname: &str,
        pid: u32,
        when: &UtcDateTime,
    ) -> Result<TIRunContext, ExecutionApiError<Self::Error>>;

    /// Tell the API server that this TI has reached a terminal state.
    async fn task_instances_finish(
        &mut self,
        id: &UniqueTaskInstanceId,
        state: TerminalTIStateNonSuccess,
        when: &UtcDateTime,
        rendered_map_index: Option<&str>,
    ) -> Result<(), ExecutionApiError<Self::Error>>;

    /// Tell the API server that this TI has failed and reached a up_for_retry state.
    async fn task_instances_retry(
        &mut self,
        id: &UniqueTaskInstanceId,
        when: &UtcDateTime,
        rendered_map_index: Option<&str>,
    ) -> Result<(), ExecutionApiError<Self::Error>>;

    /// Tell the API server that this TI has succeeded.
    async fn task_instances_succeed(
        &mut self,
        id: &UniqueTaskInstanceId,
        when: &UtcDateTime,
        task_outlets: &[AssetProfile],
        outlet_events: &[()], // TODO outlet events
        rendered_map_index: Option<&str>,
    ) -> Result<(), ExecutionApiError<Self::Error>>;

    /// Tell the API server that this TI has been deferred.
    #[allow(clippy::too_many_arguments)]
    async fn task_instances_defer<T: Serialize + Sync, N: Serialize + Sync>(
        &mut self,
        id: &UniqueTaskInstanceId,
        classpath: &str,
        trigger_kwargs: &T,
        trigger_timeout: u64,
        next_method: &str,
        next_kwargs: &N,
        rendered_map_index: Option<&str>,
    ) -> Result<(), ExecutionApiError<Self::Error>>;

    /// Tell the API server that this TI has been rescheduled.
    async fn task_instances_reschedule(
        &mut self,
        id: &UniqueTaskInstanceId,
        reschedule_date: &UtcDateTime,
        end_date: &UtcDateTime,
    ) -> Result<(), ExecutionApiError<Self::Error>>;

    /// Tell the API server that this TI is still running and send a heartbeat.
    /// Also, updates the auth token if the server returns a new one.
    async fn task_instances_heartbeat(
        &mut self,
        id: &UniqueTaskInstanceId,
        hostname: &str,
        pid: u32,
    ) -> Result<(), ExecutionApiError<Self::Error>>;

    /// Tell the API server to skip the downstream tasks of this TI.
    async fn task_instances_skip_downstream_tasks(
        &mut self,
        id: &UniqueTaskInstanceId,
        tasks: &[(String, MapIndex)], // TODO enum for mapped and non-mapped tasks?
    ) -> Result<(), ExecutionApiError<Self::Error>>;

    /// Set Rendered Task Instance Fields via the API server.
    async fn task_instances_set_rtif<F: Serialize + Sync>(
        &mut self,
        id: &UniqueTaskInstanceId,
        fields: &F,
    ) -> Result<(), ExecutionApiError<Self::Error>>;

    /// Get the previous successful dag run for a given task instance.
    ///
    /// The data from it is used to get values for Task Context.
    async fn task_instances_get_previous_successful_dagrun(
        &mut self,
        id: &UniqueTaskInstanceId,
    ) -> Result<PrevSuccessfulDagRunResponse, ExecutionApiError<Self::Error>>;

    /// Get the start date of a task reschedule via the API server.
    async fn task_instances_get_reschedule_start_date(
        &mut self,
        id: &UniqueTaskInstanceId,
        try_number: usize,
    ) -> Result<TaskRescheduleStartDate, ExecutionApiError<Self::Error>>;

    /// Get count of task instances matching the given criteria.
    #[allow(clippy::too_many_arguments)]
    async fn task_instances_get_count(
        &mut self,
        dag_id: &str,
        map_index: Option<MapIndex>,
        task_ids: Option<&Vec<String>>,
        task_group_id: Option<&str>,
        logical_dates: Option<&Vec<UtcDateTime>>,
        run_ids: Option<&Vec<String>>,
        states: Option<&Vec<TaskInstanceState>>,
    ) -> Result<TICount, ExecutionApiError<Self::Error>>;

    /// Get task states given criteria.
    async fn task_instances_get_task_states(
        &mut self,
        dag_id: &str,
        map_index: Option<MapIndex>,
        task_ids: Option<&Vec<String>>,
        task_group_id: Option<&str>,
        logical_dates: Option<&Vec<UtcDateTime>>,
        run_ids: Option<&Vec<String>>,
    ) -> Result<TaskStatesResponse, ExecutionApiError<Self::Error>>;

    /// Validate whether there're inactive assets in inlets and outlets of a given task instance.
    async fn task_instances_validate_inlets_and_outlets(
        &mut self,
        id: &UniqueTaskInstanceId,
    ) -> Result<InactiveAssetsResponse, ExecutionApiError<Self::Error>>;

    /// Get the number of mapped XCom values.
    async fn xcoms_head(
        &mut self,
        dag_id: &str,
        run_id: &str,
        task_id: &str,
        key: &str,
    ) -> Result<usize, ExecutionApiError<Self::Error>>;

    /// Get an XCom value from the API server.
    async fn xcoms_get(
        &mut self,
        dag_id: &str,
        run_id: &str,
        task_id: &str,
        key: &str,
        map_index: Option<MapIndex>,
        include_prior_dates: Option<bool>,
    ) -> Result<XComResponse, ExecutionApiError<Self::Error>>;

    /// Set an XCom value via the API server.
    #[allow(clippy::too_many_arguments)]
    async fn xcoms_set(
        &mut self,
        dag_id: &str,
        run_id: &str,
        task_id: &str,
        key: &str,
        value: &JsonValue,
        map_index: Option<MapIndex>,
        mapped_length: Option<usize>,
    ) -> Result<(), ExecutionApiError<Self::Error>>;

    /// Delete an XCom with given key via the API server.
    async fn xcoms_delete(
        &mut self,
        dag_id: &str,
        run_id: &str,
        task_id: &str,
        key: &str,
        map_index: Option<MapIndex>,
    ) -> Result<(), ExecutionApiError<Self::Error>>;

    // TODO
    // async fn xcoms_get_sequence_item(
    //     &mut self,
    //     dag_id: &str,
    //     run_id: &str,
    //     task_id: &str,
    //     key: &str,
    //     offset: usize,
    // ) -> Result<XComSequenceIndexResponse, ExecutionApiError<Self::Error>>;

    // TODO
    // async fn xcoms_get_sequence_slice(
    //     &mut self,
    //     dag_id: &str,
    //     run_id: &str,
    //     task_id: &str,
    //     key: &str,
    //     start: Option<usize>,
    //     stop: Option<usize>,
    //     step: Option<usize>,
    //     include_prior_dates: Option<bool>,
    // ) -> Result<XComSequenceSliceResponse, ExecutionApiError<Self::Error>>;
}
