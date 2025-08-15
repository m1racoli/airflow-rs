cfg_if::cfg_if! {
    if #[cfg(feature = "std")] {
        use std::collections::BTreeMap;
    } else {
        extern crate alloc;
        use alloc::string::String;
        use alloc::collections::BTreeMap;
        use alloc::vec::Vec;
    }
}

use airflow_common::{
    datetime::UtcDateTime,
    utils::{DagRunType, TaskInstanceState},
};
use serde::{Deserialize, Serialize};

/// Profile of an asset-like object.
///
/// Asset will have name, uri defined, with type set to 'Asset'.
/// AssetNameRef will have name defined, type set to 'AssetNameRef'.
/// AssetUriRef will have uri defined, type set to 'AssetUriRef'.
/// AssetAlias will have name defined, type set to 'AssetAlias'.
///
/// Note that 'type' here is distinct from 'asset_type' the user declares on an
/// Asset (or subclass). This field is for distinguishing between different
/// asset-related types (Asset, AssetRef, or AssetAlias).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum AssetProfile {
    Asset { name: String, uri: String },
    AssetNameRef { name: String },
    AssetUriRef { uri: String },
    AssetAlias { name: String },
}

#[derive(Debug, Clone, Deserialize)]
pub struct DagRun {
    pub dag_id: String,
    pub run_id: String,
    pub logical_date: Option<UtcDateTime>,
    pub data_interval_start: Option<UtcDateTime>,
    pub data_interval_end: Option<UtcDateTime>,
    pub run_after: UtcDateTime,
    pub start_date: UtcDateTime,
    pub end_date: Option<UtcDateTime>,
    pub clear_number: usize,
    pub run_type: DagRunType,
    // conf
}

/// Response for inactive assets.
#[derive(Debug, Clone, Deserialize)]
pub struct InactiveAssetsResponse {
    pub inactive_assets: Vec<AssetProfile>,
}

/// Schema for response with previous successful DagRun information for Task Template Context.
#[derive(Debug, Clone, Deserialize)]
pub struct PrevSuccessfulDagRunResponse {
    pub data_interval_start: Option<UtcDateTime>,
    pub data_interval_end: Option<UtcDateTime>,
    pub start_date: Option<UtcDateTime>,
    pub end_date: Option<UtcDateTime>,
}

/// Response containing the first reschedule date for a task instance.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
pub struct TaskRescheduleStartDate {
    pub start_date: Option<UtcDateTime>,
}

/// Response for task states with run_id, task and state.
///
/// The first key is used for the run_id, the second key is used for the task_id.
/// For results of mapped tasks, the task_id is in the form of `<task_id>_<map_index>`.
#[derive(Debug, Clone, Deserialize)]
pub struct TaskStatesResponse {
    pub states: BTreeMap<String, BTreeMap<String, TaskInstanceState>>,
}

/// Response containing count of Task Instances matching certain filters.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
pub struct TICount {
    pub count: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TIRunContext {
    pub dag_run: DagRun,
    pub task_reschedule_count: usize,
    pub max_tries: usize,
    // variables: (),
    // connections: (),
    // pub upstream_map_indexes: Option<BTreeMap<String, usize>>,
    // pub next_method: Option<String>,
    // next_kwargs: (),
    // pub xcom_keys_to_clear: Vec<String>,
    pub should_retry: bool,
}
