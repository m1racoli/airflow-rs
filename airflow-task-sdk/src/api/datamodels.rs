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
    utils::{DagRunType, TaskInstanceState, TerminalTIStateNonSuccess},
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TIEnterRunningPayload {
    pub state: TaskInstanceState,
    pub hostname: String,
    pub unixname: String,
    pub pid: u32,
    pub start_date: UtcDateTime,
}

#[derive(Debug, Clone, Serialize)]
pub struct TIEnterRunningPayloadBody<'a> {
    pub state: TaskInstanceState,
    pub hostname: &'a str,
    pub unixname: &'a str,
    pub pid: u32,
    pub start_date: &'a UtcDateTime,
}

impl<'a> From<&'a TIEnterRunningPayload> for TIEnterRunningPayloadBody<'a> {
    fn from(payload: &'a TIEnterRunningPayload) -> Self {
        TIEnterRunningPayloadBody {
            state: payload.state,
            hostname: &payload.hostname,
            unixname: &payload.unixname,
            pid: payload.pid,
            start_date: &payload.start_date,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TITerminalStatePayload {
    pub state: TerminalTIStateNonSuccess,
    pub end_date: UtcDateTime,
    pub rendered_map_index: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TITerminalStatePayloadBody<'a> {
    pub state: TerminalTIStateNonSuccess,
    pub end_date: &'a UtcDateTime,
    pub rendered_map_index: Option<&'a str>,
}

impl<'a> From<&'a TITerminalStatePayload> for TITerminalStatePayloadBody<'a> {
    fn from(payload: &'a TITerminalStatePayload) -> Self {
        TITerminalStatePayloadBody {
            state: payload.state,
            end_date: &payload.end_date,
            rendered_map_index: payload.rendered_map_index.as_deref(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TISuccessStatePayload {
    pub state: TaskInstanceState, // TODO tag success
    pub end_date: UtcDateTime,
    pub task_outlets: Vec<AssetProfile>,
    pub outlet_events: Vec<()>, // TODO outlet events
    pub rendered_map_index: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TISuccessStatePayloadBody<'a> {
    pub state: TaskInstanceState, // TODO tag success
    pub end_date: &'a UtcDateTime,
    pub task_outlets: &'a [AssetProfile],
    pub outlet_events: &'a [()], // TODO outlet events
    pub rendered_map_index: Option<&'a str>,
}

impl<'a> From<&'a TISuccessStatePayload> for TISuccessStatePayloadBody<'a> {
    fn from(payload: &'a TISuccessStatePayload) -> Self {
        TISuccessStatePayloadBody {
            state: payload.state,
            end_date: &payload.end_date,
            task_outlets: &payload.task_outlets,
            outlet_events: &payload.outlet_events,
            rendered_map_index: payload.rendered_map_index.as_deref(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TIHeartbeatInfo {
    pub hostname: String,
    pub pid: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct TIHeartbeatInfoBody<'a> {
    pub hostname: &'a str,
    pub pid: u32,
}

impl<'a> From<&'a TIHeartbeatInfo> for TIHeartbeatInfoBody<'a> {
    fn from(info: &'a TIHeartbeatInfo) -> Self {
        TIHeartbeatInfoBody {
            hostname: &info.hostname,
            pid: info.pid,
        }
    }
}
