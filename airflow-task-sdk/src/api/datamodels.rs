cfg_if::cfg_if! {
    if #[cfg(feature = "std")] {
    } else {
        extern crate alloc;
        use alloc::string::String;
    }
}

use airflow_common::{datetime::UtcDateTime, utils::DagRunType};
use serde::Deserialize;

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
