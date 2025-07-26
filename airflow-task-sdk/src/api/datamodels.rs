use airflow_common::{datetime::DateTime, utils::DagRunType};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct DagRun {
    pub dag_id: String,
    pub run_id: String,
    pub logical_date: Option<DateTime>,
    pub data_interval_start: Option<DateTime>,
    pub data_interval_end: Option<DateTime>,
    pub run_after: DateTime,
    pub start_date: DateTime,
    pub end_date: Option<DateTime>,
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
