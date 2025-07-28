use crate::api::TIRunContext;
use airflow_common::{datetime::DateTime, executors::TaskInstance};

#[derive(Debug)]
pub struct StartupDetails {
    pub ti: TaskInstance,
    pub start_date: DateTime,
    pub ti_context: TIRunContext,
}
