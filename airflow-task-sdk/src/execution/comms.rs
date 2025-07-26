use crate::api::TIRunContext;
use airflow_common::{datetime::DateTime, executors::TaskInstance};

#[derive(Debug)]
pub struct StartupDetails<'a> {
    pub ti: &'a TaskInstance,
    pub start_date: DateTime,
    pub ti_context: &'a TIRunContext,
}
