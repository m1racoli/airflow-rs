use crate::api::datamodels::TIRunContext;
use airflow_common::{datetime::UtcDateTime, executors::TaskInstance};

#[derive(Debug)]
pub struct StartupDetails {
    pub ti: TaskInstance,
    pub start_date: UtcDateTime,
    pub ti_context: TIRunContext,
}
