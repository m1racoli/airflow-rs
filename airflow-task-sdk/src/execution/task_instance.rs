use airflow_common::{
    datetime::UtcDateTime,
    executors::UniqueTaskInstanceId,
    models::TaskInstanceLike,
    utils::{MapIndex, TaskInstanceState},
};

use crate::{api::TIRunContext, definitions::Context, execution::StartupDetails};

#[derive(Debug)]
pub struct RuntimeTaskInstance {
    pub id: UniqueTaskInstanceId,
    pub task_id: String,
    pub dag_id: String,
    pub run_id: String,
    pub try_number: usize,
    pub map_index: MapIndex,
    pub queued_dttm: Option<UtcDateTime>,
    pub ti_context: TIRunContext,
    pub max_tries: usize,
    pub start_date: UtcDateTime,
    pub state: TaskInstanceState,
}

impl RuntimeTaskInstance {
    pub fn get_template_context(&self) -> Context {
        Context {
            dag_id: self.dag_id.clone(),
            task_id: self.task_id.clone(),
            run_id: self.run_id.clone(),
            try_number: self.try_number,
            map_index: self.map_index,
        }
    }
}

impl From<StartupDetails> for RuntimeTaskInstance {
    fn from(details: StartupDetails) -> Self {
        RuntimeTaskInstance {
            id: details.ti.id(),
            task_id: details.ti.task_id().to_string(),
            dag_id: details.ti.dag_id().to_string(),
            run_id: details.ti.run_id().to_string(),
            try_number: details.ti.try_number(),
            map_index: details.ti.map_index(),
            queued_dttm: details.ti.queued_dttm(),
            ti_context: details.ti_context.clone(),
            max_tries: details.ti_context.max_tries,
            start_date: details.start_date,
            state: TaskInstanceState::Running,
        }
    }
}
