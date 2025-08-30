extern crate alloc;
use alloc::string::String;
use alloc::string::ToString;

use airflow_common::{
    datetime::UtcDateTime,
    executors::UniqueTaskInstanceId,
    models::TaskInstanceLike,
    utils::{MapIndex, TaskInstanceState},
};
use log::error;

use crate::execution::LocalTaskRuntime;
use crate::{
    api::datamodels::TIRunContext,
    definitions::{Context, DagBag, Task},
    execution::{ExecutionError, StartupDetails, SupervisorClient},
};

#[derive(Debug)]
pub struct RuntimeTaskInstance<'t, R: LocalTaskRuntime> {
    pub task: &'t Task,
    pub id: UniqueTaskInstanceId,
    pub task_id: String,
    pub dag_id: String,
    pub run_id: String,
    pub try_number: usize,
    pub map_index: MapIndex,
    pub queued_dttm: Option<UtcDateTime>,
    pub max_tries: usize,
    pub start_date: UtcDateTime,
    pub state: TaskInstanceState,
    pub(super) ti_context_from_server: TIRunContext,
    pub(super) client: &'t SupervisorClient<R::Comms>,
}

impl<'t, R: LocalTaskRuntime> RuntimeTaskInstance<'t, R> {
    pub fn new(
        details: StartupDetails,
        dag_bag: &'t DagBag,
        comms: &'t SupervisorClient<R::Comms>,
    ) -> Result<Self, ExecutionError> {
        let dag_id = details.ti.dag_id();
        let task_id = details.ti.task_id();

        let dag = dag_bag.get_dag(dag_id).ok_or_else(|| {
            error!("DAG not found: {}", dag_id);
            ExecutionError::DagNotFound(dag_id.to_string())
        })?;
        let task = dag.get_task(task_id).ok_or_else(|| {
            error!("Task not found in DAG {}: {}", dag_id, task_id);
            ExecutionError::TaskNotFound(dag_id.to_string(), task_id.to_string())
        })?;

        let max_tries = details.ti_context.max_tries;
        let ti_context_from_server = details.ti_context;

        Ok(RuntimeTaskInstance {
            task,
            id: details.ti.id(),
            task_id: details.ti.task_id().to_string(),
            dag_id: details.ti.dag_id().to_string(),
            run_id: details.ti.run_id().to_string(),
            try_number: details.ti.try_number(),
            map_index: details.ti.map_index(),
            queued_dttm: details.ti.queued_dttm(),
            ti_context_from_server,
            max_tries,
            start_date: details.start_date,
            state: TaskInstanceState::Running,
            client: comms,
        })
    }

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
