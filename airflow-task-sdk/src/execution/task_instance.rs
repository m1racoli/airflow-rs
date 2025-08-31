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

use crate::bases::xcom::XComRequest;
use crate::execution::TaskRuntime;
use crate::{
    api::datamodels::TIRunContext,
    definitions::{Context, DagBag, Task},
    execution::{ExecutionError, StartupDetails, SupervisorClient},
};

pub struct RuntimeTaskInstance<'t, R: TaskRuntime> {
    id: UniqueTaskInstanceId,
    task_id: String,
    dag_id: String,
    run_id: String,
    try_number: usize,
    map_index: MapIndex,

    // TODO visibility to be determined
    _max_tries: usize,
    _start_date: UtcDateTime,
    _state: TaskInstanceState,
    _is_mapped: bool,

    pub(crate) rendered_map_index: Option<String>,
    pub(crate) task: &'t Task<R>,
    pub(crate) ti_context_from_server: TIRunContext,
    pub(crate) client: &'t SupervisorClient<R>,
}

impl<'t, R: TaskRuntime> RuntimeTaskInstance<'t, R> {
    pub(crate) fn new(
        details: StartupDetails,
        dag_bag: &'t DagBag<R>,
        client: &'t SupervisorClient<R>,
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
            id: details.ti.id(),
            task_id: details.ti.task_id().to_string(),
            dag_id: details.ti.dag_id().to_string(),
            run_id: details.ti.run_id().to_string(),
            try_number: details.ti.try_number(),
            map_index: details.ti.map_index(),

            _max_tries: max_tries,
            _start_date: details.start_date,
            _state: TaskInstanceState::Running,
            _is_mapped: false,

            rendered_map_index: None,
            task,
            ti_context_from_server,
            client,
        })
    }

    pub(crate) fn get_template_context(&'t self) -> Context<'t, R> {
        Context::new(self)
    }

    pub fn dag_id(&self) -> &str {
        &self.dag_id
    }

    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub fn try_number(&self) -> usize {
        self.try_number
    }

    pub fn map_index(&self) -> MapIndex {
        self.map_index
    }

    pub fn id(&self) -> &UniqueTaskInstanceId {
        &self.id
    }

    pub fn xcom(&'t self) -> XComRequest<'t, R> {
        XComRequest::new(self)
    }
}
