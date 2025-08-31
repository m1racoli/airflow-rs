extern crate alloc;
use crate::{
    api::datamodels::TIRunContext,
    bases::xcom::XComRequest,
    definitions::{Context, DagBag, Task},
    execution::{ExecutionError, StartupDetails, SupervisorClient, TaskRuntime},
};
use airflow_common::{
    datetime::UtcDateTime,
    executors::UniqueTaskInstanceId,
    models::TaskInstanceLike,
    utils::{MapIndex, TaskInstanceState},
};
use alloc::string::{String, ToString};
use core::fmt::Display;
use log::error;

pub struct RuntimeTaskInstance<'t, R: TaskRuntime> {
    id: UniqueTaskInstanceId,
    task_id: String,
    dag_id: String,
    run_id: String,
    try_number: usize,
    map_index: MapIndex,

    pub(crate) max_tries: usize,
    pub(crate) start_date: UtcDateTime,
    pub(crate) state: TaskInstanceState,
    pub(crate) is_mapped: bool,
    pub(crate) rendered_map_index: Option<String>,
    pub(crate) task: &'t Task<R>,
    pub(crate) ti_context_from_server: TIRunContext,
    pub(crate) client: &'t SupervisorClient<R>,
}

impl<'t, R: TaskRuntime> core::fmt::Debug for RuntimeTaskInstance<'t, R> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RuntimeTaskInstance")
            .field("id", &self.id)
            .field("task_id", &self.task_id)
            .field("dag_id", &self.dag_id)
            .field("run_id", &self.run_id)
            .field("try_number", &self.try_number)
            .field("map_index", &self.map_index)
            .field("max_tries", &self.max_tries)
            .field("start_date", &self.start_date)
            .field("state", &self.state)
            .field("is_mapped", &self.is_mapped)
            .field("rendered_map_index", &self.rendered_map_index)
            .finish()
    }
}

impl<'t, R: TaskRuntime> Display for RuntimeTaskInstance<'t, R> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "id={} task_id={} dag_id={} run_id={} try_number={} map_index={}",
            self.id, self.task_id, self.dag_id, self.run_id, self.try_number, self.map_index
        )
    }
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

            max_tries,
            start_date: details.start_date,
            state: TaskInstanceState::Running,
            is_mapped: false,
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
