cfg_if::cfg_if! {
    if #[cfg(feature = "std")] {
    } else {
        extern crate alloc;
        use alloc::string::String;
    }
}

use airflow_common::{
    datetime::{TimeProvider, UtcDateTime},
    executors::TaskInstance,
};

use crate::{
    api::{ExecutionApiError, LocalExecutionApiClient, datamodels::TIRunContext},
    definitions::{Context, DagBag, Task, TaskError},
    execution::{ExecutionResultTIState, RuntimeTaskInstance},
};

#[derive(Debug)]
pub struct StartupDetails {
    pub ti: TaskInstance,
    pub start_date: UtcDateTime,
    pub ti_context: TIRunContext,
}

/// An error which can occur during task execution outside the task itself.
#[derive(thiserror::Error, Debug)]
pub enum ExecutionError<C: LocalExecutionApiClient> {
    #[error("DAG not found: {0}")]
    DagNotFound(String),
    #[error("Task not found in DAG: {0}.{1}")]
    TaskNotFound(String, String),
    #[error("Failed to call execution API: {0}")]
    ExecutionApi(#[from] ExecutionApiError<C::Error>),
}

#[derive(Debug)]
pub struct TaskRunner<C: LocalExecutionApiClient, T: TimeProvider> {
    client: C,
    time_provider: T,
}

impl<C: LocalExecutionApiClient, T: TimeProvider> TaskRunner<C, T> {
    pub fn new(client: C, time_provider: T) -> Self {
        Self {
            client,
            time_provider,
        }
    }

    async fn run(
        &mut self,
        task: &Task,
        ti: &mut RuntimeTaskInstance,
        context: &Context,
    ) -> Result<(ExecutionResultTIState, Option<TaskError>), ExecutionError<C>> {
        // TODO call on_execute_callback
        let (state, error) = match task.execute(context).await {
            Ok(_) => {
                self._handle_current_task_success(ti, context).await?;
                (ExecutionResultTIState::Success, None)
            }
            Err(error) => (ExecutionResultTIState::Failed, Some(error)),
        };
        ti.state = state.into();
        Ok((state, error))
    }

    async fn finalize(
        &self,
        ti: &RuntimeTaskInstance,
        state: ExecutionResultTIState,
        context: &Context,
        error: Option<TaskError>,
    ) {
        let _it = ti;
        let _context = context;
        let _state = state;
        let _error = error;
        // currently no-op
        // could do:
        // - push xcom for operator extra links
        // - set rendered template fields
        // - run callbacks
    }

    async fn _handle_current_task_success(
        &mut self,
        ti: &RuntimeTaskInstance,
        _context: &Context,
    ) -> Result<(), ExecutionError<C>> {
        let when = self.time_provider.now();
        self.client
            .task_instances_succeed(&ti.id, &when, &[], &[], None)
            .await?;
        Ok(())
    }

    async fn _main(
        mut self,
        what: StartupDetails,
        dag_bag: &DagBag,
    ) -> Result<ExecutionResultTIState, ExecutionError<C>> {
        let mut ti = RuntimeTaskInstance::from(what);
        let context = ti.get_template_context();

        let dag_id = ti.dag_id.clone();
        let task_id = ti.task_id.clone();

        // TODO log errors if not found
        let dag = dag_bag
            .get_dag(&dag_id)
            .ok_or_else(|| ExecutionError::DagNotFound(ti.dag_id.clone()))?;
        let task = dag
            .get_task(&task_id)
            .ok_or_else(|| ExecutionError::TaskNotFound(ti.dag_id.clone(), ti.task_id.clone()))?;

        let (state, error) = self.run(task, &mut ti, &context).await?;
        self.finalize(&ti, state, &context, error).await;
        // TODO communicate task result properly
        Ok(state)
    }

    #[cfg(not(feature = "tracing"))]
    /// Perform the actual task execution with the given startup details
    pub async fn main(
        self,
        what: StartupDetails,
        dag_bag: &DagBag,
    ) -> Result<ExecutionResultTIState, ExecutionError<C>> {
        self._main(what, dag_bag).await
    }

    #[cfg(feature = "tracing")]
    /// Perform the actual task execution with the given startup details
    ///
    /// The task execution is instrumented with a special tracing span
    /// which allows us to filter task logs.
    pub async fn main(
        self,
        what: StartupDetails,
        dag_bag: &DagBag,
    ) -> Result<ExecutionResultTIState, ExecutionError<C>> {
        use airflow_common::models::TaskInstanceLike;
        use tracing::{Instrument, info_span};

        let dag_id = what.ti.dag_id();
        let task_id = what.ti.task_id();
        let run_id = what.ti.run_id();
        let try_number = what.ti.try_number();
        let map_index: i64 = what.ti.map_index().into();

        let span = info_span!(target: "task_context", "run_task", dag_id = dag_id, task_id=task_id, run_id=run_id, try_number = try_number, map_index = map_index);
        self._main(what, dag_bag).instrument(span).await
    }
}
