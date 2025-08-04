cfg_if::cfg_if! {
    if #[cfg(feature = "std")] {
    } else {
        extern crate alloc;
        use alloc::string::String;
    }
}

use airflow_common::prelude::*;

use crate::{
    api::{ExecutionApiClient, ExecutionApiError, TaskInstanceApiClient},
    definitions::{Context, Dag, DagBag, Task, TaskError},
    execution::{ExecutionResultTIState, RuntimeTaskInstance, StartupDetails},
};

/// An error which can occur during task execution outside the task itself.
#[derive(thiserror::Error, Debug)]
pub enum ExecutionError<C: ExecutionApiClient> {
    #[error("DAG not found: {0}")]
    DagNotFound(String),
    #[error("Task not found in DAG: {0}.{1}")]
    TaskNotFound(String, String),
    #[error("Failed to call execution API: {0}")]
    ExecutionApi(#[from] ExecutionApiError<C::Error>),
}

#[derive(Debug)]
pub struct TaskRunner<C: ExecutionApiClient, T: TimeProvider> {
    client: C,
    time_provider: T,
}

impl<C: ExecutionApiClient, T: TimeProvider> TaskRunner<C, T> {
    pub fn new(client: C, time_provider: T) -> Self {
        Self {
            client,
            time_provider,
        }
    }

    async fn run(
        &self,
        mut task: impl Task,
        ti: &mut RuntimeTaskInstance,
        context: &Context,
    ) -> Result<(ExecutionResultTIState, Option<TaskError>), ExecutionError<C>> {
        // TODO call on_execute_callback
        let (state, error) = match task.execute(context).await {
            Ok(_) => {
                let when = self.time_provider.now();
                self.client
                    .task_instances()
                    .succeed(&ti.id, &when, &[], &[], None)
                    .await
                    .map_err(ExecutionApiError::from)?;
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

    /// perform the actual task execution with the given startup details
    pub async fn main<D: DagBag>(
        self,
        what: StartupDetails,
        dag_bag: D,
    ) -> Result<ExecutionResultTIState, ExecutionError<C>> {
        let mut ti = RuntimeTaskInstance::from(what);
        let context = ti.get_template_context();

        let dag_id = ti.dag_id.clone();
        let task_id = ti.task_id.clone();

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
}
