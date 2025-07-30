use crate::{
    definitions::{Context, Dag, DagBag, Task, TaskError},
    execution::{ExecutionResultTIState, RuntimeTaskInstance, StartupDetails},
};

/// An error which can occur during task execution outside the task itself.
#[derive(thiserror::Error, Debug)]
pub enum ExecutionError {
    #[error("DAG not found: {0}")]
    DagNotFound(String),
    #[error("Task not found in DAG: {0}.{1}")]
    TaskNotFound(String, String),
}

async fn run(
    mut task: impl Task,
    ti: &mut RuntimeTaskInstance,
    context: &Context,
) -> (ExecutionResultTIState, Option<TaskError>) {
    // TODO call on_execute_callback
    let (state, error) = match task.execute(context).await {
        Ok(_result) => (ExecutionResultTIState::Success, None),
        Err(error) => (ExecutionResultTIState::Failed, Some(error)),
    };
    ti.state = state.into();
    (state, error)
}

async fn finalize(
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
    what: StartupDetails,
    dag_bag: D,
) -> Result<ExecutionResultTIState, ExecutionError> {
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

    let (state, error) = run(task, &mut ti, &context).await;
    finalize(&ti, state, &context, error).await;
    // TODO communicate task result properly
    Ok(state)
}
