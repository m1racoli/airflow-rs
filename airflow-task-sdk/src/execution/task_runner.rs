extern crate alloc;
use alloc::string::String;
use alloc::vec;

use airflow_common::{
    datetime::{TimeProvider, UtcDateTime},
    executors::TaskInstance,
    utils::TaskInstanceState,
};
use log::debug;

use crate::{
    api::datamodels::{JsonValue, TIRunContext, TISuccessStatePayload},
    definitions::{Context, DagBag, JsonSerdeError, TaskError, XComValue},
    execution::{
        ExecutionResultTIState, LocalSupervisorComms, RuntimeTaskInstance, SupervisorCommsError,
        comms::SupervisorClient,
    },
};

#[derive(Debug)]
pub struct StartupDetails {
    pub ti: TaskInstance,
    pub start_date: UtcDateTime,
    pub ti_context: TIRunContext,
}

/// An error which can occur during task execution outside the task itself.
#[derive(thiserror::Error, Debug)]
pub enum ExecutionError {
    #[error("DAG not found: {0}")]
    DagNotFound(String),
    #[error("Task not found in DAG: {0}.{1}")]
    TaskNotFound(String, String),
    #[error("Failed to communicate with supervisor: {0}")]
    SupervisorComms(#[from] SupervisorCommsError),
    #[error("Task runner has been cancelled")]
    Cancelled,
    #[error("Task runner has panicked")]
    Panicked,
    #[error("XCom error occurred: {0}")]
    XCom(#[from] JsonSerdeError),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
}

#[derive(Debug)]
pub struct TaskRunner<C: LocalSupervisorComms, T: TimeProvider> {
    client: SupervisorClient<C>,
    time_provider: T,
}

impl<C: LocalSupervisorComms, T: TimeProvider> TaskRunner<C, T> {
    pub fn new(comms: C, time_provider: T) -> Self {
        Self {
            client: comms.into(),
            time_provider,
        }
    }

    async fn run(
        &mut self,
        ti: &mut RuntimeTaskInstance<'_>,
        context: &Context,
    ) -> Result<(ExecutionResultTIState, Option<TaskError>), ExecutionError> {
        // clear the xcom data sent from server
        for x in ti.ti_context_from_server.xcom_keys_to_clear.iter() {
            debug!("Clearing XCom key: {}", x);
            // TODO call XCom backend which also handles purging
            self.client
                .delete_xcom(
                    x.to_string(),
                    ti.dag_id.to_string(),
                    ti.run_id.to_string(),
                    ti.task_id.to_string(),
                    Some(ti.map_index),
                )
                .await?;
        }

        // TODO call on_execute_callback
        let task = ti.task;
        let (state, error) = match task.execute(context).await {
            Ok(result) => {
                self.push_xcom_if_needed(result, ti).await?;
                self.handle_current_task_success(ti, context).await?;
                (ExecutionResultTIState::Success, None)
            }
            Err(error) => (ExecutionResultTIState::Failed, Some(error)),
        };
        ti.state = state.into();
        Ok((state, error))
    }

    async fn finalize(
        &self,
        ti: &RuntimeTaskInstance<'_>,
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

    async fn handle_current_task_success(
        &mut self,
        _ti: &RuntimeTaskInstance<'_>,
        _context: &Context,
    ) -> Result<(), ExecutionError> {
        let end_date = self.time_provider.now();
        let msg = TISuccessStatePayload {
            state: TaskInstanceState::Success,
            end_date,
            task_outlets: vec![],
            outlet_events: vec![],
            rendered_map_index: None,
        };
        self.client.succeed_task(msg).await?;
        // self.client
        // .task_instances_succeed(&ti.id, &when, &[], &[], None)
        // .await?;
        Ok(())
    }

    async fn push_xcom_if_needed(
        &self,
        result: Box<dyn XComValue>,
        ti: &RuntimeTaskInstance<'_>,
    ) -> Result<(), ExecutionError> {
        if !ti.task.do_xcom_push() {
            return Ok(());
        }

        // TODO handle if no xcom to push, but has mapped dependants
        // TODO handle if task is not mapped, but has mapped dependants
        // TODO handle multiple outputs

        xcom_push(&self.client, ti, "return_value", result, None).await?;
        Ok(())
    }

    async fn startup<'d>(
        &self,
        details: StartupDetails,
        dag_bag: &'d DagBag,
    ) -> Result<(RuntimeTaskInstance<'d>, Context), ExecutionError> {
        let ti = RuntimeTaskInstance::try_from((details, dag_bag))?;
        let context = ti.get_template_context();
        Ok((ti, context))
    }

    async fn _main(mut self, what: StartupDetails, dag_bag: &DagBag) -> Result<(), ExecutionError> {
        let (mut ti, context) = self.startup(what, dag_bag).await?;

        let (state, error) = self.run(&mut ti, &context).await?;
        self.finalize(&ti, state, &context, error).await;
        // TODO communicate task result properly
        Ok(())
    }

    #[cfg(not(feature = "tracing"))]
    /// Perform the actual task execution with the given startup details
    pub async fn main(self, what: StartupDetails, dag_bag: &DagBag) -> Result<(), ExecutionError> {
        self._main(what, dag_bag).await
    }

    #[cfg(feature = "tracing")]
    /// Perform the actual task execution with the given startup details
    ///
    /// The task execution is instrumented with a special tracing span
    /// which allows us to filter task logs.
    pub async fn main(self, what: StartupDetails, dag_bag: &DagBag) -> Result<(), ExecutionError> {
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

async fn xcom_push<C: LocalSupervisorComms>(
    client: &SupervisorClient<C>,
    ti: &RuntimeTaskInstance<'_>,
    key: &str,
    value: Box<dyn XComValue>,
    mapped_length: Option<usize>,
) -> Result<(), ExecutionError> {
    // TODO XCom class to handle serialization and custom backends
    let serialized = value.as_ref().serialize()?;
    let value: JsonValue = serde_json::from_str(&serialized)?;
    client
        .set_xcom(
            key.to_string(),
            value,
            ti.dag_id.to_string(),
            ti.run_id.to_string(),
            ti.task_id.to_string(),
            Some(ti.map_index),
            mapped_length,
        )
        .await?;
    Ok(())
}
