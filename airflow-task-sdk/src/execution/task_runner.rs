extern crate alloc;
use crate::{
    api::datamodels::{TIRunContext, TISuccessStatePayload},
    bases::xcom::{BaseXcom, XCOM_RETURN_KEY, XCom, XComError},
    definitions::{Context, DagBag, TaskError},
    execution::{
        ExecutionResultTIState, RuntimeTaskInstance, SupervisorCommsError, TaskRuntime,
        comms::SupervisorClient,
    },
};
use airflow_common::{
    datetime::{TimeProvider, UtcDateTime},
    executors::TaskInstance,
    serialization::serde::{JsonSerialize, JsonValue},
    utils::TaskInstanceState,
};
use alloc::{string::String, vec};
use log::{debug, error, info};

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
    XCom(#[from] XComError<BaseXcom>),
    #[error("Did not push XCom for task mapping")]
    XComForMappingNotPushed,
}

pub struct TaskRunner<R: TaskRuntime> {
    client: SupervisorClient<R>,
    time_provider: R::TimeProvider,
}

impl<R: TaskRuntime> TaskRunner<R> {
    pub fn new(comms: R::Comms, time_provider: R::TimeProvider) -> Self {
        Self {
            client: SupervisorClient::new(comms),
            time_provider,
        }
    }

    async fn run(
        &self,
        ti: &RuntimeTaskInstance<'_, R>,
        context: &Context<'_, R>,
    ) -> Result<(ExecutionResultTIState, Option<TaskError>), ExecutionError> {
        // clear the xcom data sent from server
        for key in ti.ti_context_from_server.xcom_keys_to_clear.iter() {
            debug!("Clearing XCom key: {}", key);
            XCom::<BaseXcom>::delete(
                &self.client,
                ti.dag_id(),
                ti.run_id(),
                ti.task_id(),
                ti.map_index(),
                key,
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
        // TODO can we mutate ti somehow while context exists?
        // ti.state = state.into();
        Ok((state, error))
    }

    async fn finalize(
        &self,
        ti: &RuntimeTaskInstance<'_, R>,
        state: ExecutionResultTIState,
        context: &Context<'_, R>,
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
        &self,
        ti: &RuntimeTaskInstance<'_, R>,
        _context: &Context<'_, R>,
    ) -> Result<(), ExecutionError> {
        let end_date = self.time_provider.now();
        // TODO set TI end_date
        let msg = TISuccessStatePayload {
            state: TaskInstanceState::Success,
            end_date,
            task_outlets: vec![],
            outlet_events: vec![],
            rendered_map_index: ti.rendered_map_index.clone(),
        };
        self.client.succeed_task(msg).await?;
        Ok(())
    }

    async fn push_xcom_if_needed(
        &self,
        result: JsonValue,
        ti: &RuntimeTaskInstance<'_, R>,
    ) -> Result<(), ExecutionError> {
        let xcom_value = if ti.task.do_xcom_push() {
            result
        } else {
            JsonValue::Null
        };

        let has_mapped_dependants = false; // TODO get mapped dependants from task
        if xcom_value.is_null() {
            if !ti.is_mapped && has_mapped_dependants {
                error!("Task did not push XCom value, but has mapped dependants;");
                return Err(ExecutionError::XComForMappingNotPushed);
            }
            debug!("Not pushing null XCom value");
            return Ok(());
        }

        // let mapped_length = if !ti.is_mapped && has_mapped_dependants {
        //     Some(1) // TODO get actual mapped length from task
        // } else {
        //     None
        // };

        info!("Pushing xcom {}", ti);

        // TODO handle multiple outputs

        xcom_push(ti, XCOM_RETURN_KEY, &xcom_value, None).await?;
        Ok(())
    }

    async fn startup<'t>(
        &'t self,
        details: StartupDetails,
        dag_bag: &'t DagBag<R>,
    ) -> Result<RuntimeTaskInstance<'t, R>, ExecutionError> {
        let ti = RuntimeTaskInstance::new(details, dag_bag, &self.client)?;
        Ok(ti)
    }

    async fn _main(self, what: StartupDetails, dag_bag: &DagBag<R>) -> Result<(), ExecutionError> {
        let ti = match self.startup(what, dag_bag).await {
            Ok(ti) => ti,
            Err(e) => {
                error!("{}", e);
                return Err(e);
            }
        };
        let context = ti.get_template_context();
        let (state, error) = match self.run(&ti, &context).await {
            Ok(result) => result,
            Err(e) => {
                error!("{}", e);
                return Err(e);
            }
        };
        self.finalize(&ti, state, &context, error).await;
        // TODO communicate task result properly
        // errors during task execution should be logged here and we only provide a general indication of failure
        Ok(())
    }

    #[cfg(not(feature = "tracing"))]
    /// Perform the actual task execution with the given startup details
    pub async fn main(
        self,
        what: StartupDetails,
        dag_bag: &DagBag<R>,
    ) -> Result<(), ExecutionError> {
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
        dag_bag: &DagBag<R>,
    ) -> Result<(), ExecutionError> {
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

async fn xcom_push<T: JsonSerialize + Sync, R: TaskRuntime>(
    ti: &RuntimeTaskInstance<'_, R>,
    key: &str,
    value: &T,
    mapped_length: Option<usize>,
) -> Result<(), XComError<BaseXcom>> {
    XCom::<BaseXcom>::set(
        ti.client,
        ti.dag_id(),
        ti.run_id(),
        ti.task_id(),
        ti.map_index(),
        key,
        value,
        mapped_length,
    )
    .await?;
    Ok(())
}
