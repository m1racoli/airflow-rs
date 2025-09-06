extern crate alloc;
use crate::{
    bases::{
        operator::{Operator, OperatorOpts},
        xcom::{BaseXcom, XComError},
    },
    definitions::Context,
    execution::TaskRuntime,
};
use airflow_common::{
    datetime::UtcDateTime,
    serialization::serde::{self, JsonValue, serialize},
};
use alloc::{
    boxed::Box,
    string::{String, ToString},
};
use core::fmt::{self, Pointer};
use core::{future::Future, pin::Pin};

/// An error type which represents different errors that can occur during task execution.
#[derive(thiserror::Error, Debug)]
pub enum TaskError {
    // DownstreamTasksSkipped
    // DagRunTrigger
    // TaskDeferred
    // Skip
    /// The task should be re-scheduled at a later time.
    #[error("Task was rescheduled")]
    Reschedule(UtcDateTime),
    // Fail
    // SensorTimeout
    // TaskTimeout
    /// A generic error available to the author of the operator.
    ///
    /// TODO Maybe we can use anyhow::Error here?
    #[error("Unknown error: {0}")]
    Unknown(String),
    // TODO the following can be combined into an internal AirflowError of some sort
    #[error(transparent)]
    JsonSerde(#[from] serde::JsonSerdeError),
    #[error(transparent)]
    XCom(#[from] XComError<BaseXcom>),
}

#[derive(Debug)]
pub struct Task<R: TaskRuntime> {
    task_id: String,
    operator: Box<dyn DynOperator<R>>,
    do_xcom_push: bool,
    opts: OperatorOpts,
}

// TODO for now the task and operator needs to be clonable, as we operate on a static DagBag instance
// with all DAGs and Tasks already initialized. Once we have a more dynamic system in place, we can
// revisit this requirement.
impl<R: TaskRuntime> Clone for Task<R> {
    fn clone(&self) -> Self {
        Self {
            task_id: self.task_id.clone(),
            operator: self.operator.clone_box(),
            do_xcom_push: self.do_xcom_push,
            opts: self.opts.clone(),
        }
    }
}

impl<R: TaskRuntime> Task<R> {
    pub fn new(task_id: &str, operator: impl Operator<R> + 'static) -> Self {
        let mut opts = OperatorOpts::default();
        operator.set_opts(&mut opts);
        Self {
            task_id: task_id.to_string(),
            operator: Box::new(operator),
            do_xcom_push: true,
            opts,
        }
    }

    pub fn with_xcom_push(mut self, do_xcom_push: bool) -> Self {
        self.do_xcom_push = do_xcom_push;
        self
    }

    pub fn do_xcom_push(&self) -> bool {
        self.do_xcom_push
    }

    pub fn opts(&self) -> &OperatorOpts {
        &self.opts
    }

    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    pub async fn execute<'t>(&'t self, ctx: &'t Context<'t, R>) -> Result<JsonValue, TaskError> {
        let mut operator = self.operator.clone_box();
        operator.execute_box(ctx).await
    }
}

impl<R: TaskRuntime> fmt::Debug for Box<dyn DynOperator<R>> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_ref().fmt(f)
    }
}

trait DynOperator<R: TaskRuntime>: Send + Sync + 'static {
    fn execute_box<'t>(
        &'t mut self,
        ctx: &'t Context<'t, R>,
    ) -> Pin<Box<dyn Future<Output = Result<JsonValue, TaskError>> + Send + Sync + 't>>;

    fn clone_box(&self) -> Box<dyn DynOperator<R> + 'static>;
}

impl<T, R: TaskRuntime> DynOperator<R> for T
where
    T: Operator<R> + 'static,
{
    fn execute_box<'t>(
        &'t mut self,
        ctx: &'t Context<'t, R>,
    ) -> Pin<Box<dyn Future<Output = Result<JsonValue, TaskError>> + Send + Sync + 't>> {
        Box::pin(async {
            match self.execute(ctx).await {
                Ok(xcom) => Ok(serialize(&xcom)?),
                Err(e) => Err(e),
            }
        })
    }

    fn clone_box(&self) -> Box<dyn DynOperator<R> + 'static> {
        Box::new(self.clone())
    }
}
