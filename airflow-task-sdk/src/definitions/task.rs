extern crate alloc;
use alloc::boxed::Box;
use alloc::string::String;
use alloc::string::ToString;
use core::fmt;
use core::fmt::Pointer;
use core::future::Future;
use core::pin::Pin;

use crate::definitions::{Context, Operator, xcom::XCom};

/// An error type which represents different errors that can occur during task execution.
#[derive(thiserror::Error, Debug)]
pub enum TaskError {
    // TODO all task specific errors (deferrable, skipped, etc.)
    #[error("Unknown error")]
    Unknown,
}

#[derive(Debug)]
pub struct Task {
    task_id: String,
    operator: Box<dyn DynOperator>,
    do_xcom_push: bool,
}

impl Task {
    pub fn new(task_id: &str, operator: impl Operator + 'static) -> Self {
        Self {
            task_id: task_id.to_string(),
            operator: Box::new(operator),
            do_xcom_push: true,
        }
    }

    pub fn with_xcom_push(mut self, do_xcom_push: bool) -> Self {
        self.do_xcom_push = do_xcom_push;
        self
    }

    pub fn do_xcom_push(&self) -> bool {
        self.do_xcom_push
    }

    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    pub async fn execute<'t>(&'t self, ctx: &'t Context) -> BoxedTaskResult {
        let mut operator = self.operator.clone_box();
        operator.execute_box(ctx).await
    }
}

type BoxedTaskResult = Result<Box<(dyn XCom)>, TaskError>;

impl fmt::Debug for Box<dyn DynOperator> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_ref().fmt(f)
    }
}

trait DynOperator: Send + Sync + 'static {
    fn execute_box<'t>(
        &'t mut self,
        ctx: &'t Context,
    ) -> Pin<Box<dyn Future<Output = BoxedTaskResult> + Send + Sync + 't>>;

    fn clone_box(&self) -> Box<dyn DynOperator + 'static>;
}

impl<T> DynOperator for T
where
    T: Operator + 'static,
{
    fn execute_box<'t>(
        &'t mut self,
        ctx: &'t Context,
    ) -> Pin<Box<dyn Future<Output = BoxedTaskResult> + Send + Sync + 't>> {
        Box::pin(async {
            match self.execute(ctx).await {
                Ok(xcom) => Ok(xcom.into()),
                Err(e) => Err(e),
            }
        })
    }

    fn clone_box(&self) -> Box<dyn DynOperator + 'static> {
        Box::new(self.clone())
    }
}
