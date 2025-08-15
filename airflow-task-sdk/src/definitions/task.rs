cfg_if::cfg_if! {
    if #[cfg(feature = "std")] {
        use std::pin::Pin;
    } else {
        extern crate alloc;
        use alloc::boxed::Box;
        use alloc::string::String;
        use alloc::string::ToString;
        use core::future::Future;
        use core::pin::Pin;
    }
}

use crate::definitions::{Context, Operator, xcom::XCom};

/// An error type which represents different errors that can occur during task execution.
#[derive(thiserror::Error, Debug)]
pub enum TaskError {
    // TODO all task specific errors (deferrable, skipped, etc.)
    #[error("Unknown error")]
    Unknown,
}

pub struct Task {
    task_id: String,
    operator: Box<dyn DynOperator>,
}

impl Task {
    pub fn new(task_id: &str, operator: impl Operator + 'static) -> Self {
        Self {
            task_id: task_id.to_string(),
            operator: Box::new(operator),
        }
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
