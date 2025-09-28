use crate::{
    definitions::{Context, Task, TaskError},
    execution::TaskRuntime,
};
use airflow_common::serialization::serde::JsonSerialize;

#[trait_variant::make(Send + Sync)]
pub trait Operator<R: TaskRuntime>: Clone + 'static {
    type Output: JsonSerialize;

    async fn execute<'t>(&'t mut self, ctx: &'t Context<'t, R>) -> Result<Self::Output, TaskError>;

    /// Set options for the operator.
    fn set_opts(&self, opts: &mut OperatorOpts) {
        let _ = opts;
    }

    /// Create a task from this operator with the given task ID.
    /// The task ID must be unique within the DAG.
    /// This is a convenience method to avoid having to import and construct a `Task`.
    ///
    /// # Example
    /// ```
    /// use airflow_task_sdk::prelude::*;
    ///
    /// #[derive(Clone, Default)]
    /// struct MyOperator;
    ///
    /// impl<R: TaskRuntime> Operator<R> for MyOperator {
    ///     type Output = ();
    ///
    ///     async fn execute<'t>(&'t mut self, ctx: &'t Context<'t, R>) -> Result<Self::Output, TaskError> {
    ///         Ok(())
    ///     }
    /// }
    ///
    /// pub fn get_dag_bag<R: TaskRuntime>() -> DagBag<R> {
    ///     let my_operator = MyOperator::default();
    ///     let my_task = my_operator.into_task("my_task_id");
    ///     let mut dag = Dag::new("my_dag_id");
    ///     dag.add_task(my_task);
    ///     let mut dag_bag = DagBag::default();
    ///     dag_bag.add_dag(dag);
    ///     dag_bag
    /// }
    /// ```
    fn into_task(self, task_id: &str) -> Task<R> {
        Task::new(task_id, self)
    }
}

/// [OperatorOpts] contains options for operators which may be set by the operator implementation itself as they
/// affect aspects of the task execution in accordance with the operator's implementation.
#[derive(Debug, Default, Clone)]
pub struct OperatorOpts {
    /// Whether the operator produces multiple XCom outputs or not.
    pub multiple_outputs: bool,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_operator_opts_defaults() {
        let opts = OperatorOpts::default();
        // multiple outputs is disabled
        assert!(!opts.multiple_outputs);
    }
}
