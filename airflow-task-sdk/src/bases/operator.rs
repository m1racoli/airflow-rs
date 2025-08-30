use crate::bases::xcom::XComValue;
use crate::definitions::Context;
use crate::definitions::Task;
use crate::definitions::TaskError;
use crate::execution::TaskRuntime;

#[trait_variant::make(Send + Sync)]
pub trait Operator<R: TaskRuntime>: Clone + 'static {
    type Item: XComValue + 'static;
    async fn execute<'t>(&'t mut self, ctx: &'t Context<'t, R>) -> Result<Self::Item, TaskError>;

    /// Create a task from this operator with the given task ID.
    /// The task ID must be unique within the DAG.
    /// This is a convenience method to avoid having to import and construct a `Task`.
    ///
    /// # Example
    /// ```
    /// use airflow_task_sdk::definitions::{Dag, DagBag, Operator};
    ///
    /// #[derive(Clone, Default)]
    /// struct MyOperator;
    ///
    /// impl Operator for MyOperator {
    ///     type Item = ();
    ///
    ///     async fn execute<'t>(&'t mut self, ctx: &'t Context) -> Result<Self::Item, TaskError> {
    ///         Ok(())
    ///     }
    /// }
    ///
    /// let my_operator = MyOperator::default();
    /// let my_task = my_operator.into_task("my_task_id");
    /// let mut dag = Dag::new("my_dag_id");
    /// dag.add_task(my_task);
    /// let mut dag_bag = DagBag::default();
    /// dag_bag.add_dag(dag);
    /// ```
    fn into_task(self, task_id: &str) -> Task<R> {
        Task::new(task_id, self)
    }
}
