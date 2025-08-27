use std::{sync::LazyLock, time::Duration};

use airflow_task_sdk::definitions::{Context, Dag, DagBag, Operator, TaskError};
use tokio::time::sleep;
use tracing::{info, warn};

#[derive(Debug, Clone, Default)]
pub struct ExampleOperator {
    cnt: i32,
}

impl Operator for ExampleOperator {
    type Item = i32;

    async fn execute<'t>(&'t mut self, ctx: &'t Context) -> Result<Self::Item, TaskError> {
        info!(
            "I am task instance {} {} {} {} {}",
            ctx.dag_id, ctx.task_id, ctx.run_id, ctx.try_number, ctx.map_index
        );
        info!("I am running with cnt={}", self.cnt);
        // TODO test with more than 5 minutes of heartbeat timeout
        sleep(Duration::from_secs(7)).await;
        warn!("This feels very fast! 😎");
        self.cnt += 1;
        info!("I am done with cnt={}", self.cnt);
        Ok(self.cnt)
    }
}

static DAG_BAG: LazyLock<DagBag> = LazyLock::new(|| {
    let run = ExampleOperator::default().task("run");
    let mut dag = Dag::new("example_dag");
    dag.add_task(run);
    let mut dag_bag = DagBag::default();
    dag_bag.add_dag(dag);
    dag_bag
});

pub fn get_dag_bag() -> &'static DagBag {
    &DAG_BAG
}
