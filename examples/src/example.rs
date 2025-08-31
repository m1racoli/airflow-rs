use core::f32;
use std::{sync::LazyLock, time::Duration};

use airflow_task_sdk::{
    bases::operator::Operator,
    definitions::{Context, Dag, DagBag, TaskError},
    execution::TaskRuntime,
};
use log::error;
use tokio::time::sleep;
use tracing::{info, warn};

use crate::tokio::TokioTaskRuntime;

#[derive(Debug, Clone)]
pub struct ExampleOperator {
    sleep_secs: u64,
}

impl ExampleOperator {
    pub fn new(sleep_secs: u64) -> Self {
        Self { sleep_secs }
    }
}

impl<R: TaskRuntime> Operator<R> for ExampleOperator {
    type Item = f32;

    async fn execute<'t>(&'t mut self, ctx: &'t Context<'t, R>) -> Result<Self::Item, TaskError> {
        info!(
            "I am task instance {} {} {} {} {}",
            ctx.dag_id(),
            ctx.task_id(),
            ctx.run_id(),
            ctx.try_number(),
            ctx.map_index()
        );
        sleep(Duration::from_secs(self.sleep_secs)).await;
        warn!("This feels very fast! 😎");
        info!("I am done");
        Ok(f32::consts::PI)
    }
}

#[derive(Debug, Clone, Default)]
pub struct PrintXComOperator {
    task_id: String,
}

impl PrintXComOperator {
    pub fn new(task_id: &str) -> Self {
        Self {
            task_id: task_id.to_string(),
        }
    }
}

impl<R: TaskRuntime> Operator<R> for PrintXComOperator {
    type Item = ();

    async fn execute<'t>(&'t mut self, ctx: &'t Context<'t, R>) -> Result<Self::Item, TaskError> {
        let ti = ctx.task_instance();
        match ti.xcom().task_id(&self.task_id).pull::<f32>().await {
            Ok(xcom_value) => {
                info!("XCom value: {}", xcom_value);
            }
            Err(e) => {
                error!("Failed to pull XCom value: {}", e);
            }
        };
        Ok(())
    }
}

static DAG_BAG: LazyLock<DagBag<TokioTaskRuntime>> = LazyLock::new(|| {
    let run = ExampleOperator::new(5).into_task("run");
    let print_xcom = PrintXComOperator::new("run").into_task("print_xcom");
    let mut dag = Dag::new("example_dag");
    dag.add_task(run);
    dag.add_task(print_xcom);
    let mut dag_bag = DagBag::default();
    dag_bag.add_dag(dag);
    dag_bag
});

pub fn get_dag_bag() -> &'static DagBag<TokioTaskRuntime> {
    &DAG_BAG
}
