cfg_if::cfg_if! {
    if #[cfg(feature = "std")] {
        use std::time;
    } else {
        use core::time;
    }
}

use crate::{
    api::EdgeJobFetched,
    worker::{IntercomMessage, LocalEdgeJob, LocalIntercom, WorkerState},
};
use airflow_task_sdk::{definitions::DagBag, execution::TaskRuntime};

#[trait_variant::make(WorkerRuntime: Send)]
pub trait LocalWorkerRuntime {
    type Job: LocalEdgeJob;
    type Intercom: LocalIntercom;
    type TaskRuntime: TaskRuntime;

    async fn sleep(&mut self, duration: time::Duration) -> Option<IntercomMessage>;
    fn intercom(&self) -> Self::Intercom;
    fn launch(&self, job: EdgeJobFetched, dag_bag: &'static DagBag<Self::TaskRuntime>)
    -> Self::Job;
    fn concurrency(&self) -> usize;
    async fn on_update(&mut self, state: &WorkerState);
    fn hostname(&self) -> &str;
}
