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
use airflow_task_sdk::definitions::DagBag;

#[trait_variant::make(Runtime: Send)]
pub trait LocalRuntime<'dags> {
    type Job: LocalEdgeJob;
    type Intercom: LocalIntercom;

    async fn sleep(&mut self, duration: time::Duration) -> Option<IntercomMessage>;
    fn intercom(&self) -> Self::Intercom;
    fn launch(&self, job: EdgeJobFetched, dag_bag: &'dags DagBag) -> Self::Job;
    fn concurrency(&self) -> usize;
    async fn on_update(&mut self, state: &WorkerState);
}
