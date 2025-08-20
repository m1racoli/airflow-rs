cfg_if::cfg_if! {
    if #[cfg(feature = "std")] {
        use std::time::Duration;
    } else {
        use core::time::Duration;
    }
}

use airflow_common::datetime::TimeProvider;

use crate::{
    api::LocalExecutionApiClient,
    definitions::DagBag,
    execution::{ExecutionError, ExecutionResultTIState, StartupDetails},
};

#[trait_variant::make(TaskHandle: Send)]
pub trait LocalTaskHandle<C>:
    Future<Output = Result<ExecutionResultTIState, ExecutionError<C>>> + Unpin
where
    C: LocalExecutionApiClient,
{
    fn abort(&self);
}

#[trait_variant::make(TaskRuntime: Send)]
pub trait LocalTaskRuntime<C, T>
where
    C: LocalExecutionApiClient,
    T: TimeProvider,
{
    type ActivityHandle: LocalTaskHandle<C>;
    type Instant: Copy;

    fn now(&self) -> Self::Instant;
    fn elapsed(&self, start: Self::Instant) -> Duration;
    fn hostname(&self) -> &str;
    fn unixname(&self) -> &str;
    fn pid(&self) -> u32;

    fn start(
        &self,
        client: C,
        time_provider: T,
        details: StartupDetails,
        dag_bag: &'static DagBag,
    ) -> Self::ActivityHandle;

    async fn wait(
        &self,
        handle: &mut Self::ActivityHandle,
        timeout: Duration,
    ) -> Option<Result<ExecutionResultTIState, ExecutionError<C>>>;
}
