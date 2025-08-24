use core::time::Duration;

use airflow_common::datetime::TimeProvider;

use crate::{
    definitions::DagBag,
    execution::{ExecutionError, StartupDetails, SupervisorCommsError, ToSupervisor, ToTask},
};

#[derive(Debug)]
pub enum ServiceResult {
    Comms(ToSupervisor),
    None,
    Terminated(Result<(), ExecutionError>),
}

impl From<ExecutionError> for ServiceResult {
    fn from(e: ExecutionError) -> Self {
        ServiceResult::Terminated(Err(e))
    }
}

#[trait_variant::make(TaskHandle: Send)]
pub trait LocalTaskHandle {
    async fn service(&mut self, timeout: Duration) -> ServiceResult;

    // TODO rename to kill to reflect Python equivalent
    fn abort(&self);

    /// Send a response to the task.
    /// TODO should this return a result?
    async fn respond(&mut self, msg: Result<ToTask, SupervisorCommsError>);
}

#[trait_variant::make(TaskRuntime: Send)]
pub trait LocalTaskRuntime<T>
where
    T: TimeProvider,
{
    type TaskHandle: LocalTaskHandle;
    type Instant: Copy;

    fn now(&self) -> Self::Instant;
    fn elapsed(&self, start: Self::Instant) -> Duration;
    fn hostname(&self) -> &str;
    fn unixname(&self) -> &str;
    fn pid(&self) -> u32;

    fn start(
        &self,
        time_provider: T,
        details: StartupDetails,
        dag_bag: &'static DagBag,
    ) -> Self::TaskHandle;
}
