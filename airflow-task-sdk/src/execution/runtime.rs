use core::time::Duration;

use airflow_common::datetime::TimeProvider;

use crate::{
    definitions::DagBag,
    execution::{
        ExecutionError, LocalSupervisorComms, StartupDetails, SupervisorCommsError, ToSupervisor,
        ToTask,
    },
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
pub trait LocalTaskRuntime {
    type TaskHandle: LocalTaskHandle;
    type Instant: Copy;
    type TimeProvider: TimeProvider;
    type Comms: LocalSupervisorComms;

    fn now(&self) -> Self::Instant;
    fn elapsed(&self, start: Self::Instant) -> Duration;
    fn hostname(&self) -> &str;
    fn unixname(&self) -> &str;
    fn pid(&self) -> u32;
    fn time_provider(&self) -> &Self::TimeProvider;

    fn start(&self, details: StartupDetails, dag_bag: &'static DagBag) -> Self::TaskHandle;
}
