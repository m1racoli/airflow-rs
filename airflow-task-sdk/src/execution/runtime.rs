use core::future::Future;
use core::time::Duration;

use airflow_common::datetime::TimeProvider;

use crate::{
    definitions::DagBag,
    execution::{
        ExecutionError, StartupDetails, SupervisorComms, SupervisorCommsError, ToSupervisor, ToTask,
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

#[trait_variant::make(Send)]
pub trait TaskHandle {
    async fn service(&mut self, timeout: Duration) -> ServiceResult;

    // TODO rename to kill to reflect Python equivalent
    fn abort(&self);

    /// Send a response to the task.
    async fn respond(&mut self, msg: Result<ToTask, SupervisorCommsError>);
}

pub trait TaskRuntime: Sized + 'static {
    type TaskHandle: TaskHandle;
    type Instant: Copy;
    type TimeProvider: TimeProvider;
    type Comms: SupervisorComms;

    fn now(&self) -> Self::Instant;
    fn elapsed(&self, start: Self::Instant) -> Duration;
    fn hostname(&self) -> &str;
    fn unixname(&self) -> &str;
    fn pid(&self) -> u32;
    fn time_provider(&self) -> &Self::TimeProvider;

    fn start(&self, details: StartupDetails, dag_bag: &'static DagBag<Self>) -> Self::TaskHandle;

    fn sleep(duration: Duration) -> impl Future<Output = ()> + Send + Sync;
}
