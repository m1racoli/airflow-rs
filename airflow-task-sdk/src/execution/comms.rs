extern crate alloc;
use alloc::string::String;
use alloc::string::ToString;
use core::error;

use crate::api::{ExecutionApiError, datamodels::*};

/// Messages sent from the supervisor to the task.
#[derive(Debug)]
pub enum ToTask {
    Empty,
    _Never, // until we have more members this is used to not have unreachable patterns when matching the rest
}

/// Messages sent from the task to the supervisor.
#[derive(Debug)]
pub enum ToSupervisor {
    SucceedTask(TISuccessStatePayload),
}

#[derive(thiserror::Error, Debug)]
pub enum SupervisorCommsError {
    #[error("Failed to send message to supervisor: {0}")]
    Send(String),
    #[error("The supervisor is gone: {0}")]
    SupervisorGone(String),
    #[error("Received unexpected response from supervisor: {0:?}")]
    UnexpectedResponse(ToTask),
    #[error("API error occurred: {0}")]
    Api(String),
}

impl<E> From<ExecutionApiError<E>> for SupervisorCommsError
where
    E: error::Error,
{
    fn from(err: ExecutionApiError<E>) -> Self {
        SupervisorCommsError::Api(err.to_string())
    }
}

/// A trait for communicating with the supervisor.
#[trait_variant::make(SupervisorComms: Send)]
pub trait LocalSupervisorComms {
    async fn send(&self, msg: ToSupervisor) -> Result<ToTask, SupervisorCommsError>;
}

#[derive(Debug)]
pub struct SupervisorClient<C: LocalSupervisorComms> {
    comms: C,
}

impl<C: LocalSupervisorComms> From<C> for SupervisorClient<C> {
    fn from(comms: C) -> Self {
        SupervisorClient { comms }
    }
}

/// The supervisor client implements individual requests to the supervisor,
/// given a supervisor comms instance.
impl<C: LocalSupervisorComms> SupervisorClient<C> {
    pub async fn succeed_task(
        &self,
        msg: TISuccessStatePayload,
    ) -> Result<(), SupervisorCommsError> {
        match self.comms.send(ToSupervisor::SucceedTask(msg)).await? {
            ToTask::Empty => Ok(()),
            r => Err(SupervisorCommsError::UnexpectedResponse(r)),
        }
    }
}
