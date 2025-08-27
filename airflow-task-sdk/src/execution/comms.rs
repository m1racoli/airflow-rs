extern crate alloc;
use alloc::string::String;
use alloc::string::ToString;
use core::error;

use airflow_common::utils::MapIndex;

use crate::api::{ExecutionApiError, datamodels::*};
use crate::definitions::serde::JsonValue;

/// Messages sent from the supervisor to the task.
#[derive(Debug)]
pub enum ToTask {
    Empty,
    XCom(XComResponse),
    XComCount(usize),
}

/// Messages sent from the task to the supervisor.
#[derive(Debug)]
pub enum ToSupervisor {
    SucceedTask(TISuccessStatePayload),
    GetXCom {
        key: String,
        dag_id: String,
        run_id: String,
        task_id: String,
        map_index: Option<MapIndex>,
        include_prior_dates: Option<bool>,
    },
    GetXComCount {
        key: String,
        dag_id: String,
        run_id: String,
        task_id: String,
    },
    // GetXComSequenceItem
    // GetXComSequenceSlice
    SetXCom {
        key: String,
        value: JsonValue,
        dag_id: String,
        run_id: String,
        task_id: String,
        map_index: Option<MapIndex>,
        mapped_length: Option<usize>,
    },
    DeleteXCom {
        key: String,
        dag_id: String,
        run_id: String,
        task_id: String,
        map_index: Option<MapIndex>,
    },
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

    pub async fn get_xcom(
        &self,
        key: String,
        dag_id: String,
        run_id: String,
        task_id: String,
        map_index: Option<MapIndex>,
        include_prior_dates: Option<bool>,
    ) -> Result<XComResponse, SupervisorCommsError> {
        match self
            .comms
            .send(ToSupervisor::GetXCom {
                key,
                dag_id,
                run_id,
                task_id,
                map_index,
                include_prior_dates,
            })
            .await?
        {
            ToTask::XCom(r) => Ok(r),
            r => Err(SupervisorCommsError::UnexpectedResponse(r)),
        }
    }

    pub async fn get_xcom_count(
        &self,
        key: String,
        dag_id: String,
        run_id: String,
        task_id: String,
    ) -> Result<usize, SupervisorCommsError> {
        match self
            .comms
            .send(ToSupervisor::GetXComCount {
                key,
                dag_id,
                run_id,
                task_id,
            })
            .await?
        {
            ToTask::XComCount(r) => Ok(r),
            r => Err(SupervisorCommsError::UnexpectedResponse(r)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn set_xcom(
        &self,
        key: String,
        value: JsonValue,
        dag_id: String,
        run_id: String,
        task_id: String,
        map_index: Option<MapIndex>,
        mapped_length: Option<usize>,
    ) -> Result<(), SupervisorCommsError> {
        match self
            .comms
            .send(ToSupervisor::SetXCom {
                key,
                value,
                dag_id,
                run_id,
                task_id,
                map_index,
                mapped_length,
            })
            .await?
        {
            ToTask::Empty => Ok(()),
            r => Err(SupervisorCommsError::UnexpectedResponse(r)),
        }
    }

    pub async fn delete_xcom(
        &self,
        key: String,
        dag_id: String,
        run_id: String,
        task_id: String,
        map_index: Option<MapIndex>,
    ) -> Result<(), SupervisorCommsError> {
        match self
            .comms
            .send(ToSupervisor::DeleteXCom {
                key,
                dag_id,
                run_id,
                task_id,
                map_index,
            })
            .await?
        {
            ToTask::Empty => Ok(()),
            r => Err(SupervisorCommsError::UnexpectedResponse(r)),
        }
    }
}
