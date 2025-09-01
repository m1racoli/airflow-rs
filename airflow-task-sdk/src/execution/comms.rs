extern crate alloc;
use crate::{
    api::{ExecutionApiError, datamodels::*},
    execution::{ExecutionResultTIState, TaskRuntime},
};
use airflow_common::{datetime::UtcDateTime, serialization::serde::JsonValue, utils::MapIndex};
use alloc::{
    string::{String, ToString},
    vec::Vec,
};
use core::error;

/// Messages sent from the supervisor to the task.
#[derive(Debug)]
pub enum ToTask {
    Empty,
    XCom(XComResponse),
    XComCount(usize),
    XComSequenceIndex(JsonValue),
    XComSequenceSlice(Vec<JsonValue>),
}

/// Messages sent from the task to the supervisor.
#[derive(Debug)]
pub enum ToSupervisor {
    SucceedTask {
        end_date: UtcDateTime,
        task_outlets: Vec<AssetProfile>,
        outlet_events: Vec<()>, // TODO outlet events
        rendered_map_index: Option<String>,
    },
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
    GetXComSequenceItem {
        key: String,
        dag_id: String,
        run_id: String,
        task_id: String,
        offset: usize,
    },
    GetXComSequenceSlice {
        key: String,
        dag_id: String,
        run_id: String,
        task_id: String,
        start: Option<usize>,
        stop: Option<usize>,
        step: Option<usize>,
        include_prior_dates: Option<bool>,
    },
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
    RetryTask {
        end_date: UtcDateTime,
        rendered_map_index: Option<String>,
    },
    TaskState {
        state: ExecutionResultTIState,
        end_date: UtcDateTime,
        rendered_map_index: Option<String>,
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
#[trait_variant::make(Send + Sync)]
pub trait SupervisorComms {
    async fn send(&self, msg: ToSupervisor) -> Result<ToTask, SupervisorCommsError>;
}

#[derive(Debug)]
pub struct SupervisorClient<R: TaskRuntime> {
    comms: R::Comms,
}

/// The supervisor client implements individual requests to the supervisor,
/// given a supervisor comms instance.
impl<R: TaskRuntime> SupervisorClient<R> {
    pub(crate) fn new(comms: R::Comms) -> Self {
        Self { comms }
    }

    pub async fn succeed_task(
        &self,
        end_date: UtcDateTime,
        task_outlets: Vec<AssetProfile>,
        outlet_events: Vec<()>, // TODO outlet events
        rendered_map_index: Option<String>,
    ) -> Result<(), SupervisorCommsError> {
        match self
            .comms
            .send(ToSupervisor::SucceedTask {
                end_date,
                task_outlets,
                outlet_events,
                rendered_map_index,
            })
            .await?
        {
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

    pub async fn get_xcom_sequence_item(
        &self,
        key: String,
        dag_id: String,
        run_id: String,
        task_id: String,
        offset: usize,
    ) -> Result<JsonValue, SupervisorCommsError> {
        match self
            .comms
            .send(ToSupervisor::GetXComSequenceItem {
                key,
                dag_id,
                run_id,
                task_id,
                offset,
            })
            .await?
        {
            ToTask::XComSequenceIndex(r) => Ok(r),
            r => Err(SupervisorCommsError::UnexpectedResponse(r)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn get_xcom_sequence_slice(
        &self,
        key: String,
        dag_id: String,
        run_id: String,
        task_id: String,
        start: Option<usize>,
        stop: Option<usize>,
        step: Option<usize>,
        include_prior_dates: Option<bool>,
    ) -> Result<Vec<JsonValue>, SupervisorCommsError> {
        match self
            .comms
            .send(ToSupervisor::GetXComSequenceSlice {
                key,
                dag_id,
                run_id,
                task_id,
                start,
                stop,
                step,
                include_prior_dates,
            })
            .await?
        {
            ToTask::XComSequenceSlice(r) => Ok(r),
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

    pub async fn retry_task(
        &self,
        end_date: UtcDateTime,
        rendered_map_index: Option<String>,
    ) -> Result<(), SupervisorCommsError> {
        match self
            .comms
            .send(ToSupervisor::RetryTask {
                end_date,
                rendered_map_index,
            })
            .await?
        {
            ToTask::Empty => Ok(()),
            r => Err(SupervisorCommsError::UnexpectedResponse(r)),
        }
    }

    pub async fn task_state(
        &self,
        state: ExecutionResultTIState,
        end_date: UtcDateTime,
        rendered_map_index: Option<String>,
    ) -> Result<(), SupervisorCommsError> {
        match self
            .comms
            .send(ToSupervisor::TaskState {
                state,
                end_date,
                rendered_map_index,
            })
            .await?
        {
            ToTask::Empty => Ok(()),
            r => Err(SupervisorCommsError::UnexpectedResponse(r)),
        }
    }
}
