use airflow_common::{
    datetime::DateTime, executors::ExecuteTask, models::TaskInstanceLike, utils::MapIndex,
};
use serde::Deserialize;

use crate::models::EdgeWorkerState;

cfg_if::cfg_if! {
    if #[cfg(feature = "std")] {
    } else {
        extern crate alloc;
        use alloc::string::String;
        use alloc::vec::Vec;
    }
}

/// The return class for the worker registration.
#[derive(Debug, Deserialize)]
pub struct WorkerRegistrationReturn {
    /// Time of the last update of the worker.
    pub last_update: DateTime,
}

/// The return class for the worker set state.
#[derive(Debug, Deserialize)]
pub struct WorkerSetStateReturn {
    /// State of the worker from the view of the server.
    pub state: EdgeWorkerState,
    /// List of queues the worker is pulling jobs from. If not provided, worker pulls from all queues.
    pub queues: Option<Vec<String>>,
    /// Comments about the maintenance state of the worker.
    pub maintenance_comments: Option<String>,
}

/// Job that is to be executed on the edge worker.
#[derive(Debug, Deserialize, Clone)]
pub struct EdgeJobFetched {
    /// Identifier of the DAG to which the task belongs.
    pub dag_id: String,
    /// Task name in the DAG.
    pub task_id: String,
    /// Run ID of the DAG execution.
    pub run_id: String,
    /// For dynamically mapped tasks the mapping number, -1 if the task is not mapped.
    pub map_index: MapIndex,
    /// The number of attempt to execute this task.
    pub try_number: usize,
    /// Task definition
    pub command: ExecuteTask,
    /// Number of concurrency slots the job requires.
    pub concurrency_slots: usize,
}

impl TaskInstanceLike for EdgeJobFetched {
    fn dag_id(&self) -> &str {
        &self.dag_id
    }

    fn task_id(&self) -> &str {
        &self.task_id
    }

    fn run_id(&self) -> &str {
        &self.run_id
    }

    fn try_number(&self) -> usize {
        self.try_number
    }

    fn map_index(&self) -> MapIndex {
        self.map_index
    }
}
