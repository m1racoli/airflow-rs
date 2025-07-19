use serde::{Deserialize, Serialize};

use crate::{
    datetime::DateTime,
    models::TaskInstanceLike,
    utils::{MapIndex, SecretString},
};

cfg_if::cfg_if! {
    if #[cfg(feature = "std")] {
        use std::fmt;
    } else {
        extern crate alloc;
        use alloc::string::String;
        use core::fmt;
    }
}

/// Schema for telling task which bundle to run with.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleInfo {
    name: String,
    version: Option<String>,
}

impl BundleInfo {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }
}

/// Execute the given Task.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub struct ExecuteTask {
    token: SecretString,
    ti: TaskInstance,
    // TODO as path
    dag_rel_path: String,
    bundle_info: BundleInfo,
    // TODO as path
    log_path: Option<String>,
}

impl ExecuteTask {
    pub fn token(&self) -> &SecretString {
        &self.token
    }

    pub fn ti(&self) -> &TaskInstance {
        &self.ti
    }

    pub fn dag_rel_path(&self) -> &str {
        &self.dag_rel_path
    }

    pub fn bundle_info(&self) -> &BundleInfo {
        &self.bundle_info
    }

    pub fn log_path(&self) -> Option<&str> {
        self.log_path.as_deref()
    }
}

// TODO: what is context carrier?
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextCarrier;

/// Schema for TaskInstance with minimal required fields needed for Executors and Task SDK.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskInstance {
    id: UniqueTaskInstanceId,

    task_id: String,
    dag_id: String,
    run_id: String,
    try_number: usize,
    map_index: MapIndex,

    pool_slots: usize,
    queue: String,
    priority_weight: u64,

    // TODO tracing info
    // parent_context_carrier: Option<ContextCarrier>,
    // context_carrier: Option<ContextCarrier>,
    queued_dttm: Option<DateTime>,
}

impl TaskInstance {
    pub fn id(&self) -> UniqueTaskInstanceId {
        self.id
    }

    pub fn pool_slots(&self) -> usize {
        self.pool_slots
    }

    pub fn queue(&self) -> &str {
        &self.queue
    }

    pub fn priority_weight(&self) -> u64 {
        self.priority_weight
    }

    pub fn queued_dttm(&self) -> Option<DateTime> {
        self.queued_dttm
    }
}

impl TaskInstanceLike for TaskInstance {
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

/// A unique identifier for a task instance in form of a UUID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
pub struct UniqueTaskInstanceId(uuid::Uuid);

impl fmt::Display for UniqueTaskInstanceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
