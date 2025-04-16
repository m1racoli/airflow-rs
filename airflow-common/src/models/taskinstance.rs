use serde::{Deserialize, Serialize};

use crate::utils::MapIndex;

cfg_if::cfg_if! {
    if #[cfg(feature = "std")] {
        use std::fmt;
    } else {
        extern crate alloc;
        use alloc::string::String;
        use alloc::string::ToString;
        use core::fmt;
    }
}

/// Key used to identify a task instance.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskInstanceKey {
    dag_id: String,
    task_id: String,
    run_id: String,
    try_number: usize,
    map_index: MapIndex,
}

impl TaskInstanceKey {
    fn new(
        dag_id: &str,
        task_id: &str,
        run_id: &str,
        try_number: usize,
        map_index: Option<usize>,
    ) -> Self {
        Self {
            dag_id: dag_id.to_string(),
            task_id: task_id.to_string(),
            run_id: run_id.to_string(),
            try_number,
            map_index: map_index.into(),
        }
    }

    pub fn dag_id(&self) -> &str {
        &self.dag_id
    }

    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub fn try_number(&self) -> usize {
        self.try_number
    }

    pub fn map_index(&self) -> Option<usize> {
        self.map_index.into()
    }
}

impl fmt::Display for TaskInstanceKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}.{} {}, try_number: {}, map_index: {}",
            self.dag_id, self.task_id, self.run_id, self.try_number, self.map_index
        )
    }
}

/// A trait for types that represent a task instance.
pub trait TaskInstanceLike {
    fn dag_id(&self) -> &str;
    fn task_id(&self) -> &str;
    fn run_id(&self) -> &str;
    fn try_number(&self) -> usize;
    fn map_index(&self) -> Option<usize>;

    fn ti_key(&self) -> TaskInstanceKey {
        TaskInstanceKey::new(
            self.dag_id(),
            self.task_id(),
            self.run_id(),
            self.try_number(),
            self.map_index(),
        )
    }
}
