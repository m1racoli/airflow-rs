use serde::{Deserialize, Serialize};

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
    map_index: i64, //TODO: can we use custom serde implementation?
}

impl TaskInstanceKey {
    fn new(
        dag_id: &str,
        task_id: &str,
        run_id: &str,
        try_number: usize,
        map_index: Option<usize>,
    ) -> Self {
        let map_index = match map_index {
            Some(i) => i as i64,
            None => -1,
        };
        Self {
            dag_id: dag_id.to_string(),
            task_id: task_id.to_string(),
            run_id: run_id.to_string(),
            try_number,
            map_index,
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
        if self.map_index == -1 {
            None
        } else {
            Some(self.map_index as usize)
        }
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
