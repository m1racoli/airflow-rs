cfg_if::cfg_if! {
    if #[cfg(feature = "std")] {
        use std::fmt;
    } else {
        use core::fmt;
    }
}

use crate::definitions::Task;

/// A trait representing a DAG (Directed Acyclic Graph) that contains tasks.
pub trait Dag: fmt::Debug {
    fn get_task(&self, task_id: &str) -> Option<impl Task>;
}
