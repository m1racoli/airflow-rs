cfg_if::cfg_if! {
    if #[cfg(feature = "std")] {
        use std::collections::BTreeMap;
    } else {
        extern crate alloc;
        use alloc::string::String;
        use alloc::string::ToString;
        use alloc::collections::BTreeMap;
    }
}

use crate::{definitions::Task, execution::TaskRuntime};

pub struct Dag<R: TaskRuntime> {
    dag_id: String,
    tasks: BTreeMap<String, Task<R>>,
}

impl<R: TaskRuntime> Dag<R> {
    pub fn new(dag_id: &str) -> Self {
        Self {
            dag_id: dag_id.to_string(),
            tasks: BTreeMap::new(),
        }
    }

    pub fn dag_id(&self) -> &str {
        &self.dag_id
    }

    pub fn add_task(&mut self, task: Task<R>) {
        self.tasks.insert(task.task_id().to_string(), task);
    }

    pub fn get_task(&self, task_id: &str) -> Option<&Task<R>> {
        self.tasks.get(task_id)
    }
}
