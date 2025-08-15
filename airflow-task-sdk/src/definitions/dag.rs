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

use crate::definitions::Task;

pub struct Dag {
    dag_id: String,
    tasks: BTreeMap<String, Task>,
}

impl Dag {
    pub fn new(dag_id: &str) -> Self {
        Self {
            dag_id: dag_id.to_string(),
            tasks: BTreeMap::new(),
        }
    }

    pub fn dag_id(&self) -> &str {
        &self.dag_id
    }

    pub fn add_task(&mut self, task: Task) {
        self.tasks.insert(task.task_id().to_string(), task);
    }

    pub fn get_task(&self, task_id: &str) -> Option<&Task> {
        self.tasks.get(task_id)
    }
}
