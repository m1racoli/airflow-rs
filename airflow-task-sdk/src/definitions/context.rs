use airflow_common::utils::MapIndex;

use crate::execution::{RuntimeTaskInstance, TaskRuntime};

pub struct Context<'t, R: TaskRuntime> {
    ti: &'t RuntimeTaskInstance<'t, R>,
}

impl<'t, R: TaskRuntime> Context<'t, R> {
    pub(crate) fn new(ti: &'t RuntimeTaskInstance<'t, R>) -> Self {
        Self { ti }
    }

    pub fn dag_id(&self) -> &str {
        self.ti.dag_id()
    }

    pub fn map_index(&self) -> MapIndex {
        self.ti.map_index()
    }

    pub fn run_id(&self) -> &str {
        self.ti.run_id()
    }

    pub fn task_id(&self) -> &str {
        self.ti.task_id()
    }

    pub fn task_instance(&self) -> &RuntimeTaskInstance<'t, R> {
        self.ti
    }

    pub fn ti(&self) -> &RuntimeTaskInstance<'t, R> {
        self.ti
    }

    pub fn try_number(&self) -> usize {
        self.ti.try_number()
    }
}
