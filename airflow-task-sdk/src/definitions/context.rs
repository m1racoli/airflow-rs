extern crate alloc;

use alloc::string::String;

use airflow_common::utils::MapIndex;

use crate::execution::{RuntimeTaskInstance, TaskRuntime};

// TODO remove any owned fields and operate fully on borrowed RuntimeTaskInstance
pub struct Context<'t, R: TaskRuntime> {
    pub dag_id: String,
    pub map_index: MapIndex,
    pub run_id: String,
    pub task_id: String,
    pub task_instance: &'t RuntimeTaskInstance<'t, R>,
    pub ti: &'t RuntimeTaskInstance<'t, R>,
    pub try_number: usize,
}
