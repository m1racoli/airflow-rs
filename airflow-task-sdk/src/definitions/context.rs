cfg_if::cfg_if! {
    if #[cfg(feature = "std")] {
    } else {
        extern crate alloc;
        use alloc::string::String;
    }
}

use airflow_common::utils::MapIndex;

#[derive(Debug)]
pub struct Context {
    pub dag_id: String,
    pub task_id: String,
    pub run_id: String,
    pub try_number: usize,
    pub map_index: MapIndex,
}
