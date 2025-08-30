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

use crate::{definitions::Dag, execution::LocalTaskRuntime};

pub struct DagBag<R: LocalTaskRuntime> {
    dags: BTreeMap<String, Dag<R>>,
}

impl<R: LocalTaskRuntime> Default for DagBag<R> {
    fn default() -> Self {
        Self {
            dags: BTreeMap::new(),
        }
    }
}

impl<R: LocalTaskRuntime> DagBag<R> {
    pub fn add_dag(&mut self, dag: Dag<R>) {
        self.dags.insert(dag.dag_id().to_string(), dag);
    }

    pub fn get_dag(&self, dag_id: &str) -> Option<&Dag<R>> {
        self.dags.get(dag_id)
    }
}
