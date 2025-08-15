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

use crate::definitions::Dag;

#[derive(Default)]
pub struct DagBag {
    dags: BTreeMap<String, Dag>,
}

impl DagBag {
    pub fn add_dag(&mut self, dag: Dag) {
        self.dags.insert(dag.dag_id().to_string(), dag);
    }

    pub fn get_dag(&self, dag_id: &str) -> Option<&Dag> {
        self.dags.get(dag_id)
    }
}
