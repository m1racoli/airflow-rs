cfg_if::cfg_if! {
    if #[cfg(feature = "std")] {
        use std::fmt;
    } else {
        use core::fmt;
    }
}

use crate::definitions::Dag;

// A trait representing a collection of DAGs.
pub trait DagBag: fmt::Debug {
    fn get_dag(&self, dag_id: &str) -> Option<impl Dag>;
}
