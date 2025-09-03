#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(not(feature = "std"))]
extern crate alloc;

pub mod api;
pub mod bases;
pub mod definitions;
pub mod execution;

pub mod prelude {
    pub use crate::bases::operator::Operator;
    pub use crate::definitions::Context;
    pub use crate::definitions::Dag;
    pub use crate::definitions::DagBag;
    pub use crate::definitions::Task;
    pub use crate::definitions::TaskError;
    pub use crate::execution::TaskRuntime;
}
