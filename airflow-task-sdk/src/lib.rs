#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(not(feature = "std"))]
#[macro_use]
extern crate alloc;

pub mod api;
pub mod definitions;
pub mod execution;

pub mod prelude {
    pub use crate::api::ExecutionApiClient;
    pub use crate::api::TaskInstanceApiClient;
    pub use crate::definitions::Context;
    pub use crate::definitions::Dag;
    pub use crate::definitions::DagBag;
    pub use crate::definitions::Task;
    pub use crate::definitions::TaskError;
}
