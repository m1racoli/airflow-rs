#![cfg_attr(not(feature = "std"), no_std)]

pub mod api;
pub mod datetime;
pub mod executors;
pub mod models;
pub mod utils;

#[cfg(not(feature = "std"))]
#[macro_use]
extern crate alloc;

pub mod prelude {
    pub use crate::api::JWTGenerator;
    pub use crate::datetime::TimeProvider;
    pub use crate::models::TaskInstanceLike;
}
