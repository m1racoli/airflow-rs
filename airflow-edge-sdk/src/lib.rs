#![cfg_attr(not(feature = "std"), no_std)]
pub mod api;
pub mod models;

pub mod prelude {
    pub use crate::api::EdgeApiClient;
}
