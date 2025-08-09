mod client;
mod models;
#[cfg(feature = "reqwest")]
mod reqwest_impl;

pub use client::EdgeApiClient;
pub use client::LocalEdgeApiClient;
pub use models::EdgeJobFetched;
pub use models::HealthReturn;
pub use models::WorkerRegistrationReturn;
pub use models::WorkerSetStateReturn;
#[cfg(feature = "reqwest")]
pub use reqwest_impl::ReqwestEdgeApiClient;
#[cfg(feature = "reqwest")]
pub use reqwest_impl::ReqwestEdgeApiError;
