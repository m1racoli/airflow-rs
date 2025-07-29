#[cfg(feature = "reqwest")]
use airflow_common::api::StdJWTGenerator;

mod client;
mod models;
#[cfg(feature = "reqwest")]
mod reqwest;

pub use client::EdgeApiClient;
pub use models::EdgeJobFetched;
pub use models::HealthReturn;
pub use models::WorkerRegistrationReturn;
pub use models::WorkerSetStateReturn;
#[cfg(feature = "reqwest")]
pub use reqwest::ReqwestEdgeApiClient;
#[cfg(feature = "reqwest")]
pub use reqwest::ReqwestEdgeApiError;

#[cfg(feature = "reqwest")]
pub type StdEdgeApiClient = reqwest::ReqwestEdgeApiClient<StdJWTGenerator>;
#[cfg(feature = "reqwest")]
pub type StdEdgeApiError = reqwest::ReqwestEdgeApiError<StdJWTGenerator>;
