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
pub use reqwest::DefaultEdgeApiClient;
#[cfg(feature = "reqwest")]
pub use reqwest::DefaultEdgeApiError;

#[cfg(feature = "reqwest")]
pub type StdEdgeApiClient = reqwest::DefaultEdgeApiClient<StdJWTGenerator>;
#[cfg(feature = "reqwest")]
pub type StdEdgeApiError = reqwest::DefaultEdgeApiError<StdJWTGenerator>;
