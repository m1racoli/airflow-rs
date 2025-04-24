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
pub use reqwest::StdEdgeApiClient;
#[cfg(feature = "reqwest")]
pub use reqwest::StdEdgeApiError;
