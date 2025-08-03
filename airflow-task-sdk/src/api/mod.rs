mod client;
mod datamodels;
#[cfg(feature = "reqwest")]
mod reqwest_impl;

pub use client::ExecutionApiClient;
pub use client::TaskInstanceApiClient;
pub use client::TaskInstanceApiError;
pub use datamodels::AssetProfile;
pub use datamodels::DagRun;
pub use datamodels::InactiveAssetsResponse;
pub use datamodels::PrevSuccessfulDagRunResponse;
pub use datamodels::TICount;
pub use datamodels::TIRunContext;
pub use datamodels::TaskRescheduleStartDate;
pub use datamodels::TaskStatesResponse;
#[cfg(feature = "reqwest")]
pub use reqwest_impl::ReqwestExecutionApiClient;

#[cfg(feature = "reqwest")]
pub type StdExecutionApiClient = reqwest_impl::ReqwestExecutionApiClient;
#[cfg(feature = "reqwest")]
pub type StdExecutionApiError = TaskInstanceApiError<reqwest::Error>;
