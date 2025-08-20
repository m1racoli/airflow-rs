use airflow_common::{
    api::{JWTGenerator, JsonWebTokenJWTGenerator},
    datetime::StdTimeProvider,
};
use airflow_edge_sdk::{
    api::{EdgeApiError, ReqwestEdgeApiClient, ReqwestEdgeApiError},
    worker::EdgeWorkerError,
};
use airflow_task_sdk::api::{
    ReqwestExecutionApiClient, ReqwestExecutionApiClientFactory, TaskInstanceApiError,
};

pub mod example;
pub mod tokio;
pub mod tracing;

pub type StdJWTGenerator = JsonWebTokenJWTGenerator<StdTimeProvider>;
pub type StdEdgeApiClient = ReqwestEdgeApiClient<StdJWTGenerator>;
pub type StdEdgeApiError = EdgeApiError<
    ReqwestEdgeApiError<<JsonWebTokenJWTGenerator<StdTimeProvider> as JWTGenerator>::Error>,
>;
pub type StdExecutionApiClient = ReqwestExecutionApiClient;
/// TODO: leakage of E, which might need crate installed, i.e. `request::Error``
pub type StdTaskInstanceApiError = TaskInstanceApiError<reqwest::Error>;
pub type StdExecutionApiClientFactory = ReqwestExecutionApiClientFactory;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    EdgeWorker(#[from] EdgeWorkerError<StdEdgeApiClient>),
    #[error("IO error: {0}")]
    IO(#[from] std::io::Error),
}
