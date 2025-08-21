cfg_if::cfg_if! {
    if #[cfg(feature = "std")] {
        use std::error;
    } else {
        use core::error;
    }
}

use airflow_common::utils::SecretString;

use crate::api::{ExecutionApiClient, LocalExecutionApiClient};

/// A factory which builds an execution API client for the given base URL and token.
/// This is a [Send] version of [LocalExecutionApiClientFactory].
pub trait ExecutionApiClientFactory: Send {
    type Client: ExecutionApiClient;
    type Error: error::Error + Send;

    fn create(&self, base_url: &str, token: &SecretString) -> Result<Self::Client, Self::Error>;
}

/// A factory which builds an execution API client for the given base URL and token.
pub trait LocalExecutionApiClientFactory {
    type Client: LocalExecutionApiClient;
    type Error: error::Error;

    fn create(&self, base_url: &str, token: &SecretString) -> Result<Self::Client, Self::Error>;
}

// ExecutionApiClientFactory is just a send version of LocalExecutionApiClientFactory. This blanket
// implementation allows us to use ExecutionApiClientFactory as LocalExecutionApiClientFactory.
impl<F> LocalExecutionApiClientFactory for F
where
    F: ExecutionApiClientFactory,
{
    type Client = F::Client;
    type Error = F::Error;

    fn create(&self, base_url: &str, token: &SecretString) -> Result<Self::Client, Self::Error> {
        self.create(base_url, token)
    }
}
