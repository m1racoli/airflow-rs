cfg_if::cfg_if! {
    if #[cfg(feature = "std")] {
        use std::error;
    } else {
        use core::error;
    }
}

use airflow_common::utils::SecretString;

use crate::api::LocalExecutionApiClient;

/// A factory which builds an execution API client for the given base URL and token.
#[trait_variant::make(ExecutionApiClientFactory: Send)]
pub trait LocalExecutionApiClientFactory {
    type Client: LocalExecutionApiClient;
    type Error: error::Error;

    fn create(&self, base_url: &str, token: &SecretString) -> Result<Self::Client, Self::Error>;
}
