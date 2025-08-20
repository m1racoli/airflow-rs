mod task_instances;

use std::sync::{Arc, RwLock};

use crate::api::{ExecutionApiClient, TaskInstanceApiClient, client::ExecutionApiClientFactory};
use airflow_common::utils::SecretString;
use reqwest::{Method, header::HeaderMap};

use serde::Serialize;
use task_instances::ReqwestTaskInstanceApiClient;

static API_VERSION: &str = "2025-08-10";

/// An ExecutionAPIClient implementation using Reqwest.
#[derive(Debug, Clone)]
pub struct ReqwestExecutionApiClient {
    client: reqwest::Client,
    base_url: String,
    token: Arc<RwLock<SecretString>>,
}

impl ReqwestExecutionApiClient {
    pub fn new(base_url: &str, token: &SecretString) -> Result<Self, reqwest::Error> {
        let mut headers = HeaderMap::new();
        headers.insert("accept", "application/json".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());
        headers.insert("airflow-api-version", API_VERSION.parse().unwrap());

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .user_agent("airflow-rs-task-sdk")
            .build()?;

        Ok(Self {
            client,
            base_url: base_url.to_string(),
            token: Arc::new(RwLock::new(token.clone())),
        })
    }

    fn request<T: Serialize + ?Sized>(
        &self,
        method: Method,
        path: &str,
        json: Option<&T>,
    ) -> Result<reqwest::RequestBuilder, reqwest::Error> {
        let r = self.token.read().unwrap();
        let builder = self
            .client
            .request(method, format!("{}/{}", self.base_url, path))
            .header("authorization", format!("Bearer {}", r.secret()));
        let builder = if let Some(json) = json {
            builder.json(json)
        } else {
            builder
        };
        Ok(builder)
    }
}

impl ExecutionApiClient for ReqwestExecutionApiClient {
    type Error = reqwest::Error;

    fn task_instances(&self) -> impl TaskInstanceApiClient<Error = Self::Error> {
        ReqwestTaskInstanceApiClient(self.clone())
    }
}

#[derive(Debug, Clone, Default)]
pub struct ReqwestExecutionApiClientFactory;

impl ExecutionApiClientFactory for ReqwestExecutionApiClientFactory {
    type Client = ReqwestExecutionApiClient;
    type Error = reqwest::Error;

    fn create(&self, base_url: &str, token: &SecretString) -> Result<Self::Client, Self::Error> {
        ReqwestExecutionApiClient::new(base_url, token)
    }
}
