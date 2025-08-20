mod task_instances;

use std::sync::{Arc, RwLock};

use crate::api::{
    ExecutionApiClient, TaskInstanceApiClient, TaskInstanceApiError,
    client::ExecutionApiClientFactory,
};
use airflow_common::utils::SecretString;
use reqwest::{Method, Response, StatusCode, header::HeaderMap};

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

    async fn handle_response(
        &self,
        response: Response,
    ) -> Result<Response, TaskInstanceApiError<reqwest::Error>> {
        match response.status() {
            StatusCode::NOT_FOUND => Err(TaskInstanceApiError::NotFound(response.text().await?)),
            StatusCode::CONFLICT => Err(TaskInstanceApiError::Conflict(response.text().await?)),
            _ => match response.error_for_status() {
                Ok(response) => Ok(response),
                Err(e) => Err(TaskInstanceApiError::Other(e)),
            },
        }
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

#[cfg(test)]
mod tests {

    use std::sync::LazyLock;

    use airflow_common::{
        datetime::UtcDateTime,
        executors::UniqueTaskInstanceId,
        utils::{SecretString, TerminalTIStateNonSuccess},
    };
    use httpmock::MockServer;

    use crate::api::ExecutionApiClient;

    use super::*;

    static ID: LazyLock<UniqueTaskInstanceId> = LazyLock::new(|| {
        UniqueTaskInstanceId::parse_str("67e55044-10b1-426f-9247-bb680e5fe0c8").unwrap()
    });
    static TOKEN: LazyLock<SecretString> = LazyLock::new(|| SecretString::from("secret123"));
    static HOSTNAME: &str = "test-host";
    static UNIXNAME: &str = "test-unix";
    static PID: u32 = 1234;
    /// 2019-10-12T07:20:50.52Z
    static WHEN: LazyLock<UtcDateTime> =
        LazyLock::new(|| UtcDateTime::from_timestamp(1570864850, 520_000_000).unwrap());

    static TI_NOT_FOUND_BODY: &str =
        r#"{"detail":{"reason":"not_found","message":"Task Instance not found"}}"#;

    static TI_INVALID_STATE_BODY: &str = r#"{"detail":{"reason":"invalid_state","message":"TI was not in the right state","previous_state":"failed"}}"#;

    fn client(base_url: &str) -> ReqwestExecutionApiClient {
        ReqwestExecutionApiClient::new(base_url, &TOKEN).unwrap()
    }

    #[tokio::test]
    async fn test_start_ok() {
        let server = MockServer::start_async().await;
        let client = client(&server.base_url());

        let http_mock = server
            .mock_async(|when, then| {
                when.method("PATCH")
                    .path("/task-instances/67e55044-10b1-426f-9247-bb680e5fe0c8/run")
                    .header("authorization", "Bearer secret123")
                    .body(r#"{"state":"running","hostname":"test-host","unixname":"test-unix","pid":1234,"start_date":"2019-10-12T07:20:50.520Z"}"#);
                then.status(200).body(
                    r#"{
                        "dag_run":{
                            "dag_id": "example_dag",
                            "run_id": "manual__2019-10-12T07:20:50.52Z",
                            "logical_date": "2019-10-12T07:20:50.52Z",
                            "run_after": "2019-10-12T07:20:50.52Z",
                            "start_date": "2019-10-12T07:20:50.52Z",
                            "clear_number": 0,
                            "run_type": "manual"
                        },
                        "task_reschedule_count": 0,
                        "max_tries": 1,
                        "should_retry": false
                    }"#,
                );
            })
            .await;

        let result = client
            .task_instances()
            .start(&ID, HOSTNAME, UNIXNAME, PID, &WHEN)
            .await;

        http_mock.assert_async().await;
        result.unwrap();
    }

    #[tokio::test]
    async fn test_start_not_found() {
        let server = MockServer::start_async().await;
        let client = client(&server.base_url());

        let http_mock = server
            .mock_async(|when, then| {
                when.method("PATCH")
                    .path("/task-instances/67e55044-10b1-426f-9247-bb680e5fe0c8/run")
                    .header("authorization", "Bearer secret123");
                then.status(404).body(TI_NOT_FOUND_BODY);
            })
            .await;

        let result = client
            .task_instances()
            .start(&ID, HOSTNAME, UNIXNAME, PID, &WHEN)
            .await;

        http_mock.assert_async().await;
        let result = result.unwrap_err();

        assert!(
            matches!(result, TaskInstanceApiError::NotFound(_)),
            "Expected NotFound error, got: {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_start_conflict() {
        let server = MockServer::start_async().await;
        let client = client(&server.base_url());

        let http_mock = server
            .mock_async(|when, then| {
                when.method("PATCH")
                    .path("/task-instances/67e55044-10b1-426f-9247-bb680e5fe0c8/run")
                    .header("authorization", "Bearer secret123");
                then.status(409).body(TI_INVALID_STATE_BODY);
            })
            .await;

        let result = client
            .task_instances()
            .start(&ID, HOSTNAME, UNIXNAME, PID, &WHEN)
            .await;

        http_mock.assert_async().await;
        let result = result.unwrap_err();

        assert!(
            matches!(result, TaskInstanceApiError::Conflict(_)),
            "Expected Conflict error, got: {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_finish_ok() {
        let server = MockServer::start_async().await;
        let client = client(&server.base_url());

        let http_mock = server
            .mock_async(|when, then| {
                when.method("PATCH")
                    .path("/task-instances/67e55044-10b1-426f-9247-bb680e5fe0c8/state")
                    .header("authorization", "Bearer secret123")
                    .body(r#"{"state":"failed","end_date":"2019-10-12T07:20:50.520Z","rendered_map_index":null}"#);
                then.status(204);
            })
            .await;

        let result = client
            .task_instances()
            .finish(&ID, TerminalTIStateNonSuccess::Failed, &WHEN, None)
            .await;

        http_mock.assert_async().await;
        result.unwrap();
    }

    #[tokio::test]
    async fn test_finish_not_found() {
        let server = MockServer::start_async().await;
        let client = client(&server.base_url());

        let http_mock = server
            .mock_async(|when, then| {
                when.method("PATCH")
                    .path("/task-instances/67e55044-10b1-426f-9247-bb680e5fe0c8/state")
                    .header("authorization", "Bearer secret123");
                then.status(404).body(TI_NOT_FOUND_BODY);
            })
            .await;

        let result = client
            .task_instances()
            .finish(&ID, TerminalTIStateNonSuccess::Failed, &WHEN, None)
            .await;

        http_mock.assert_async().await;
        let result = result.unwrap_err();
        assert!(
            matches!(result, TaskInstanceApiError::NotFound(_)),
            "Expected NotFound error, got: {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_finish_conflict() {
        let server = MockServer::start_async().await;
        let client = client(&server.base_url());

        let http_mock = server
            .mock_async(|when, then| {
                when.method("PATCH")
                    .path("/task-instances/67e55044-10b1-426f-9247-bb680e5fe0c8/state")
                    .header("authorization", "Bearer secret123");
                then.status(409).body(TI_INVALID_STATE_BODY);
            })
            .await;

        let result = client
            .task_instances()
            .finish(&ID, TerminalTIStateNonSuccess::Failed, &WHEN, None)
            .await;

        http_mock.assert_async().await;
        let result = result.unwrap_err();
        assert!(
            matches!(result, TaskInstanceApiError::Conflict(_)),
            "Expected Conflict error, got: {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_succeed_ok() {
        let server = MockServer::start_async().await;
        let client = client(&server.base_url());

        let http_mock = server
            .mock_async(|when, then| {
                when.method("PATCH")
                    .path("/task-instances/67e55044-10b1-426f-9247-bb680e5fe0c8/state")
                    .header("authorization", "Bearer secret123")
                    .body(r#"{"state":"success","end_date":"2019-10-12T07:20:50.520Z","task_outlets":[],"outlet_events":[],"rendered_map_index":null}"#);
                then.status(204);
            })
            .await;

        let result = client
            .task_instances()
            .succeed(&ID, &WHEN, &[], &[], None)
            .await;

        http_mock.assert_async().await;
        result.unwrap();
    }

    #[tokio::test]
    async fn test_succeed_not_found() {
        let server = MockServer::start_async().await;
        let client = client(&server.base_url());

        let http_mock = server
            .mock_async(|when, then| {
                when.method("PATCH")
                    .path("/task-instances/67e55044-10b1-426f-9247-bb680e5fe0c8/state")
                    .header("authorization", "Bearer secret123");
                then.status(404).body(TI_NOT_FOUND_BODY);
            })
            .await;

        let result = client
            .task_instances()
            .succeed(&ID, &WHEN, &[], &[], None)
            .await;

        http_mock.assert_async().await;
        let result = result.unwrap_err();
        assert!(
            matches!(result, TaskInstanceApiError::NotFound(_)),
            "Expected NotFound error, got: {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_succeed_conflict() {
        let server = MockServer::start_async().await;
        let client = client(&server.base_url());

        let http_mock = server
            .mock_async(|when, then| {
                when.method("PATCH")
                    .path("/task-instances/67e55044-10b1-426f-9247-bb680e5fe0c8/state")
                    .header("authorization", "Bearer secret123");
                then.status(409).body(TI_INVALID_STATE_BODY);
            })
            .await;

        let result = client
            .task_instances()
            .succeed(&ID, &WHEN, &[], &[], None)
            .await;

        http_mock.assert_async().await;
        let result = result.unwrap_err();
        assert!(
            matches!(result, TaskInstanceApiError::Conflict(_)),
            "Expected Conflict error, got: {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_heartbeat_ok() {
        let server = MockServer::start_async().await;
        let client = client(&server.base_url());

        let http_mock = server
            .mock_async(|when, then| {
                when.method("PUT")
                    .path("/task-instances/67e55044-10b1-426f-9247-bb680e5fe0c8/heartbeat")
                    .header("authorization", "Bearer secret123")
                    .body(r#"{"hostname":"test-host","pid":1234}"#);
                then.status(204);
            })
            .await;

        let result = client.task_instances().heartbeat(&ID, HOSTNAME, PID).await;

        http_mock.assert_async().await;
        result.unwrap();
    }

    #[tokio::test]
    async fn test_heartbeat_token_refresh() {
        let server = MockServer::start_async().await;
        let client = client(&server.base_url());

        let http_mock = server
            .mock_async(|when, then| {
                when.method("PUT")
                    .path("/task-instances/67e55044-10b1-426f-9247-bb680e5fe0c8/heartbeat")
                    .header("authorization", "Bearer secret123");
                then.status(204).header("Refreshed-API-Token", "new456");
            })
            .await;

        let result = client.task_instances().heartbeat(&ID, HOSTNAME, PID).await;

        http_mock.assert_async().await;
        result.unwrap();

        let http_mock = server
            .mock_async(|when, then| {
                when.method("PUT")
                    .path("/task-instances/67e55044-10b1-426f-9247-bb680e5fe0c8/heartbeat")
                    .header("authorization", "Bearer new456");
                then.status(204);
            })
            .await;

        let result = client.task_instances().heartbeat(&ID, HOSTNAME, PID).await;

        http_mock.assert_async().await;
        result.unwrap();
    }

    #[tokio::test]
    async fn test_heartbeat_not_found() {
        let server = MockServer::start_async().await;
        let client = client(&server.base_url());

        let http_mock = server
            .mock_async(|when, then| {
                when.method("PUT")
                    .path("/task-instances/67e55044-10b1-426f-9247-bb680e5fe0c8/heartbeat")
                    .header("authorization", "Bearer secret123");
                then.status(404).body(TI_NOT_FOUND_BODY);
            })
            .await;

        let result = client.task_instances().heartbeat(&ID, HOSTNAME, PID).await;

        http_mock.assert_async().await;
        let result = result.unwrap_err();
        assert!(
            matches!(result, TaskInstanceApiError::NotFound(_)),
            "Expected NotFound error, got: {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_heartbeat_conflict() {
        let server = MockServer::start_async().await;
        let client = client(&server.base_url());

        let http_mock = server
            .mock_async(|when, then| {
                when.method("PUT")
                    .path("/task-instances/67e55044-10b1-426f-9247-bb680e5fe0c8/heartbeat")
                    .header("authorization", "Bearer secret123");
                then.status(409).body(TI_INVALID_STATE_BODY);
            })
            .await;

        let result = client.task_instances().heartbeat(&ID, HOSTNAME, PID).await;

        http_mock.assert_async().await;
        let result = result.unwrap_err();
        assert!(
            matches!(result, TaskInstanceApiError::Conflict(_)),
            "Expected Conflict error, got: {:?}",
            result
        );
    }
}
