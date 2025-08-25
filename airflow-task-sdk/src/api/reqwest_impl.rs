use airflow_common::{
    datetime::UtcDateTime,
    executors::UniqueTaskInstanceId,
    utils::{MapIndex, SecretString, TaskInstanceState, TerminalTIStateNonSuccess},
};
use log::error;
use reqwest::{Method, Response, StatusCode, header::HeaderMap};
use serde::Serialize;

use crate::api::{ExecutionApiClient, ExecutionApiClientFactory, ExecutionApiError, datamodels::*};

#[derive(Debug, Clone, Default)]
pub struct ReqwestExecutionApiClientFactory;

impl ExecutionApiClientFactory for ReqwestExecutionApiClientFactory {
    type Client = ReqwestExecutionApiClient;
    type Error = reqwest::Error;

    fn create(&self, base_url: &str, token: &SecretString) -> Result<Self::Client, Self::Error> {
        ReqwestExecutionApiClient::new(base_url, token)
    }
}

static API_VERSION: &str = "2025-08-10";

/// An ExecutionAPIClient implementation using Reqwest.
#[derive(Debug)]
pub struct ReqwestExecutionApiClient {
    client: reqwest::Client,
    base_url: String,
    token: SecretString,
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
            token: token.clone(),
        })
    }

    fn request(
        &self,
        method: Method,
        path: &str,
    ) -> Result<reqwest::RequestBuilder, reqwest::Error> {
        let builder = self
            .client
            .request(method, format!("{}/{}", self.base_url, path))
            .bearer_auth(self.token.secret());
        Ok(builder)
    }

    async fn handle_response(
        &self,
        response: Response,
    ) -> Result<Response, ExecutionApiError<reqwest::Error>> {
        match response.status() {
            StatusCode::NOT_FOUND => Err(ExecutionApiError::NotFound(response.text().await?)),
            StatusCode::CONFLICT => Err(ExecutionApiError::Conflict(response.text().await?)),
            _ => match response.error_for_status() {
                Ok(response) => Ok(response),
                Err(e) => Err(ExecutionApiError::Client(e)),
            },
        }
    }
}

impl ExecutionApiClient for ReqwestExecutionApiClient {
    type Error = reqwest::Error;

    async fn task_instances_start(
        &mut self,
        id: &UniqueTaskInstanceId,
        hostname: &str,
        unixname: &str,
        pid: u32,
        when: &UtcDateTime,
    ) -> Result<TIRunContext, ExecutionApiError<Self::Error>> {
        let path = format!("task-instances/{id}/run");
        let body = TIEnterRunningPayloadBody {
            state: TaskInstanceState::Running,
            hostname,
            unixname,
            pid,
            start_date: when,
        };
        let response = self
            .request(Method::PATCH, &path)?
            .json(&body)
            .send()
            .await?;
        let response = self.handle_response(response).await?;
        Ok(response.json().await?)
    }

    async fn task_instances_finish(
        &mut self,
        id: &UniqueTaskInstanceId,
        state: TerminalTIStateNonSuccess,
        when: &UtcDateTime,
        rendered_map_index: Option<&str>,
    ) -> Result<(), ExecutionApiError<Self::Error>> {
        let path = format!("task-instances/{id}/state");
        let body = TITerminalStatePayloadBody {
            state,
            end_date: when,
            rendered_map_index,
        };
        let response = self
            .request(Method::PATCH, &path)?
            .json(&body)
            .send()
            .await?;
        self.handle_response(response).await?;
        Ok(())
    }

    async fn task_instances_retry(
        &mut self,
        _id: &UniqueTaskInstanceId,
        _when: &UtcDateTime,
        _rendered_map_index: Option<&str>,
    ) -> Result<(), ExecutionApiError<Self::Error>> {
        todo!()
    }

    async fn task_instances_succeed(
        &mut self,
        id: &UniqueTaskInstanceId,
        when: &UtcDateTime,
        task_outlets: &[AssetProfile],
        outlet_events: &[()],
        rendered_map_index: Option<&str>,
    ) -> Result<(), ExecutionApiError<Self::Error>> {
        let path = format!("task-instances/{id}/state");
        let body = TISuccessStatePayloadBody {
            state: TaskInstanceState::Success,
            end_date: when,
            task_outlets,
            outlet_events,
            rendered_map_index,
        };
        let response = self
            .request(Method::PATCH, &path)?
            .json(&body)
            .send()
            .await?;
        self.handle_response(response).await?;
        Ok(())
    }

    async fn task_instances_defer<T: Serialize + Sync, N: Serialize + Sync>(
        &mut self,
        _id: &UniqueTaskInstanceId,
        _classpath: &str,
        _trigger_kwargs: &T,
        _trigger_timeout: u64,
        _next_method: &str,
        _next_kwargs: &N,
        _rendered_map_index: Option<&str>,
    ) -> Result<(), ExecutionApiError<Self::Error>> {
        todo!()
    }

    async fn task_instances_reschedule(
        &mut self,
        _id: &UniqueTaskInstanceId,
        _reschedule_date: &UtcDateTime,
        _end_date: &UtcDateTime,
    ) -> Result<(), ExecutionApiError<Self::Error>> {
        todo!()
    }

    async fn task_instances_heartbeat(
        &mut self,
        id: &UniqueTaskInstanceId,
        hostname: &str,
        pid: u32,
    ) -> Result<(), ExecutionApiError<Self::Error>> {
        let path = format!("task-instances/{id}/heartbeat");
        let body = TIHeartbeatInfoBody { hostname, pid };
        let response = self.request(Method::PUT, &path)?.json(&body).send().await?;
        let response = self.handle_response(response).await?;

        if let Some(token) = response.headers().get("Refreshed-API-Token") {
            let token = token.to_str().unwrap();
            self.token = token.into();
            // TODO debug logging
        }
        Ok(())
    }

    async fn task_instances_skip_downstream_tasks(
        &mut self,
        _id: &UniqueTaskInstanceId,
        _tasks: &[(String, MapIndex)],
    ) -> Result<(), ExecutionApiError<Self::Error>> {
        todo!()
    }

    async fn task_instances_set_rtif<F: Serialize + Sync>(
        &mut self,
        _id: &UniqueTaskInstanceId,
        _fields: &F,
    ) -> Result<(), ExecutionApiError<Self::Error>> {
        todo!()
    }

    async fn task_instances_get_previous_successful_dagrun(
        &mut self,
        _id: &UniqueTaskInstanceId,
    ) -> Result<PrevSuccessfulDagRunResponse, ExecutionApiError<Self::Error>> {
        todo!()
    }

    async fn task_instances_get_reschedule_start_date(
        &mut self,
        _id: &UniqueTaskInstanceId,
        _try_number: usize,
    ) -> Result<TaskRescheduleStartDate, ExecutionApiError<Self::Error>> {
        todo!()
    }

    async fn task_instances_get_count(
        &mut self,
        _dag_id: &str,
        _map_index: Option<airflow_common::utils::MapIndex>,
        _task_ids: Option<&Vec<String>>,
        _task_group_id: Option<&str>,
        _logical_dates: Option<&Vec<UtcDateTime>>,
        _run_ids: Option<&Vec<String>>,
        _states: Option<&Vec<TaskInstanceState>>,
    ) -> Result<TICount, ExecutionApiError<Self::Error>> {
        todo!()
    }

    async fn task_instances_get_task_states(
        &mut self,
        _dag_id: &str,
        _map_index: Option<airflow_common::utils::MapIndex>,
        _task_ids: Option<&Vec<String>>,
        _task_group_id: Option<&str>,
        _logical_dates: Option<&Vec<UtcDateTime>>,
        _run_ids: Option<&Vec<String>>,
    ) -> Result<TaskStatesResponse, ExecutionApiError<Self::Error>> {
        todo!()
    }

    async fn task_instances_validate_inlets_and_outlets(
        &mut self,
        _id: &UniqueTaskInstanceId,
    ) -> Result<InactiveAssetsResponse, ExecutionApiError<Self::Error>> {
        todo!()
    }

    async fn xcoms_head(
        &mut self,
        dag_id: &str,
        run_id: &str,
        task_id: &str,
        key: &str,
    ) -> Result<usize, ExecutionApiError<Self::Error>> {
        let path = format!("xcoms/{dag_id}/{run_id}/{task_id}/{key}");

        let response = self.request(Method::HEAD, &path)?.send().await?;
        let response = self.handle_response(response).await?;
        let value = match response.headers().get("Content-Range") {
            Some(value) => match value.to_str() {
                Ok(value) => {
                    if let Some(v) = value.strip_prefix("map_indexes ") {
                        v.parse::<usize>().ok()
                    } else {
                        None
                    }
                }
                Err(_) => None,
            },
            None => None,
        };

        match value {
            Some(v) => Ok(v),
            None => Err(ExecutionApiError::Other(format!(
                "Unable to parse Content-Range header from HEAD {}",
                response.url()
            ))),
        }
    }

    async fn xcoms_get(
        &mut self,
        dag_id: &str,
        run_id: &str,
        task_id: &str,
        key: &str,
        map_index: Option<MapIndex>,
        include_prior_dates: Option<bool>,
    ) -> Result<XComResponse, ExecutionApiError<Self::Error>> {
        let path = format!("xcoms/{dag_id}/{run_id}/{task_id}/{key}");
        let mut query = Vec::new();
        if let Some(map_index) = map_index {
            query.push(("map_index", map_index.to_string()));
        }
        if let Some(include_prior_dates) = include_prior_dates {
            query.push(("include_prior_dates", include_prior_dates.to_string()));
        }
        let response = self
            .request(Method::GET, &path)?
            .query(&query)
            .send()
            .await?;
        match self.handle_response(response).await {
            Ok(response) => Ok(response.json().await?),
            Err(ExecutionApiError::NotFound(detail)) => {
                error!(
                    "XCom not found. dag_id: {}, run_id: {}, task_id: {}, key: {}, map_index: {:?}, detail: {}",
                    dag_id, run_id, task_id, key, map_index, detail
                );
                Ok(XComResponse {
                    key: key.to_string(),
                    value: JsonValue::Null,
                })
            }
            Err(err) => Err(err),
        }
    }

    async fn xcoms_set(
        &mut self,
        dag_id: &str,
        run_id: &str,
        task_id: &str,
        key: &str,
        value: &JsonValue,
        map_index: Option<MapIndex>,
        mapped_length: Option<usize>,
    ) -> Result<(), ExecutionApiError<Self::Error>> {
        let path = format!("xcoms/{dag_id}/{run_id}/{task_id}/{key}");
        let mut query = Vec::new();
        if let Some(map_index) = map_index {
            query.push(("map_index", map_index.to_string()));
        }
        if let Some(mapped_length) = mapped_length {
            query.push(("mapped_length", mapped_length.to_string()));
        }
        let response = self
            .request(Method::POST, &path)?
            .json(value)
            .query(&query)
            .send()
            .await?;
        self.handle_response(response).await?;
        Ok(())
    }

    async fn xcoms_delete(
        &mut self,
        dag_id: &str,
        run_id: &str,
        task_id: &str,
        key: &str,
        map_index: Option<MapIndex>,
    ) -> Result<(), ExecutionApiError<Self::Error>> {
        let path = format!("xcoms/{dag_id}/{run_id}/{task_id}/{key}");
        let mut query = Vec::new();
        if let Some(map_index) = map_index {
            query.push(("map_index", map_index.to_string()));
        }
        let response = self
            .request(Method::DELETE, &path)?
            .query(&query)
            .send()
            .await?;
        self.handle_response(response).await?;
        Ok(())
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
    use serde_json::json;

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
        let mut client = client(&server.base_url());

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
                        "should_retry": false,
                        "xcom_keys_to_clear": ["return_value"]
                    }"#,
                );
            })
            .await;

        let result = client
            .task_instances_start(&ID, HOSTNAME, UNIXNAME, PID, &WHEN)
            .await;

        http_mock.assert_async().await;
        result.unwrap();
    }

    #[tokio::test]
    async fn test_start_not_found() {
        let server = MockServer::start_async().await;
        let mut client = client(&server.base_url());

        let http_mock = server
            .mock_async(|when, then| {
                when.method("PATCH")
                    .path("/task-instances/67e55044-10b1-426f-9247-bb680e5fe0c8/run")
                    .header("authorization", "Bearer secret123");
                then.status(404).body(TI_NOT_FOUND_BODY);
            })
            .await;

        let result = client
            .task_instances_start(&ID, HOSTNAME, UNIXNAME, PID, &WHEN)
            .await;

        http_mock.assert_async().await;
        let result = result.unwrap_err();

        assert!(
            matches!(result, ExecutionApiError::NotFound(_)),
            "Expected NotFound error, got: {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_start_conflict() {
        let server = MockServer::start_async().await;
        let mut client = client(&server.base_url());

        let http_mock = server
            .mock_async(|when, then| {
                when.method("PATCH")
                    .path("/task-instances/67e55044-10b1-426f-9247-bb680e5fe0c8/run")
                    .header("authorization", "Bearer secret123");
                then.status(409).body(TI_INVALID_STATE_BODY);
            })
            .await;

        let result = client
            .task_instances_start(&ID, HOSTNAME, UNIXNAME, PID, &WHEN)
            .await;

        http_mock.assert_async().await;
        let result = result.unwrap_err();

        assert!(
            matches!(result, ExecutionApiError::Conflict(_)),
            "Expected Conflict error, got: {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_finish_ok() {
        let server = MockServer::start_async().await;
        let mut client = client(&server.base_url());

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
            .task_instances_finish(&ID, TerminalTIStateNonSuccess::Failed, &WHEN, None)
            .await;

        http_mock.assert_async().await;
        result.unwrap();
    }

    #[tokio::test]
    async fn test_finish_not_found() {
        let server = MockServer::start_async().await;
        let mut client = client(&server.base_url());

        let http_mock = server
            .mock_async(|when, then| {
                when.method("PATCH")
                    .path("/task-instances/67e55044-10b1-426f-9247-bb680e5fe0c8/state")
                    .header("authorization", "Bearer secret123");
                then.status(404).body(TI_NOT_FOUND_BODY);
            })
            .await;

        let result = client
            .task_instances_finish(&ID, TerminalTIStateNonSuccess::Failed, &WHEN, None)
            .await;

        http_mock.assert_async().await;
        let result = result.unwrap_err();
        assert!(
            matches!(result, ExecutionApiError::NotFound(_)),
            "Expected NotFound error, got: {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_finish_conflict() {
        let server = MockServer::start_async().await;
        let mut client = client(&server.base_url());

        let http_mock = server
            .mock_async(|when, then| {
                when.method("PATCH")
                    .path("/task-instances/67e55044-10b1-426f-9247-bb680e5fe0c8/state")
                    .header("authorization", "Bearer secret123");
                then.status(409).body(TI_INVALID_STATE_BODY);
            })
            .await;

        let result = client
            .task_instances_finish(&ID, TerminalTIStateNonSuccess::Failed, &WHEN, None)
            .await;

        http_mock.assert_async().await;
        let result = result.unwrap_err();
        assert!(
            matches!(result, ExecutionApiError::Conflict(_)),
            "Expected Conflict error, got: {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_succeed_ok() {
        let server = MockServer::start_async().await;
        let mut client = client(&server.base_url());

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
            .task_instances_succeed(&ID, &WHEN, &[], &[], None)
            .await;

        http_mock.assert_async().await;
        result.unwrap();
    }

    #[tokio::test]
    async fn test_succeed_not_found() {
        let server = MockServer::start_async().await;
        let mut client = client(&server.base_url());

        let http_mock = server
            .mock_async(|when, then| {
                when.method("PATCH")
                    .path("/task-instances/67e55044-10b1-426f-9247-bb680e5fe0c8/state")
                    .header("authorization", "Bearer secret123");
                then.status(404).body(TI_NOT_FOUND_BODY);
            })
            .await;

        let result = client
            .task_instances_succeed(&ID, &WHEN, &[], &[], None)
            .await;

        http_mock.assert_async().await;
        let result = result.unwrap_err();
        assert!(
            matches!(result, ExecutionApiError::NotFound(_)),
            "Expected NotFound error, got: {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_succeed_conflict() {
        let server = MockServer::start_async().await;
        let mut client = client(&server.base_url());

        let http_mock = server
            .mock_async(|when, then| {
                when.method("PATCH")
                    .path("/task-instances/67e55044-10b1-426f-9247-bb680e5fe0c8/state")
                    .header("authorization", "Bearer secret123");
                then.status(409).body(TI_INVALID_STATE_BODY);
            })
            .await;

        let result = client
            .task_instances_succeed(&ID, &WHEN, &[], &[], None)
            .await;

        http_mock.assert_async().await;
        let result = result.unwrap_err();
        assert!(
            matches!(result, ExecutionApiError::Conflict(_)),
            "Expected Conflict error, got: {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_heartbeat_ok() {
        let server = MockServer::start_async().await;
        let mut client = client(&server.base_url());

        let http_mock = server
            .mock_async(|when, then| {
                when.method("PUT")
                    .path("/task-instances/67e55044-10b1-426f-9247-bb680e5fe0c8/heartbeat")
                    .header("authorization", "Bearer secret123")
                    .body(r#"{"hostname":"test-host","pid":1234}"#);
                then.status(204);
            })
            .await;

        let result = client.task_instances_heartbeat(&ID, HOSTNAME, PID).await;

        http_mock.assert_async().await;
        result.unwrap();
    }

    #[tokio::test]
    async fn test_heartbeat_token_refresh() {
        let server = MockServer::start_async().await;
        let mut client = client(&server.base_url());

        let http_mock = server
            .mock_async(|when, then| {
                when.method("PUT")
                    .path("/task-instances/67e55044-10b1-426f-9247-bb680e5fe0c8/heartbeat")
                    .header("authorization", "Bearer secret123");
                then.status(204).header("Refreshed-API-Token", "new456");
            })
            .await;

        let result = client.task_instances_heartbeat(&ID, HOSTNAME, PID).await;

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

        let result = client.task_instances_heartbeat(&ID, HOSTNAME, PID).await;

        http_mock.assert_async().await;
        result.unwrap();
    }

    #[tokio::test]
    async fn test_heartbeat_not_found() {
        let server = MockServer::start_async().await;
        let mut client = client(&server.base_url());

        let http_mock = server
            .mock_async(|when, then| {
                when.method("PUT")
                    .path("/task-instances/67e55044-10b1-426f-9247-bb680e5fe0c8/heartbeat")
                    .header("authorization", "Bearer secret123");
                then.status(404).body(TI_NOT_FOUND_BODY);
            })
            .await;

        let result = client.task_instances_heartbeat(&ID, HOSTNAME, PID).await;

        http_mock.assert_async().await;
        let result = result.unwrap_err();
        assert!(
            matches!(result, ExecutionApiError::NotFound(_)),
            "Expected NotFound error, got: {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_heartbeat_conflict() {
        let server = MockServer::start_async().await;
        let mut client = client(&server.base_url());

        let http_mock = server
            .mock_async(|when, then| {
                when.method("PUT")
                    .path("/task-instances/67e55044-10b1-426f-9247-bb680e5fe0c8/heartbeat")
                    .header("authorization", "Bearer secret123");
                then.status(409).body(TI_INVALID_STATE_BODY);
            })
            .await;

        let result = client.task_instances_heartbeat(&ID, HOSTNAME, PID).await;

        http_mock.assert_async().await;
        let result = result.unwrap_err();
        assert!(
            matches!(result, ExecutionApiError::Conflict(_)),
            "Expected Conflict error, got: {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_xcom_head() {
        let server = MockServer::start_async().await;
        let mut client = client(&server.base_url());

        let http_mock = server
            .mock_async(|when, then| {
                when.method("HEAD")
                    .path("/xcoms/example_dag/example_run/example_task/example_key")
                    .header("authorization", "Bearer secret123");
                then.status(200).header("Content-Range", "map_indexes 42");
            })
            .await;

        let result = client
            .xcoms_head("example_dag", "example_run", "example_task", "example_key")
            .await;

        http_mock.assert_async().await;
        let result = result.unwrap();
        assert_eq!(result, 42);
    }

    #[tokio::test]
    async fn test_xcom_get() {
        let server = MockServer::start_async().await;
        let mut client = client(&server.base_url());

        let http_mock = server
            .mock_async(|when, then| {
                when.method("GET")
                    .path("/xcoms/example_dag/example_run/example_task/example_key")
                    .header("authorization", "Bearer secret123");
                then.status(200).body(
                    r#"{
                        "key": "example_key",
                        "value": {"hello": "world"}
                    }"#,
                );
            })
            .await;

        let result = client
            .xcoms_get(
                "example_dag",
                "example_run",
                "example_task",
                "example_key",
                None,
                None,
            )
            .await;

        http_mock.assert_async().await;
        let result = result.unwrap();

        assert_eq!(result.key, "example_key");
        assert_eq!(result.value, json!({"hello": "world"}));
    }

    #[tokio::test]
    async fn test_xcom_get_404() {
        let server = MockServer::start_async().await;
        let mut client = client(&server.base_url());

        let http_mock = server
            .mock_async(|when, then| {
                when.method("GET")
                    .path("/xcoms/example_dag/example_run/example_task/example_key")
                    .header("authorization", "Bearer secret123");
                then.status(404).body("Not Found");
            })
            .await;

        let result = client
            .xcoms_get(
                "example_dag",
                "example_run",
                "example_task",
                "example_key",
                None,
                None,
            )
            .await;

        http_mock.assert_async().await;
        let result = result.unwrap();

        assert_eq!(result.key, "example_key");
        assert_eq!(result.value, json!(null));
    }

    #[tokio::test]
    async fn test_xcom_set() {
        let server = MockServer::start_async().await;
        let mut client = client(&server.base_url());

        let http_mock = server
            .mock_async(|when, then| {
                when.method("POST")
                    .path("/xcoms/example_dag/example_run/example_task/example_key")
                    .header("authorization", "Bearer secret123")
                    .query_param("map_index", "-1")
                    .body(r#"{"hello":"world"}"#);
                then.status(200);
            })
            .await;

        let result = client
            .xcoms_set(
                "example_dag",
                "example_run",
                "example_task",
                "example_key",
                &json!({"hello": "world"}),
                Some(MapIndex::none()),
                None,
            )
            .await;

        http_mock.assert_async().await;
        result.unwrap();
    }

    #[tokio::test]
    async fn test_xcom_delete() {
        let server = MockServer::start_async().await;
        let mut client = client(&server.base_url());

        let http_mock = server
            .mock_async(|when, then| {
                when.method("DELETE")
                    .path("/xcoms/example_dag/example_run/example_task/example_key")
                    .header("authorization", "Bearer secret123")
                    .query_param("map_index", "-1");
                then.status(200);
            })
            .await;

        let result = client
            .xcoms_delete(
                "example_dag",
                "example_run",
                "example_task",
                "example_key",
                Some(MapIndex::none()),
            )
            .await;

        http_mock.assert_async().await;
        result.unwrap();
    }
}
