use core::ops::Deref;

use airflow_common::{
    datetime::UtcDateTime,
    executors::UniqueTaskInstanceId,
    utils::{MapIndex, TaskInstanceState, TerminalTIStateNonSuccess},
};
use reqwest::Method;
use serde::Serialize;

use crate::api::{
    TIRunContext, TaskInstanceApiClient,
    client::TaskInstanceApiError,
    datamodels::{
        AssetProfile, InactiveAssetsResponse, PrevSuccessfulDagRunResponse, TICount,
        TaskRescheduleStartDate, TaskStatesResponse,
    },
    reqwest_impl::ReqwestExecutionApiClient,
};

#[derive(Debug, Clone)]
pub(super) struct ReqwestTaskInstanceApiClient(pub(super) ReqwestExecutionApiClient);

impl Deref for ReqwestTaskInstanceApiClient {
    type Target = ReqwestExecutionApiClient;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TaskInstanceApiClient for ReqwestTaskInstanceApiClient {
    type Error = reqwest::Error;

    async fn start(
        &self,
        id: &UniqueTaskInstanceId,
        hostname: &str,
        unixname: &str,
        pid: u32,
        when: &UtcDateTime,
    ) -> Result<TIRunContext, TaskInstanceApiError<Self::Error>> {
        let path = format!("task-instances/{id}/run");
        let body = TIEnterRunningPayload {
            state: TaskInstanceState::Running,
            hostname,
            unixname,
            pid,
            start_date: when,
        };
        let response = self
            .request(Method::PATCH, &path, Some(&body))?
            .send()
            .await?;
        let response = self.handle_response(response).await?;
        Ok(response.json().await?)
    }

    async fn finish(
        &self,
        id: &UniqueTaskInstanceId,
        state: TerminalTIStateNonSuccess,
        when: &UtcDateTime,
        rendered_map_index: Option<&str>,
    ) -> Result<(), TaskInstanceApiError<Self::Error>> {
        let path = format!("task-instances/{id}/state");
        let body = TITerminalStatePayload {
            state,
            end_date: when,
            rendered_map_index,
        };
        let response = self
            .request(Method::PATCH, &path, Some(&body))?
            .send()
            .await?;
        self.handle_response(response).await?;
        Ok(())
    }

    async fn retry(
        &self,
        _id: &UniqueTaskInstanceId,
        _when: &UtcDateTime,
        _rendered_map_index: Option<&str>,
    ) -> Result<(), TaskInstanceApiError<Self::Error>> {
        todo!()
    }

    async fn succeed(
        &self,
        id: &UniqueTaskInstanceId,
        when: &UtcDateTime,
        task_outlets: &[AssetProfile],
        outlet_events: &[()],
        rendered_map_index: Option<&str>,
    ) -> Result<(), TaskInstanceApiError<Self::Error>> {
        let path = format!("task-instances/{id}/state");
        let body = TISuccessStatePayload {
            state: TaskInstanceState::Success,
            end_date: when,
            task_outlets,
            outlet_events,
            rendered_map_index,
        };
        let response = self
            .request(Method::PATCH, &path, Some(&body))?
            .send()
            .await?;
        self.handle_response(response).await?;
        Ok(())
    }

    async fn defer<T: Serialize + Sync, N: Serialize + Sync>(
        &self,
        _id: &UniqueTaskInstanceId,
        _classpath: &str,
        _trigger_kwargs: &T,
        _trigger_timeout: u64,
        _next_method: &str,
        _next_kwargs: &N,
        _rendered_map_index: Option<&str>,
    ) -> Result<(), TaskInstanceApiError<Self::Error>> {
        todo!()
    }

    async fn reschedule(
        &self,
        _id: &UniqueTaskInstanceId,
        _reschedule_date: &UtcDateTime,
        _end_date: &UtcDateTime,
    ) -> Result<(), TaskInstanceApiError<Self::Error>> {
        todo!()
    }

    async fn heartbeat(
        &self,
        id: &UniqueTaskInstanceId,
        hostname: &str,
        pid: u32,
    ) -> Result<(), TaskInstanceApiError<Self::Error>> {
        let path = format!("task-instances/{id}/heartbeat");
        let body = TIHeartbeatInfo { hostname, pid };
        let response = self
            .request(Method::PUT, &path, Some(&body))?
            .send()
            .await?;
        let response = self.handle_response(response).await?;

        if let Some(token) = response.headers().get("Refreshed-API-Token") {
            let token = token.to_str().unwrap();
            let mut w = self.token.write().unwrap();
            *w = token.into();
            // TODO debug logging
        }
        Ok(())
    }

    async fn skip_downstream_tasks(
        &self,
        _id: &UniqueTaskInstanceId,
        _tasks: &[(String, MapIndex)],
    ) -> Result<(), TaskInstanceApiError<Self::Error>> {
        todo!()
    }

    async fn set_rtif<F: Serialize + Sync>(
        &self,
        _id: &UniqueTaskInstanceId,
        _fields: &F,
    ) -> Result<(), TaskInstanceApiError<Self::Error>> {
        todo!()
    }

    async fn get_previous_successful_dagrun(
        &self,
        _id: &UniqueTaskInstanceId,
    ) -> Result<PrevSuccessfulDagRunResponse, TaskInstanceApiError<Self::Error>> {
        todo!()
    }

    async fn get_reschedule_start_date(
        &self,
        _id: &UniqueTaskInstanceId,
        _try_number: usize,
    ) -> Result<TaskRescheduleStartDate, TaskInstanceApiError<Self::Error>> {
        todo!()
    }

    async fn get_count(
        &self,
        _dag_id: &str,
        _map_index: Option<airflow_common::utils::MapIndex>,
        _task_ids: Option<&Vec<String>>,
        _task_group_id: Option<&str>,
        _logical_dates: Option<&Vec<UtcDateTime>>,
        _run_ids: Option<&Vec<String>>,
        _states: Option<&Vec<TaskInstanceState>>,
    ) -> Result<TICount, TaskInstanceApiError<Self::Error>> {
        todo!()
    }

    async fn get_task_states(
        &self,
        _dag_id: &str,
        _map_index: Option<airflow_common::utils::MapIndex>,
        _task_ids: Option<&Vec<String>>,
        _task_group_id: Option<&str>,
        _logical_dates: Option<&Vec<UtcDateTime>>,
        _run_ids: Option<&Vec<String>>,
    ) -> Result<TaskStatesResponse, TaskInstanceApiError<Self::Error>> {
        todo!()
    }

    async fn validate_inlets_and_outlets(
        &self,
        _id: &UniqueTaskInstanceId,
    ) -> Result<InactiveAssetsResponse, TaskInstanceApiError<Self::Error>> {
        todo!()
    }
}

#[derive(Debug, Clone, Serialize)]
struct TIEnterRunningPayload<'a> {
    state: TaskInstanceState,
    hostname: &'a str,
    unixname: &'a str,
    pid: u32,
    start_date: &'a UtcDateTime,
}

#[derive(Debug, Clone, Serialize)]
struct TITerminalStatePayload<'a> {
    state: TerminalTIStateNonSuccess,
    end_date: &'a UtcDateTime,
    rendered_map_index: Option<&'a str>,
}

#[derive(Debug, Clone, Serialize)]
struct TISuccessStatePayload<'a> {
    state: TaskInstanceState, // TODO tag success
    end_date: &'a UtcDateTime,
    task_outlets: &'a [AssetProfile],
    outlet_events: &'a [()], // TODO outlet events
    rendered_map_index: Option<&'a str>,
}

#[derive(Debug, Clone, Serialize)]
struct TIHeartbeatInfo<'a> {
    hostname: &'a str,
    pid: u32,
}

#[cfg(test)]
mod tests {

    use std::sync::LazyLock;

    use airflow_common::utils::SecretString;
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
