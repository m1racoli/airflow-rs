cfg_if::cfg_if! {
    if #[cfg(feature = "std")] {
        use std::error;
    } else {
        extern crate alloc;
        use alloc::string::String;
        use alloc::string::ToString;
        use alloc::vec::Vec;
        use core::error;
    }
}

use core::fmt::Debug;

use crate::{
    api::EdgeApiError,
    models::{EdgeWorkerState, SysInfo},
};
use airflow_common::{api::JWTGenerator, datetime::UtcDateTime};
use reqwest::{Client, Method, Response, StatusCode, header::HeaderMap};
use serde::{Serialize, de::DeserializeOwned};

use super::EdgeApiClient;

#[derive(thiserror::Error, Debug)]
pub enum ReqwestEdgeApiError<J: error::Error> {
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
    #[error(transparent)]
    JWT(J),
}

/// Client for the Edge API using Reqwest.
#[derive(Debug, Clone)]
pub struct ReqwestEdgeApiClient<J: JWTGenerator> {
    client: Client,
    base_url: String,
    jwt_generator: J,
}

impl<J: JWTGenerator> ReqwestEdgeApiClient<J> {
    pub fn new(base_url: &str, jwt_generator: J) -> Result<Self, reqwest::Error> {
        let mut headers = HeaderMap::new();
        headers.insert("accept", "application/json".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());

        // TODO make user agent configurable
        let client = Client::builder()
            .default_headers(headers)
            .user_agent("airflow-rs-edge-sdk")
            .build()?;

        Ok(Self {
            client,
            base_url: base_url.to_string(),
            jwt_generator,
        })
    }

    fn token(&self, path: &str) -> Result<String, ReqwestEdgeApiError<J::Error>> {
        self.jwt_generator
            .generate(path)
            .map_err(ReqwestEdgeApiError::JWT)
    }

    async fn request<T: Serialize + ?Sized>(
        &self,
        method: Method,
        path: &str,
        json: Option<&T>,
    ) -> Result<reqwest::Response, EdgeApiError<ReqwestEdgeApiError<J::Error>>> {
        let token = self.token(path)?;
        let builder = self
            .client
            .request(method, format!("{}/{}", self.base_url, path))
            .header("authorization", token);
        let builder = if let Some(json) = json {
            builder.json(json)
        } else {
            builder
        };

        let response = builder.send().await.map_err(ReqwestEdgeApiError::Reqwest)?;
        match response.status() {
            StatusCode::NOT_FOUND => Err(EdgeApiError::EdgeNotEnabled),
            StatusCode::BAD_REQUEST => {
                let body = response
                    .text()
                    .await
                    .map_err(ReqwestEdgeApiError::Reqwest)?;
                Err(EdgeApiError::VersionMismatch(body))
            }
            _ => match response.error_for_status() {
                Ok(response) => Ok(response),
                Err(e) => Err(ReqwestEdgeApiError::Reqwest(e))?,
            },
        }
    }

    async fn deserialize<T: DeserializeOwned>(
        &self,
        response: Response,
    ) -> Result<T, ReqwestEdgeApiError<J::Error>> {
        Ok(response.json().await?)
    }
}

impl<J: JWTGenerator + Sync + Send> EdgeApiClient for ReqwestEdgeApiClient<J> {
    type Error = ReqwestEdgeApiError<J::Error>;

    async fn health(&mut self) -> Result<super::HealthReturn, EdgeApiError<Self::Error>> {
        let path = "health";
        let response = self.request::<()>(Method::GET, path, None).await?;
        Ok(self.deserialize(response).await?)
    }

    async fn worker_register(
        &mut self,
        hostname: &str,
        state: EdgeWorkerState,
        queues: Option<&Vec<String>>,
        sysinfo: &SysInfo,
    ) -> Result<super::WorkerRegistrationReturn, EdgeApiError<Self::Error>> {
        let path = format!("worker/{hostname}");
        let body = WorkerStateBody {
            state,
            jobs_active: 0,
            queues,
            sysinfo,
            maintenance_comments: None,
        };
        let response = self.request(Method::POST, &path, Some(&body)).await?;
        Ok(self.deserialize(response).await?)
    }

    async fn worker_set_state(
        &mut self,
        hostname: &str,
        state: EdgeWorkerState,
        jobs_active: usize,
        queues: Option<&Vec<String>>,
        sysinfo: &SysInfo,
        maintenance_comments: Option<&str>,
    ) -> Result<super::WorkerSetStateReturn, EdgeApiError<Self::Error>> {
        let path = format!("worker/{hostname}");
        let body = WorkerStateBody {
            state,
            jobs_active,
            queues,
            sysinfo,
            maintenance_comments,
        };
        let response = self.request(Method::PATCH, &path, Some(&body)).await?;
        Ok(self.deserialize(response).await?)
    }

    async fn jobs_fetch(
        &mut self,
        hostname: &str,
        queues: Option<&Vec<String>>,
        free_concurrency: usize,
    ) -> Result<Option<super::EdgeJobFetched>, EdgeApiError<Self::Error>> {
        let path = format!("jobs/fetch/{hostname}");
        let body = WorkerQueuesBody {
            queues,
            free_concurrency,
        };
        let response = self.request(Method::POST, &path, Some(&body)).await?;
        Ok(self.deserialize(response).await?)
    }

    async fn jobs_set_state(
        &mut self,
        key: &airflow_common::models::TaskInstanceKey,
        state: airflow_common::utils::TaskInstanceState,
    ) -> Result<(), EdgeApiError<Self::Error>> {
        let path = format!(
            "jobs/state/{}/{}/{}/{}/{}/{}",
            key.dag_id(),
            key.task_id(),
            key.run_id(),
            key.try_number(),
            key.map_index(),
            state
        );
        self.request::<()>(Method::PATCH, &path, None).await?;
        Ok(())
    }

    async fn logs_logfile_path(
        &mut self,
        key: &airflow_common::models::TaskInstanceKey,
    ) -> Result<String, EdgeApiError<Self::Error>> {
        let path = format!(
            "logs/logfile_path/{}/{}/{}/{}/{}",
            key.dag_id(),
            key.task_id(),
            key.run_id(),
            key.try_number(),
            key.map_index(),
        );

        let response = self.request::<()>(Method::GET, &path, None).await?;
        Ok(self.deserialize(response).await?)
    }

    async fn logs_push(
        &mut self,
        key: &airflow_common::models::TaskInstanceKey,
        log_chunk_time: &UtcDateTime,
        log_chunk_data: &str,
    ) -> Result<(), EdgeApiError<Self::Error>> {
        let path = format!(
            "logs/push/{}/{}/{}/{}/{}",
            key.dag_id(),
            key.task_id(),
            key.run_id(),
            key.try_number(),
            key.map_index(),
        );
        let body = PushLogsBody {
            log_chunk_time,
            log_chunk_data,
        };
        self.request(Method::POST, &path, Some(&body)).await?;
        Ok(())
    }
}

/// Incremental new log content from worker.
#[derive(Debug, Serialize)]
struct PushLogsBody<'a> {
    /// Time of the log chunk at point of sending.
    log_chunk_time: &'a UtcDateTime,
    /// Log chunk data as incremental log text.
    log_chunk_data: &'a str,
}

/// Queues that a worker supports to run jobs on.
#[derive(Debug, Serialize)]
struct WorkerQueuesBody<'a> {
    /// List of queues the worker is pulling jobs from. If not provided, worker pulls from all queues.
    queues: Option<&'a Vec<String>>,
    /// Number of free concurrency slots on the worker.
    free_concurrency: usize,
}

/// Details of the worker state sent to the scheduler.
#[derive(Debug, Serialize)]
struct WorkerStateBody<'a> {
    /// State of the worker from the view of the worker.
    state: EdgeWorkerState,
    /// Number of active jobs the worker is running.
    jobs_active: usize,
    /// List of queues the worker is pulling jobs from. If not provided, worker pulls from all queues.
    queues: Option<&'a Vec<String>>,
    /// System information of the worker.
    sysinfo: &'a SysInfo,
    /// Comments about the maintenance state of the worker.
    maintenance_comments: Option<&'a str>,
}

#[cfg(test)]
mod tests {
    use airflow_common::{
        api::MockJWTGenerator,
        models::TaskInstanceKey,
        utils::{MapIndex, TaskInstanceState},
    };
    use httpmock::MockServer;

    use super::*;

    fn mock_jwt_generator() -> MockJWTGenerator {
        MockJWTGenerator::new("secret123")
    }

    fn sys_info() -> SysInfo {
        SysInfo {
            airflow_version: "3.0.0".to_string(),
            edge_provider_version: "1.0.0".to_string(),
            concurrency: 1,
            free_concurrency: 1,
        }
    }

    fn ti_key() -> TaskInstanceKey {
        TaskInstanceKey::new("dag_id", "task_id", "run_id", 1, None.into())
    }

    fn datetime() -> UtcDateTime {
        // 2019-10-12T07:20:50.52Z
        UtcDateTime::from_timestamp(1570864850, 0).unwrap()
    }

    fn queues() -> Vec<String> {
        vec!["queue1".to_string(), "queue2".to_string()]
    }

    #[tokio::test]
    async fn test_health() {
        let server = MockServer::start_async().await;
        let mut client =
            ReqwestEdgeApiClient::new(&server.base_url(), mock_jwt_generator()).unwrap();

        let http_mock = server
            .mock_async(|when, then| {
                when.method("GET")
                    .path("/health")
                    .header("authorization", "health:secret123");
                then.status(200).body(r#"{"status": "healthy"}"#);
            })
            .await;

        let result = client.health().await;

        http_mock.assert_async().await;

        let result = result.unwrap();
        assert_eq!(result.status, "healthy");
    }

    #[tokio::test]
    async fn test_worker_register() {
        let server = MockServer::start_async().await;
        let mut client =
            ReqwestEdgeApiClient::new(&server.base_url(), mock_jwt_generator()).unwrap();

        let http_mock = server
            .mock_async(|when, then| {
                when.method("POST")
                    .path("/worker/hello")
                    .header("authorization", "worker/hello:secret123")
                    .body(r#"{"state":"starting","jobs_active":0,"queues":null,"sysinfo":{"airflow_version":"3.0.0","edge_provider_version":"1.0.0","concurrency":1,"free_concurrency":1},"maintenance_comments":null}"#                    );
                then.status(200)
                    .body(r#"{"last_update": "2019-10-12T07:20:50Z"}"#);
            })
            .await;

        let result = client
            .worker_register("hello", EdgeWorkerState::Starting, None, &sys_info())
            .await;

        http_mock.assert_async().await;

        let result = result.unwrap();
        assert_eq!(result.last_update, datetime());
    }

    #[tokio::test]
    async fn test_worker_set_state() {
        let server = MockServer::start_async().await;
        let mut client =
            ReqwestEdgeApiClient::new(&server.base_url(), mock_jwt_generator()).unwrap();

        let http_mock = server
            .mock_async(|when, then| {
                when.method("PATCH")
                    .path("/worker/hello")
                    .header("authorization", "worker/hello:secret123")
                    .body(r#"{"state":"running","jobs_active":1,"queues":["queue1","queue2"],"sysinfo":{"airflow_version":"3.0.0","edge_provider_version":"1.0.0","concurrency":1,"free_concurrency":1},"maintenance_comments":"test"}"#                    );
                then.status(200)
                    .body(r#"{"state": "starting", "queues":["queue1","queue2"], "maintenance_comments":"test"}"#);
            })
            .await;

        let result = client
            .worker_set_state(
                "hello",
                EdgeWorkerState::Running,
                1,
                Some(&queues()),
                &sys_info(),
                Some("test"),
            )
            .await;

        http_mock.assert_async().await;

        let result = result.unwrap();
        assert_eq!(result.state, EdgeWorkerState::Starting);
        assert_eq!(result.queues, Some(queues()));
        assert_eq!(result.maintenance_comments, Some("test".to_string()));
    }

    #[tokio::test]
    async fn test_jobs_fetch() {
        let server = MockServer::start_async().await;
        let mut client =
            ReqwestEdgeApiClient::new(&server.base_url(), mock_jwt_generator()).unwrap();

        let http_mock = server
            .mock_async(|when, then| {
                when.method("POST")
                    .path("/jobs/fetch/hello")
                    .header("authorization", "jobs/fetch/hello:secret123")
                    .body(r#"{"queues":["queue1","queue2"],"free_concurrency":1}"#);
                then.status(200).body(
                    r#"{
                        "dag_id":"mydag",
                        "task_id":"mytask",
                        "run_id":"myrun",
                        "map_index":3,
                        "try_number":2,
                        "concurrency_slots":4,
                        "command":{
                            "token": "mysecrettoken",
                            "ti":{
                                "id": "cd39d984-0c40-4938-9e74-c240c48a76e4",
                                "dag_id":"mydag",
                                "task_id":"mytask",
                                "run_id":"myrun",
                                "try_number":2,
                                "map_index":3,
                                "pool_slots":7,
                                "queue":"myqueue",
                                "priority_weight":2
                            },
                            "dag_rel_path":"dagfilepath",
                            "bundle_info":{"name":"mybundle","version":"v123"},
                            "log_path":"logfilepath",
                            "type":"ExecuteTask"
                        }
                    }"#,
                );
            })
            .await;

        let result = client.jobs_fetch("hello", Some(&queues()), 1).await;

        http_mock.assert_async().await;

        let result = result.unwrap();
        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(result.dag_id, "mydag");
        assert_eq!(result.task_id, "mytask");
        assert_eq!(result.run_id, "myrun");
        assert_eq!(result.map_index, MapIndex::some(3));
        assert_eq!(result.try_number, 2);
        assert_eq!(result.concurrency_slots, 4);
    }

    #[tokio::test]
    async fn test_jobs_set_state() {
        let server = MockServer::start_async().await;
        let mut client =
            ReqwestEdgeApiClient::new(&server.base_url(), mock_jwt_generator()).unwrap();

        let http_mock = server
            .mock_async(|when, then| {
                when.method("PATCH")
                    .path("/jobs/state/dag_id/task_id/run_id/1/-1/failed")
                    .header(
                        "authorization",
                        "jobs/state/dag_id/task_id/run_id/1/-1/failed:secret123",
                    );
                then.status(200);
            })
            .await;

        let result = client
            .jobs_set_state(&ti_key(), TaskInstanceState::Failed)
            .await;

        http_mock.assert_async().await;

        result.unwrap();
    }

    #[tokio::test]
    async fn test_logs_logfile_path() {
        let server = MockServer::start_async().await;
        let mut client =
            ReqwestEdgeApiClient::new(&server.base_url(), mock_jwt_generator()).unwrap();

        let http_mock = server
            .mock_async(|when, then| {
                when.method("GET")
                    .path("/logs/logfile_path/dag_id/task_id/run_id/1/-1")
                    .header(
                        "authorization",
                        "logs/logfile_path/dag_id/task_id/run_id/1/-1:secret123",
                    );
                then.status(200).body(r#""mylogpath""#);
            })
            .await;

        let result = client.logs_logfile_path(&ti_key()).await;

        http_mock.assert_async().await;

        let result = result.unwrap();
        assert_eq!(result, "mylogpath");
    }

    #[tokio::test]
    async fn test_logs_push() {
        let server = MockServer::start_async().await;
        let mut client =
            ReqwestEdgeApiClient::new(&server.base_url(), mock_jwt_generator()).unwrap();

        let http_mock = server
            .mock_async(|when, then| {
                when.method("POST")
                    .path("/logs/push/dag_id/task_id/run_id/1/-1")
                    .header("authorization", "logs/push/dag_id/task_id/run_id/1/-1:secret123")
                    .body(r#"{"log_chunk_time":"2019-10-12T07:20:50Z","log_chunk_data":"Hello world!"}"#);
                then.status(200);
            })
            .await;

        let result = client
            .logs_push(&ti_key(), &datetime(), "Hello world!")
            .await;

        http_mock.assert_async().await;

        result.unwrap();
    }

    #[tokio::test]
    async fn test_not_found() {
        let server = MockServer::start_async().await;
        let mut client =
            ReqwestEdgeApiClient::new(&server.base_url(), mock_jwt_generator()).unwrap();

        let http_mock = server
            .mock_async(|when, then| {
                when.method("POST").path("/worker/hello");
                then.status(404);
            })
            .await;

        let result = client
            .worker_register("hello", EdgeWorkerState::Running, None, &sys_info())
            .await;

        http_mock.assert_async().await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), EdgeApiError::EdgeNotEnabled));
    }

    #[tokio::test]
    async fn test_bad_request() {
        let server = MockServer::start_async().await;
        let mut client =
            ReqwestEdgeApiClient::new(&server.base_url(), mock_jwt_generator()).unwrap();

        let http_mock = server
            .mock_async(|when, then| {
                when.method("POST").path("/worker/hello");
                then.status(400).body("Wrong version!");
            })
            .await;

        let result = client
            .worker_register("hello", EdgeWorkerState::Running, None, &sys_info())
            .await;

        http_mock.assert_async().await;

        assert!(result.is_err());
        match result {
            Err(EdgeApiError::VersionMismatch(msg)) => {
                assert_eq!(msg, "Wrong version!");
            }
            _ => panic!("Expected VersionMismatch error"),
        }
    }

    #[tokio::test]
    async fn test_http_error() {
        let server = MockServer::start_async().await;
        let mut client =
            ReqwestEdgeApiClient::new(&server.base_url(), mock_jwt_generator()).unwrap();

        let http_mock = server
            .mock_async(|when, then| {
                when.method("POST").path("/worker/hello");
                then.status(403).body("Not authorized!");
            })
            .await;

        let result = client
            .worker_register("hello", EdgeWorkerState::Running, None, &sys_info())
            .await;

        http_mock.assert_async().await;

        assert!(result.is_err());
        match result {
            Err(EdgeApiError::Other(ReqwestEdgeApiError::Reqwest(e))) => {
                assert_eq!(e.status(), Some(StatusCode::FORBIDDEN));
            }
            _ => panic!("Expected request error"),
        }
    }
}
