use airflow_common::{datetime::StdTimeProvider, utils::SecretString};
use airflow_edge_sdk::{
    api::EdgeApiClient,
    worker::{EdgeWorker, Intercom, IntercomMessage, LocalWorkerRuntime},
};
use airflow_task_sdk::api::ReqwestExecutionApiClientFactory;
use examples::{
    StdEdgeApiClient, StdJWTGenerator,
    example::get_dag_bag,
    tokio::{TokioIntercom, TokioRuntime},
    tracing::{
        LogEvent, NonTaskContextFilter, TaskContextFilter, TaskInstanceKeyLayer, TaskLogLayer,
    },
};
use tokio::signal::unix::{SignalKind, signal};
use tracing::{debug, error, level_filters::LevelFilter};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

static API_AUTH_JWT_SECRET: &str = "rEVfqc5f8WQDLCJcrKKKtQ==";

static EDGE_API_URL: &str = "http://localhost:28080/edge_worker/v1";

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    let (send, recv) = tokio::sync::mpsc::unbounded_channel();

    let log = fmt::subscriber()
        .with_filter(NonTaskContextFilter)
        .with_filter(EnvFilter::from_default_env());

    let task_log = TaskLogLayer::new(send.clone(), StdTimeProvider)
        .and_then(TaskInstanceKeyLayer)
        .with_filter(TaskContextFilter)
        .with_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .with_env_var("AIRFLOW_TASK_LOG")
                .from_env_lossy(),
        );
    // TODO env filter for task log

    tracing_subscriber::registry()
        .with(log)
        .with(task_log)
        .init();

    let runtime = TokioRuntime::default().with_concurrency(2);
    let time_provider = StdTimeProvider;
    let key = SecretString::from(API_AUTH_JWT_SECRET);
    let jwt_generator = StdJWTGenerator::new(key, "api", time_provider);
    let client = StdEdgeApiClient::new(EDGE_API_URL, jwt_generator)
        .expect("Failed to build StdEdgeApiClient");
    let log_client = client.clone();

    let handle = tokio::spawn(async move {
        let mut log_client = log_client;
        let mut recv = recv;
        while let Some(event) = recv.recv().await {
            match event {
                LogEvent::Message(key, ts, msg) => {
                    match log_client.logs_push(&key, &ts, &msg).await {
                        Ok(_) => {
                            debug!("Log sent successfully");
                        }
                        Err(e) => {
                            error!("Failed to send log: {:?}", e);
                        }
                    };
                }
                LogEvent::Exit => {
                    debug!("Log exit");
                    break;
                }
            }
        }
    });

    let dag_bag = get_dag_bag();

    register_signal(runtime.intercom()).expect("Failed to register signal handler");
    let worker = EdgeWorker::<
        StdEdgeApiClient,
        StdTimeProvider,
        TokioRuntime<ReqwestExecutionApiClientFactory>,
    >::new(client, time_provider, runtime, dag_bag);

    match worker.start().await {
        Ok(_) => {}
        Err(e) => error!("Worker terminated with error: {:?}", e),
    }

    send.send(LogEvent::Exit).expect("Failed to send exit log");
    // TODO close sender on shutdown
    // handle.abort();
    handle.await.unwrap();
}

fn register_signal(intercom: TokioIntercom) -> std::io::Result<()> {
    let mut stream = signal(SignalKind::interrupt())?;
    let mut first = true;
    tokio::spawn(async move {
        loop {
            match stream.recv().await {
                Some(_) => {
                    debug!("Received interrupt signal");
                    let msg = match first {
                        true => {
                            first = false;
                            IntercomMessage::Shutdown
                        }
                        false => IntercomMessage::Terminate,
                    };
                    debug!("Sending {:?}", msg);
                    match intercom.send(msg).await {
                        Ok(_) => {}
                        Err(e) => {
                            debug!("Failed to send message: {:?}", e);
                            return;
                        }
                    };
                }
                None => {
                    debug!("Signal stream closed, terminating worker");
                    intercom.send(IntercomMessage::Terminate).await.ok();
                    return;
                }
            }
        }
    });
    Ok(())
}
