use airflow_common::{
    executors::{BundleInfo, TaskInstance},
    utils::SecretString,
};

cfg_if::cfg_if! {
    if #[cfg(feature = "std")] {
        use std::error;
        use std::future;
    } else {
        use core::error;
        use core::future;
    }
}

/// This is the boundary between worker and task execution.
pub trait Supervisor {
    type Error: error::Error;

    /// Run a single task execution to completion.
    ///
    /// # Arguments
    ///
    /// * `ti` - The task instance to run.
    /// * `bundle_info` - Current DAG bundle of the task instance.
    /// * `dag_rel_path` - The file path to the DAG.
    /// * `token` - Authentication token for the API client.
    /// * `log_path` - Path to write logs, if required.
    fn supervise(
        &self,
        ti: &TaskInstance,
        bundle_info: &BundleInfo,
        dag_rel_path: &str,
        token: &SecretString,
        log_path: Option<&str>,
    ) -> impl future::Future<Output = Result<bool, Self::Error>> + Send;
}
