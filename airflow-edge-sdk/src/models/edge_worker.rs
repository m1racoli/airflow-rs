use serde::{Deserialize, Serialize};

/// Status of a Edge Worker instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Copy)]
#[serde(rename_all = "snake_case")]
pub enum EdgeWorkerState {
    /// Edge Worker is in initialization.
    Starting,
    /// Edge Worker is actively running a task.
    Running,
    /// Edge Worker is active and waiting for a task.
    Idle,
    /// Edge Worker is completing work and stopping.
    Terminating,
    /// Edge Worker was shut down.
    Offline,
    /// No heartbeat signal from worker for some time, Edge Worker probably down.
    Unknown,
    /// Worker was requested to enter maintenance mode. Once worker receives this it will pause fetching jobs.
    MaintenanceRequest,
    /// Edge worker received the request for maintenance, waiting for jobs to finish. Once jobs are finished will move to 'maintenance mode'
    MaintenancePending,
    /// Edge worker is in maintenance mode. It is online but pauses fetching jobs.
    MaintenanceMode,
    /// Request worker to exit maintenance mode. Once the worker receives this state it will un-pause and fetch new jobs.
    MaintenanceExit,
    /// Worker was shut down in maintenance mode. It will be in maintenance mode when restarted.
    OfflineMaintenance,
}

/// Produce the sysinfo from worker to post to central site.
#[derive(Debug, Serialize, Clone)]
pub struct SysInfo<'a> {
    pub airflow_version: &'a str,
    pub edge_provider_version: &'a str,
    pub concurrency: usize,
    /// Number of free slots for running tasks.
    pub free_concurrency: usize,
}
