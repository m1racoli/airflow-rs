cfg_if::cfg_if! {
    if #[cfg(feature = "std")] {
        use std::fmt;
    } else {
        use core::fmt;
    }
}

use serde::{Deserialize, Serialize};

/// All possible states that a Task Instance can be in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskInstanceState {
    /// Task vanished from DAG before it ran
    Removed,
    /// Task should run and will be handed to executor soon
    Scheduled,
    /// Executor has enqueued the task
    Queued,
    /// Task is executing
    Running,
    /// Task completed
    Success,
    /// External request to restart (e.g. cleared when running)
    Restarting,
    /// Task errored out
    Failed,
    /// Task failed but has retries left
    UpForRetry,
    /// A waiting `reschedule` sensor
    UpForReschedule,
    /// One or more upstream deps failed
    UpstreamFailed,
    /// Skipped by branching or some other mechanism
    Skipped,
    /// Deferrable operator waiting on a trigger
    Deferred,
}

impl TaskInstanceState {
    /// Returns true if the task instance is in a terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TaskInstanceState::Success
                | TaskInstanceState::Failed
                | TaskInstanceState::Skipped
                | TaskInstanceState::Removed
        )
    }

    /// Returns true if the task instance is not yet in a terminal or running state.
    pub fn is_intermediate(&self) -> bool {
        matches!(
            self,
            TaskInstanceState::Scheduled
                | TaskInstanceState::Queued
                | TaskInstanceState::Restarting
                | TaskInstanceState::UpForRetry
                | TaskInstanceState::UpForReschedule
                | TaskInstanceState::UpstreamFailed
                | TaskInstanceState::Deferred
        )
    }
}

impl fmt::Display for TaskInstanceState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TaskInstanceState::Removed => write!(f, "removed"),
            TaskInstanceState::Scheduled => write!(f, "scheduled"),
            TaskInstanceState::Queued => write!(f, "queued"),
            TaskInstanceState::Running => write!(f, "running"),
            TaskInstanceState::Success => write!(f, "success"),
            TaskInstanceState::Restarting => write!(f, "restarting"),
            TaskInstanceState::Failed => write!(f, "failed"),
            TaskInstanceState::UpForRetry => write!(f, "up_for_retry"),
            TaskInstanceState::UpForReschedule => write!(f, "up_for_reschedule"),
            TaskInstanceState::UpstreamFailed => write!(f, "upstream_failed"),
            TaskInstanceState::Skipped => write!(f, "skipped"),
            TaskInstanceState::Deferred => write!(f, "deferred"),
        }
    }
}

/// States that a Task Instance can be in that indicate it is not yet in a terminal or running state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntermediateTIState {
    /// Task should run and will be handed to executor soon
    Scheduled,
    /// Executor has enqueued the task
    Queued,
    /// External request to restart (e.g. cleared when running)
    Restarting,
    /// Task failed but has retries left
    UpForRetry,
    /// A waiting `reschedule` sensor
    UpForReschedule,
    /// One or more upstream deps failed
    UpstreamFailed,
    /// Deferrable operator waiting on a trigger
    Deferred,
}

impl From<IntermediateTIState> for TaskInstanceState {
    fn from(state: IntermediateTIState) -> Self {
        match state {
            IntermediateTIState::Scheduled => TaskInstanceState::Scheduled,
            IntermediateTIState::Queued => TaskInstanceState::Queued,
            IntermediateTIState::Restarting => TaskInstanceState::Restarting,
            IntermediateTIState::UpForRetry => TaskInstanceState::UpForRetry,
            IntermediateTIState::UpForReschedule => TaskInstanceState::UpForReschedule,
            IntermediateTIState::UpstreamFailed => TaskInstanceState::UpstreamFailed,
            IntermediateTIState::Deferred => TaskInstanceState::Deferred,
        }
    }
}

impl From<TaskInstanceState> for Option<IntermediateTIState> {
    fn from(state: TaskInstanceState) -> Self {
        match state {
            TaskInstanceState::Scheduled => Some(IntermediateTIState::Scheduled),
            TaskInstanceState::Queued => Some(IntermediateTIState::Queued),
            TaskInstanceState::Restarting => Some(IntermediateTIState::Restarting),
            TaskInstanceState::UpForRetry => Some(IntermediateTIState::UpForRetry),
            TaskInstanceState::UpForReschedule => Some(IntermediateTIState::UpForReschedule),
            TaskInstanceState::UpstreamFailed => Some(IntermediateTIState::UpstreamFailed),
            TaskInstanceState::Deferred => Some(IntermediateTIState::Deferred),
            _ => None,
        }
    }
}

impl fmt::Display for IntermediateTIState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        TaskInstanceState::from(*self).fmt(f)
    }
}

/// States that a Task Instance can be in that indicate it has reached a terminal state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalTIState {
    /// Task completed
    Success,
    /// Task errored out
    Failed,
    /// Skipped by branching or some other mechanism
    Skipped,
    /// Task vanished from DAG before it ran
    Removed,
}

impl From<TerminalTIState> for TaskInstanceState {
    fn from(state: TerminalTIState) -> Self {
        match state {
            TerminalTIState::Success => TaskInstanceState::Success,
            TerminalTIState::Failed => TaskInstanceState::Failed,
            TerminalTIState::Skipped => TaskInstanceState::Skipped,
            TerminalTIState::Removed => TaskInstanceState::Removed,
        }
    }
}

impl From<TaskInstanceState> for Option<TerminalTIState> {
    fn from(state: TaskInstanceState) -> Self {
        match state {
            TaskInstanceState::Success => Some(TerminalTIState::Success),
            TaskInstanceState::Failed => Some(TerminalTIState::Failed),
            TaskInstanceState::Skipped => Some(TerminalTIState::Skipped),
            TaskInstanceState::Removed => Some(TerminalTIState::Removed),
            _ => None,
        }
    }
}

impl fmt::Display for TerminalTIState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        TaskInstanceState::from(*self).fmt(f)
    }
}

/// Non-success states that a Task Instance can be in that indicate it has reached a terminal state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalTIStateNonSuccess {
    /// Task vanished from DAG before it ran
    Removed,
    /// Task errored out
    Failed,
    /// Skipped by branching or some other mechanism
    Skipped,
}

impl From<TerminalTIStateNonSuccess> for TaskInstanceState {
    fn from(state: TerminalTIStateNonSuccess) -> Self {
        match state {
            TerminalTIStateNonSuccess::Removed => TaskInstanceState::Removed,
            TerminalTIStateNonSuccess::Failed => TaskInstanceState::Failed,
            TerminalTIStateNonSuccess::Skipped => TaskInstanceState::Skipped,
        }
    }
}

impl From<TerminalTIStateNonSuccess> for TerminalTIState {
    fn from(state: TerminalTIStateNonSuccess) -> Self {
        match state {
            TerminalTIStateNonSuccess::Removed => TerminalTIState::Removed,
            TerminalTIStateNonSuccess::Failed => TerminalTIState::Failed,
            TerminalTIStateNonSuccess::Skipped => TerminalTIState::Skipped,
        }
    }
}

impl From<TaskInstanceState> for Option<TerminalTIStateNonSuccess> {
    fn from(state: TaskInstanceState) -> Self {
        match state {
            TaskInstanceState::Removed => Some(TerminalTIStateNonSuccess::Removed),
            TaskInstanceState::Failed => Some(TerminalTIStateNonSuccess::Failed),
            TaskInstanceState::Skipped => Some(TerminalTIStateNonSuccess::Skipped),
            _ => None,
        }
    }
}

impl From<TerminalTIState> for Option<TerminalTIStateNonSuccess> {
    fn from(state: TerminalTIState) -> Self {
        match state {
            TerminalTIState::Removed => Some(TerminalTIStateNonSuccess::Removed),
            TerminalTIState::Failed => Some(TerminalTIStateNonSuccess::Failed),
            TerminalTIState::Skipped => Some(TerminalTIStateNonSuccess::Skipped),
            _ => None,
        }
    }
}

impl fmt::Display for TerminalTIStateNonSuccess {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        TaskInstanceState::from(*self).fmt(f)
    }
}
