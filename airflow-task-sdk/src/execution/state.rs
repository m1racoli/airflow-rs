cfg_if::cfg_if! {
    if #[cfg(feature = "std")] {
        use std::fmt;
    } else {
        use core::fmt;
    }
}

use airflow_common::utils::{TaskInstanceState, TerminalTIStateNonSuccess};

/// Possible states of a task instance as a result of task execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionResultTIState {
    /// Task vanished from DAG before it ran
    Removed,
    /// Task completed
    Success,
    /// Task errored out
    Failed,
    /// Task failed but has retries left
    UpForRetry,
    /// A waiting `reschedule` sensor
    UpForReschedule,
    /// Skipped by branching or some other mechanism
    Skipped,
    /// Deferrable operator waiting on a trigger
    Deferred,
}

impl From<ExecutionResultTIState> for TaskInstanceState {
    fn from(state: ExecutionResultTIState) -> Self {
        match state {
            ExecutionResultTIState::Removed => TaskInstanceState::Removed,
            ExecutionResultTIState::Success => TaskInstanceState::Success,
            ExecutionResultTIState::Failed => TaskInstanceState::Failed,
            ExecutionResultTIState::UpForRetry => TaskInstanceState::UpForRetry,
            ExecutionResultTIState::UpForReschedule => TaskInstanceState::UpForReschedule,
            ExecutionResultTIState::Skipped => TaskInstanceState::Skipped,
            ExecutionResultTIState::Deferred => TaskInstanceState::Deferred,
        }
    }
}

impl From<ExecutionResultTIState> for Option<TerminalTIStateNonSuccess> {
    fn from(state: ExecutionResultTIState) -> Self {
        match state {
            ExecutionResultTIState::Removed => Some(TerminalTIStateNonSuccess::Removed),
            ExecutionResultTIState::Success => None,
            ExecutionResultTIState::Failed => Some(TerminalTIStateNonSuccess::Failed),
            ExecutionResultTIState::UpForRetry => None,
            ExecutionResultTIState::UpForReschedule => None,
            ExecutionResultTIState::Skipped => Some(TerminalTIStateNonSuccess::Skipped),
            ExecutionResultTIState::Deferred => None,
        }
    }
}

impl fmt::Display for ExecutionResultTIState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        TaskInstanceState::from(*self).fmt(f)
    }
}
