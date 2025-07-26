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
pub enum ExecutionTIState {
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

impl From<ExecutionTIState> for TaskInstanceState {
    fn from(state: ExecutionTIState) -> Self {
        match state {
            ExecutionTIState::Removed => TaskInstanceState::Removed,
            ExecutionTIState::Success => TaskInstanceState::Success,
            ExecutionTIState::Failed => TaskInstanceState::Failed,
            ExecutionTIState::UpForRetry => TaskInstanceState::UpForRetry,
            ExecutionTIState::UpForReschedule => TaskInstanceState::UpForReschedule,
            ExecutionTIState::Skipped => TaskInstanceState::Skipped,
            ExecutionTIState::Deferred => TaskInstanceState::Deferred,
        }
    }
}

impl From<ExecutionTIState> for Option<TerminalTIStateNonSuccess> {
    fn from(state: ExecutionTIState) -> Self {
        match state {
            ExecutionTIState::Removed => Some(TerminalTIStateNonSuccess::Removed),
            ExecutionTIState::Success => None,
            ExecutionTIState::Failed => Some(TerminalTIStateNonSuccess::Failed),
            ExecutionTIState::UpForRetry => None,
            ExecutionTIState::UpForReschedule => None,
            ExecutionTIState::Skipped => Some(TerminalTIStateNonSuccess::Skipped),
            ExecutionTIState::Deferred => None,
        }
    }
}

impl fmt::Display for ExecutionTIState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        TaskInstanceState::from(*self).fmt(f)
    }
}
