cfg_if::cfg_if! {
    if #[cfg(feature = "std")] {
        use std::fmt;
        use std::future;
    } else {
        use core::fmt;
        use core::future;
    }

}

use crate::definitions::Context;

/// An error type which represents different errors that can occur during task execution.
#[derive(thiserror::Error, Debug)]
pub enum TaskError {
    // TODO all task specific errors (deferrable, skipped, etc.)
    #[error("Unknown error")]
    Unknown,
}

/// A trait representing a task that can be executed.
pub trait Task: fmt::Debug + Send {
    fn execute(
        &mut self,
        ctx: &Context,
    ) -> impl future::Future<Output = Result<(), TaskError>> + Send;
}
