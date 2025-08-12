cfg_if::cfg_if! {
    if #[cfg(feature = "std")] {
        use std::error;
    } else {
        use core::error;
    }
}

use airflow_common::models::TaskInstanceKey;

#[derive(Debug, Clone)]
pub enum IntercomMessage {
    Shutdown,
    Terminate,
    JobCompleted(TaskInstanceKey),
}

#[trait_variant::make(Intercom: Send)]
pub trait LocalIntercom: Clone {
    type SendError: error::Error;
    async fn send(&self, msg: IntercomMessage) -> Result<(), Self::SendError>;
}
