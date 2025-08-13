cfg_if::cfg_if! {
    if #[cfg(feature = "std")] {
        use std::time;
    } else {
        use core::time;
    }
}

use crate::{
    api::EdgeJobFetched,
    worker::{IntercomMessage, LocalEdgeJob, LocalIntercom},
};

#[trait_variant::make(Runtime: Send)]
pub trait LocalRuntime {
    type Job: LocalEdgeJob;
    type Intercom: LocalIntercom;

    async fn sleep(&mut self, duration: time::Duration) -> Option<IntercomMessage>;
    fn intercom(&self) -> Self::Intercom;
    fn launch(&self, job: EdgeJobFetched) -> Self::Job;
    fn concurrency(&self) -> usize;
}
