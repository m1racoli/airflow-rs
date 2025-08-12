use airflow_common::models::TaskInstanceKey;

#[trait_variant::make(EdgeJob: Send)]
pub trait LocalEdgeJob {
    fn is_running(&self) -> bool;
    fn abort(&mut self);
    fn ti_key(&self) -> &TaskInstanceKey;
    fn concurrency_slots(&self) -> usize;
    async fn is_success(&mut self) -> bool;
}
