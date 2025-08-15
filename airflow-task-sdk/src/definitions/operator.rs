use crate::definitions::Context;
use crate::definitions::TaskError;
use crate::definitions::XCom;

#[trait_variant::make(Send + Sync)]
pub trait Operator: Clone {
    type Item: XCom + 'static;
    async fn execute<'t>(&'t mut self, ctx: &'t Context) -> Result<Self::Item, TaskError>;
}
