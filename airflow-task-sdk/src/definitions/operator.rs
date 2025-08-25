use core::fmt;

use crate::definitions::Context;
use crate::definitions::TaskError;
use crate::definitions::XComValue;

#[trait_variant::make(Send + Sync)]
pub trait Operator: Clone + fmt::Debug {
    type Item: XComValue + 'static;
    async fn execute<'t>(&'t mut self, ctx: &'t Context) -> Result<Self::Item, TaskError>;
}
