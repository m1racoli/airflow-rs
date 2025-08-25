cfg_if::cfg_if! {
    if #[cfg(feature = "std")] {
    } else {
        extern crate alloc;
        use alloc::boxed::Box;
    }
}

use crate::definitions::{JsonDeserialize, JsonSerialize};

pub trait XComValue: JsonSerialize + JsonDeserialize + Send {}

impl<T> XComValue for T where T: JsonSerialize + JsonDeserialize + Send {}

impl<T> From<T> for Box<dyn XComValue>
where
    T: XComValue + 'static,
{
    fn from(value: T) -> Self {
        Box::new(value)
    }
}
