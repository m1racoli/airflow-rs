cfg_if::cfg_if! {
    if #[cfg(feature = "std")] {
    } else {
        extern crate alloc;
        use alloc::boxed::Box;
    }
}

use crate::definitions::{JsonDeserialize, JsonSerialize};

pub trait XCom: JsonSerialize + JsonDeserialize + Send {}

impl<T> XCom for T where T: JsonSerialize + JsonDeserialize + Send {}

impl<T> From<T> for Box<dyn XCom>
where
    T: XCom + 'static,
{
    fn from(value: T) -> Self {
        Box::new(value)
    }
}
