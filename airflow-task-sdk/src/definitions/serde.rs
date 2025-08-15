cfg_if::cfg_if! {
    if #[cfg(feature = "std")] {
    } else {
        extern crate alloc;
        use alloc::string::String;
    }
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct JsonSerdeError(#[from] serde_json::Error);

pub trait JsonSerialize {
    fn serialize(&self) -> Result<String, JsonSerdeError>;
}

impl<T> JsonSerialize for T
where
    T: serde::Serialize,
{
    fn serialize(&self) -> Result<String, JsonSerdeError> {
        serde_json::to_string(self).map_err(JsonSerdeError)
    }
}

pub trait JsonDeserialize {
    fn deserialize(s: &str) -> Result<Self, JsonSerdeError>
    where
        Self: Sized;
}

impl<T> JsonDeserialize for T
where
    T: serde::de::DeserializeOwned,
{
    fn deserialize(s: &str) -> Result<Self, JsonSerdeError> {
        serde_json::from_str(s).map_err(JsonSerdeError)
    }
}
