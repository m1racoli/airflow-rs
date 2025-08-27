extern crate alloc;
use alloc::string::String;

pub type JsonValue = serde_json::Value;

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

/// Serialize an object into a representation consisting only of valid JSON types.
pub fn serialize<T: JsonSerialize>(value: &T) -> Result<JsonValue, JsonSerdeError> {
    let serialized = value.serialize()?;
    let value = serde_json::from_str(&serialized)?;
    Ok(value)
}

/// Deserialize an object from a representation consisting only of valid JSON types.
pub fn deserialize<T: JsonDeserialize>(value: &JsonValue) -> Result<T, JsonSerdeError> {
    let s = serde_json::to_string(value)?;
    let deserialized = T::deserialize(&s)?;
    Ok(deserialized)
}
