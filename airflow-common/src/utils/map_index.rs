cfg_if::cfg_if! {
    if #[cfg(feature = "std")] {
        use std::fmt;
    } else {
        use core::fmt;
    }
}

use serde::de::Error;
use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub struct MapIndexConversionError {
    value: i64,
}

impl fmt::Display for MapIndexConversionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Invalid map index {} - must be -1 or a non-negative integer",
            self.value
        )
    }
}

/// Helper type for serializing and deserializing map indexes.
///
/// Historically in Airflow, map indexes were represented as integers, with -1
/// indicating that the task instance was not a mapped task.
/// To be more idiomatic, we use an `Option<usize>` to represent the map index in Rust.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct MapIndex(Option<usize>);

impl MapIndex {
    pub fn some(index: usize) -> Self {
        MapIndex(Some(index))
    }

    pub fn none() -> Self {
        MapIndex(None)
    }
}

impl Serialize for MapIndex {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self.0 {
            Some(index) => index.serialize(serializer),
            None => serializer.serialize_i64(-1),
        }
    }
}

impl<'de> Deserialize<'de> for MapIndex {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let val = i64::deserialize(deserializer)?;
        if val == -1 {
            Ok(MapIndex(None))
        } else if val >= 0 {
            Ok(MapIndex(Some(val as usize)))
        } else {
            Err(D::Error::custom(format!("Invalid map index: {val}")))
        }
    }
}

impl fmt::Display for MapIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(index) => index.fmt(f),
            None => write!(f, "-1"),
        }
    }
}

impl From<MapIndex> for Option<usize> {
    fn from(map_index: MapIndex) -> Self {
        map_index.0
    }
}

impl From<Option<usize>> for MapIndex {
    fn from(map_index: Option<usize>) -> Self {
        MapIndex(map_index)
    }
}

impl From<usize> for MapIndex {
    fn from(map_index: usize) -> Self {
        MapIndex(Some(map_index))
    }
}

impl TryFrom<i64> for MapIndex {
    type Error = MapIndexConversionError;

    fn try_from(map_index: i64) -> Result<Self, Self::Error> {
        match map_index {
            -1 => Ok(MapIndex(None)),
            v if v >= 0 => Ok(MapIndex(Some(v as usize))),
            _ => Err(MapIndexConversionError { value: map_index }),
        }
    }
}

impl From<MapIndex> for i64 {
    fn from(map_index: MapIndex) -> Self {
        match map_index.0 {
            Some(index) => index as i64,
            None => -1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_map_index_serialize_none() {
        let map_index = MapIndex(None);
        let serialized = serde_json::to_string(&map_index).unwrap();
        assert_eq!(serialized, "-1");
    }

    #[test]
    fn test_map_index_serialize_some() {
        let map_index = MapIndex(Some(5));
        let serialized = serde_json::to_string(&map_index).unwrap();
        assert_eq!(serialized, "5");
    }

    #[test]
    fn test_map_index_deserialize_none() {
        let json = "-1";
        let deserialized: MapIndex = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized, MapIndex(None));
    }

    #[test]
    fn test_map_index_deserialize_some() {
        let json = "5";
        let deserialized: MapIndex = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized, MapIndex(Some(5)));
    }

    #[test]
    fn test_map_index_deserialize_invalid_negative() {
        let json = "-2";
        let deserialized: Result<MapIndex, _> = serde_json::from_str(json);
        assert!(deserialized.is_err());
    }

    #[test]
    fn test_map_index_deserialize_invalid_format() {
        let json = "\"invalid\"";
        let deserialized: Result<MapIndex, _> = serde_json::from_str(json);
        assert!(deserialized.is_err());
    }
}
