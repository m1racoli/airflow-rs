use serde::{Deserialize, Serialize};

cfg_if::cfg_if! {
    if #[cfg(feature = "std")] {
        use std::fmt;
    } else {
        use core::fmt;
    }
}

/// All DagRun types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DagRunType {
    Backfill,
    Scheduled,
    Manual,
    AssetTriggered,
}

impl fmt::Display for DagRunType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DagRunType::Backfill => write!(f, "backfill"),
            DagRunType::Scheduled => write!(f, "scheduled"),
            DagRunType::Manual => write!(f, "manual"),
            DagRunType::AssetTriggered => write!(f, "asset_triggered"),
        }
    }
}

/// A secret string that hides its content in Debug and Display implementations.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretString(String);

impl SecretString {
    /// Returns the inner secret string.
    pub fn secret(&self) -> &str {
        &self.0
    }
}

impl From<String> for SecretString {
    fn from(secret: String) -> Self {
        SecretString(secret)
    }
}

impl From<&str> for SecretString {
    fn from(secret: &str) -> Self {
        SecretString(secret.to_string())
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SecretString(***)")
    }
}

impl fmt::Display for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "***")
    }
}
