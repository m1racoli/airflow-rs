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
