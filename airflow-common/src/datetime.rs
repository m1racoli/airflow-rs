use serde::{Deserialize, Serialize};

cfg_if::cfg_if! {
    if #[cfg(feature = "std")] {
        use std::fmt;
        use std::ops;
    } else {
        use core::fmt;
        use core::ops;
    }
}

/// A UTC date time to be used in Airflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DateTime(chrono::DateTime<chrono::Utc>);

impl DateTime {
    pub fn from_timestamp(secs: i64, nsecs: u32) -> Option<Self> {
        let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nsecs)?;
        Some(DateTime(dt))
    }

    pub fn min() -> Self {
        DateTime(chrono::DateTime::<chrono::Utc>::MIN_UTC)
    }
}

impl From<chrono::DateTime<chrono::Utc>> for DateTime {
    fn from(dt: chrono::DateTime<chrono::Utc>) -> Self {
        DateTime(dt)
    }
}

impl From<DateTime> for chrono::DateTime<chrono::Utc> {
    fn from(dt: DateTime) -> Self {
        dt.0
    }
}

impl ops::Deref for DateTime {
    type Target = chrono::DateTime<chrono::Utc>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Display for DateTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// A trait for providing the current time as a [DateTime].
pub trait TimeProvider {
    fn now(&self) -> DateTime;
}

/// A time provider that uses the system clock to get the current time.
#[cfg(feature = "now")]
#[derive(Debug, Clone, Copy)]
pub struct StdTimeProvider;

#[cfg(feature = "now")]
impl TimeProvider for StdTimeProvider {
    fn now(&self) -> DateTime {
        DateTime(chrono::Utc::now())
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub struct MockTimeProvider {
    now: DateTime,
}

#[cfg(test)]
impl MockTimeProvider {
    pub fn new(now: DateTime) -> Self {
        MockTimeProvider { now }
    }
}

#[cfg(test)]
impl TimeProvider for MockTimeProvider {
    fn now(&self) -> DateTime {
        self.now
    }
}
