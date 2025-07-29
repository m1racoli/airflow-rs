/// A UTC date time to be used in Airflow.
pub type UtcDateTime = chrono::DateTime<chrono::Utc>;

/// The minimum possible DateTime.
pub const MIN_UTC: UtcDateTime = chrono::DateTime::<chrono::Utc>::MIN_UTC;

/// A trait for providing the current time as a [DateTime].
pub trait TimeProvider {
    fn now(&self) -> UtcDateTime;
}

/// A time provider that uses the system clock to get the current time.
#[cfg(feature = "now")]
#[derive(Debug, Clone, Copy, Default)]
pub struct StdTimeProvider;

#[cfg(feature = "now")]
impl TimeProvider for StdTimeProvider {
    fn now(&self) -> UtcDateTime {
        chrono::Utc::now()
    }
}

/// A mock time provider that returns a fixed time.
#[derive(Debug, Clone, Copy)]
pub struct MockTimeProvider {
    now: UtcDateTime,
}

impl MockTimeProvider {
    pub fn new(now: UtcDateTime) -> Self {
        MockTimeProvider { now }
    }
}

impl TimeProvider for MockTimeProvider {
    fn now(&self) -> UtcDateTime {
        self.now
    }
}
