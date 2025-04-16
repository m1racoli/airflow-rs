/// A UTC date time to be used in Airflow.
pub type DateTime = chrono::DateTime<chrono::Utc>;

/// The minimum possible DateTime.
pub const MIN_UTC: DateTime = chrono::DateTime::<chrono::Utc>::MIN_UTC;

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
        chrono::Utc::now()
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
