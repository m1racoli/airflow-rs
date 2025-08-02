cfg_if::cfg_if! {
    if #[cfg(feature = "std")] {
        use std::error;
        use std::fmt;
    } else {
        extern crate alloc;
        use alloc::string::String;
        use alloc::string::ToString;
        use core::error;
        use core::fmt;
    }
}

pub trait JWTGenerator {
    type Error: error::Error;

    fn generate(&self, method: &str) -> Result<String, Self::Error>;
}

/// A mock JWT generator that generates a simple token for testing purposes.
///
/// The token has the form `<method>:<secret>`, where `<method>` is the method passed to the `generate`
/// method and `<secret>` is the secret used to create the generator.
#[derive(Debug, Clone)]
pub struct MockJWTGenerator {
    secret: String,
}

impl MockJWTGenerator {
    pub fn new(secret: &str) -> Self {
        MockJWTGenerator {
            secret: secret.to_string(),
        }
    }
}

impl JWTGenerator for MockJWTGenerator {
    type Error = fmt::Error;

    fn generate(&self, method: &str) -> Result<String, Self::Error> {
        Ok(format!("{}:{}", method, self.secret))
    }
}
