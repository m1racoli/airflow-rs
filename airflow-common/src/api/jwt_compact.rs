cfg_if::cfg_if! {
    if #[cfg(feature = "std")] {
    } else {
        extern crate alloc;
        use alloc::string::String;
        use alloc::string::ToString;
    }
}

use core::fmt;

use jwt_compact::AlgorithmExt;
use serde::Serialize;

use crate::{api::JWTGenerator, prelude::TimeProvider, utils::SecretString};

#[derive(Debug, Serialize)]
struct Claims<'i, 'a, 'm> {
    #[serde(rename = "iss")]
    issuer: Option<&'i str>,
    #[serde(rename = "aud")]
    audience: &'a str,
    method: &'m str,
}

pub struct JWTCompactJWTGenerator<T: TimeProvider + Clone> {
    key: SecretString,
    valid_for: u64,
    time_provider: T,
    audience: String,
    issuer: Option<String>,
}

impl<T: TimeProvider + Clone> JWTCompactJWTGenerator<T> {
    pub fn new(key: SecretString, audience: &str, time_provider: T) -> Self {
        JWTCompactJWTGenerator {
            key,
            valid_for: 600,
            time_provider,
            audience: audience.to_string(),
            issuer: None,
        }
    }

    pub fn with_valid_for(mut self, valid_for: u64) -> Self {
        self.valid_for = valid_for;
        self
    }

    pub fn with_issuer(mut self, issuer: &str) -> Self {
        self.issuer = Some(issuer.to_string());
        self
    }
}

#[derive(Debug, thiserror::Error)]
pub struct CreationError(jwt_compact::CreationError);

impl fmt::Display for CreationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<jwt_compact::CreationError> for CreationError {
    fn from(err: jwt_compact::CreationError) -> Self {
        CreationError(err)
    }
}

impl<T: TimeProvider + Clone> JWTGenerator for JWTCompactJWTGenerator<T> {
    type Error = CreationError;

    fn generate(&self, method: &str) -> Result<String, Self::Error> {
        let now = self.time_provider.now();
        let leeway = chrono::Duration::seconds(self.valid_for as i64);
        let time_options = jwt_compact::TimeOptions::new(leeway, move || now);
        let claims = Claims {
            issuer: self.issuer.as_deref(),
            audience: &self.audience,
            method,
        };

        let header = jwt_compact::Header::empty();
        let claims: jwt_compact::Claims<Claims<'_, '_, '_>> = jwt_compact::Claims::new(claims)
            .set_not_before(now)
            .set_duration_and_issuance(&time_options, leeway);
        let bytes: &[u8] = self.key.secret().as_ref();
        let key = jwt_compact::alg::Hs512Key::new(bytes);
        jwt_compact::alg::Hs512
            .token(&header, &claims, &key)
            .map_err(CreationError)
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;
    use crate::datetime::MockTimeProvider;
    use crate::datetime::UtcDateTime;

    static METHOD: &str = "/test123";
    static SECRET: &str = "test123";
    static TS: i64 = 1744597723;
    static ISSUER: &str = "airflow-rs";
    static AUDIENCE: &str = "api";

    #[derive(Debug, Deserialize)]
    struct TestClaims {
        iss: Option<String>,
        aud: String,
        method: String,
    }

    #[test]
    fn test_default_jwt_generator() {
        let key = SecretString::from(SECRET);
        let now = UtcDateTime::from_timestamp(TS, 0).unwrap();
        let time_provider = MockTimeProvider::new(now);

        let generator =
            JWTCompactJWTGenerator::new(key.clone(), AUDIENCE, time_provider).with_issuer(ISSUER);
        let token = generator.generate(METHOD).unwrap();

        let bytes: &[u8] = key.secret().as_ref();
        let key = jwt_compact::alg::Hs512Key::new(bytes);
        let token = jwt_compact::UntrustedToken::new(&token).unwrap();
        let decoded: jwt_compact::Token<TestClaims> = jwt_compact::alg::Hs512
            .validator(&key)
            .validate(&token)
            .unwrap();

        let claims = decoded.claims();
        assert_eq!(
            claims.expiration,
            Some(now + chrono::Duration::seconds(600))
        );
        assert_eq!(claims.not_before, Some(now));
        assert_eq!(claims.issued_at, Some(now));

        let claims = &claims.custom;
        assert_eq!(claims.iss, Some(ISSUER.to_string()));
        assert_eq!(claims.aud, AUDIENCE);
        assert_eq!(claims.method, METHOD);
    }
}
