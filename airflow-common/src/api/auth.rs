#[cfg(feature = "jwt-compact")]
use jwt_compact::AlgorithmExt;
use serde::Serialize;

use crate::datetime::TimeProvider;
use crate::utils::SecretString;

cfg_if::cfg_if! {
    if #[cfg(feature = "std")] {
        use std::error;
        use std::io;
    } else {
        extern crate alloc;
        use alloc::string::String;
        use alloc::string::ToString;
        use core::error;
        use core::io;
    }
}

pub trait JWTGenerator {
    type Error: error::Error;

    fn generate(&self, method: &str) -> Result<String, Self::Error>;
}

#[cfg(any(feature = "jsonwebtoken", feature = "jwt-compact"))]
#[derive(Debug, Serialize)]
struct Claims<'i, 'a, 'm> {
    #[serde(rename = "iss")]
    issuer: Option<&'i str>,
    #[serde(rename = "aud")]
    audience: &'a str,
    #[cfg(feature = "jsonwebtoken")]
    #[serde(rename = "nbf")]
    not_before: i64,
    #[cfg(feature = "jsonwebtoken")]
    #[serde(rename = "exp")]
    expiry: i64,
    #[cfg(feature = "jsonwebtoken")]
    #[serde(rename = "iat")]
    issued_at: i64,
    method: &'m str,
}

#[cfg(any(feature = "jsonwebtoken", feature = "jwt-compact"))]
#[derive(Debug, Clone)]
pub struct DefaultJWTGenerator<T: TimeProvider + Clone> {
    key: SecretString,
    valid_for: u64,
    time_provider: T,
    audience: String,
    issuer: Option<String>,
}

#[cfg(any(feature = "jsonwebtoken", feature = "jwt-compact"))]
impl<T: TimeProvider + Clone> DefaultJWTGenerator<T> {
    pub fn new(key: SecretString, audience: &str, time_provider: T) -> Self {
        DefaultJWTGenerator {
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

#[cfg(feature = "jsonwebtoken")]
impl<T: TimeProvider + Clone> JWTGenerator for DefaultJWTGenerator<T> {
    type Error = jsonwebtoken::errors::Error;

    fn generate(&self, method: &str) -> Result<String, Self::Error> {
        let now = self.time_provider.now().timestamp();
        let claims = Claims {
            issuer: self.issuer.as_deref(),
            audience: &self.audience,
            not_before: now,
            expiry: now + self.valid_for as i64,
            issued_at: now,
            method,
        };
        let header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS512);
        let key = jsonwebtoken::EncodingKey::from_secret(self.key.secret().as_ref());
        jsonwebtoken::encode(&header, &claims, &key)
    }
}

#[cfg(feature = "jwt-compact")]
impl<T: TimeProvider + Clone> JWTGenerator for DefaultJWTGenerator<T> {
    type Error = jwt_compact::CreationError;

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
        jwt_compact::alg::Hs512.token(&header, &claims, &key)
    }
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
    type Error = io::Error;

    fn generate(&self, method: &str) -> Result<String, Self::Error> {
        Ok(format!("{}:{}", method, self.secret))
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
        #[cfg(feature = "jsonwebtoken")]
        nbf: i64,
        #[cfg(feature = "jsonwebtoken")]
        exp: i64,
        #[cfg(feature = "jsonwebtoken")]
        iat: i64,
        method: String,
    }

    #[cfg(feature = "jsonwebtoken")]
    #[test]
    fn test_default_jwt_generator() {
        let key = SecretString::from(SECRET);
        let now = UtcDateTime::from_timestamp(TS, 0).unwrap();
        let time_provider = MockTimeProvider::new(now);

        let generator = DefaultJWTGenerator::new(key, AUDIENCE, time_provider).with_issuer(ISSUER);
        let token = generator.generate(METHOD).unwrap();

        let key = jsonwebtoken::DecodingKey::from_secret(SECRET.as_ref());
        let algorithm = jsonwebtoken::Algorithm::HS512;
        let mut validation = jsonwebtoken::Validation::new(algorithm);
        validation.validate_exp = false;
        validation.validate_aud = false;

        let decoded = jsonwebtoken::decode::<TestClaims>(&token, &key, &validation).unwrap();
        let claims = decoded.claims;

        assert_eq!(claims.iss, Some(ISSUER.to_string()));
        assert_eq!(claims.aud, AUDIENCE);
        assert_eq!(claims.nbf, TS);
        assert_eq!(claims.exp, TS + 600);
        assert_eq!(claims.iat, TS);
        assert_eq!(claims.method, METHOD);
    }

    #[cfg(feature = "jwt-compact")]
    #[test]
    fn test_default_jwt_generator() {
        let key = SecretString::from(SECRET);
        let now = UtcDateTime::from_timestamp(TS, 0).unwrap();
        let time_provider = MockTimeProvider::new(now);

        let generator =
            DefaultJWTGenerator::new(key.clone(), AUDIENCE, time_provider).with_issuer(ISSUER);
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
