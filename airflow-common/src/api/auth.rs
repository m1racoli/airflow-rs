use serde::Serialize;

use crate::datetime::TimeProvider;
use crate::utils::SecretString;

cfg_if::cfg_if! {
    if #[cfg(feature = "std")] {
    } else {
        extern crate alloc;
        use alloc::string::String;
        use alloc::string::ToString;
    }
}

pub trait JWTGenerator {
    type Error;

    fn generate(&self, method: &str) -> Result<String, Self::Error>;
}

#[cfg(feature = "jsonwebtoken")]
#[derive(Debug, Serialize)]
struct Claims<'i, 'a, 'm> {
    #[serde(rename = "iss")]
    issuer: Option<&'i str>,
    #[serde(rename = "aud")]
    audience: &'a str,
    #[serde(rename = "nbf")]
    not_before: i64,
    #[serde(rename = "exp")]
    expiry: i64,
    #[serde(rename = "iat")]
    issued_at: i64,
    method: &'m str,
}

#[cfg(feature = "jsonwebtoken")]
#[derive(Debug, Clone)]
pub struct DefaultJWTGenerator<T: TimeProvider + Clone> {
    key: SecretString,
    valid_for: u64,
    time_provider: T,
    audience: String,
    issuer: Option<String>,
}

#[cfg(feature = "jsonwebtoken")]
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

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;
    use crate::datetime::DateTime;
    use crate::datetime::MockTimeProvider;

    static METHOD: &str = "/test123";
    static SECRET: &str = "test123";
    static TS: i64 = 1744597723;
    static ISSUER: &str = "airflow-rs";
    static AUDIENCE: &str = "api";

    #[derive(Debug, Deserialize)]
    struct TestClaims {
        iss: Option<String>,
        aud: String,
        nbf: i64,
        exp: i64,
        iat: i64,
        method: String,
    }

    #[cfg(feature = "jsonwebtoken")]
    #[test]
    fn test_default_jwt_generator() {
        let key = SecretString::from(SECRET);
        let now = DateTime::from_timestamp(TS, 0).unwrap();
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
}
