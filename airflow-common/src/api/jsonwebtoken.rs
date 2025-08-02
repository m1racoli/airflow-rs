use serde::Serialize;

use crate::{api::JWTGenerator, datetime::TimeProvider, utils::SecretString};

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

#[derive(Debug, Clone)]
pub struct JsonWebTokenJWTGenerator<T: TimeProvider + Clone> {
    key: SecretString,
    valid_for: u64,
    time_provider: T,
    audience: String,
    issuer: Option<String>,
}

impl<T: TimeProvider + Clone> JsonWebTokenJWTGenerator<T> {
    pub fn new(key: SecretString, audience: &str, time_provider: T) -> Self {
        JsonWebTokenJWTGenerator {
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

impl<T: TimeProvider + Clone> JWTGenerator for JsonWebTokenJWTGenerator<T> {
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
        nbf: i64,
        exp: i64,
        iat: i64,
        method: String,
    }

    #[test]
    fn test_default_jwt_generator() {
        let key = SecretString::from(SECRET);
        let now = UtcDateTime::from_timestamp(TS, 0).unwrap();
        let time_provider = MockTimeProvider::new(now);

        let generator =
            JsonWebTokenJWTGenerator::new(key, AUDIENCE, time_provider).with_issuer(ISSUER);
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
