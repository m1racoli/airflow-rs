#[cfg(any(feature = "jsonwebtoken", feature = "jwt-compact"))]
use crate::datetime::StdTimeProvider;

mod auth;
#[cfg(any(feature = "jsonwebtoken", feature = "jwt-compact"))]
pub use auth::DefaultJWTGenerator;
pub use auth::JWTGenerator;
pub use auth::MockJWTGenerator;

#[cfg(any(feature = "jsonwebtoken", feature = "jwt-compact"))]
pub type StdJWTGenerator = DefaultJWTGenerator<StdTimeProvider>;
