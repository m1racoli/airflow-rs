#[cfg(all(feature = "jsonwebtoken", feature = "now"))]
use crate::datetime::StdTimeProvider;

mod auth;
#[cfg(all(feature = "jsonwebtoken", feature = "std"))]
mod jsonwebtoken;
#[cfg(feature = "jwt-compact")]
mod jwt_compact;

pub use auth::JWTGenerator;
pub use auth::MockJWTGenerator;
#[cfg(all(feature = "jsonwebtoken", feature = "std"))]
pub use jsonwebtoken::JsonWebTokenJWTGenerator;
#[cfg(feature = "jwt-compact")]
pub use jwt_compact::JWTCompactJWTGenerator;

#[cfg(all(feature = "jsonwebtoken", feature = "now", feature = "std"))]
pub type StdJWTGenerator = JsonWebTokenJWTGenerator<StdTimeProvider>;
