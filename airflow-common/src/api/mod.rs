#[cfg(all(feature = "jsonwebtoken", feature = "now"))]
use crate::datetime::StdTimeProvider;

mod auth;
#[cfg(all(feature = "jsonwebtoken", feature = "std"))]
mod jsonwebtoken_impl;
#[cfg(feature = "jwt-compact")]
mod jwt_compact_impl;

pub use auth::JWTGenerator;
pub use auth::MockJWTGenerator;
#[cfg(all(feature = "jsonwebtoken", feature = "std"))]
pub use jsonwebtoken_impl::JsonWebTokenJWTGenerator;
#[cfg(feature = "jwt-compact")]
pub use jwt_compact_impl::JWTCompactJWTGenerator;

#[cfg(all(feature = "jsonwebtoken", feature = "now", feature = "std"))]
pub type StdJWTGenerator = JsonWebTokenJWTGenerator<StdTimeProvider>;
