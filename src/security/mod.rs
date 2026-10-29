//! Security: mutual TLS with hot-reloaded certificates, caller identity,
//! role-based access control, hashed bearer tokens, tamper-evident audit and
//! key management.

pub mod audit;
pub mod identity;
pub mod keys;
pub mod rbac;
pub mod tls;
pub mod tokens;

pub use rbac::{Policy, PolicyHandle, Role, Rpc};
pub use tls::{PeerInfo, TlsPaths, TlsState};
pub use tokens::{TokenHandle, TokenSet};
