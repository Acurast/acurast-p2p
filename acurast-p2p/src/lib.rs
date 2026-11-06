//! Processor side of the Acurast P2P layer, built on iroh.
//!
//! Shared by the Android and iOS processors through `acurast-p2p-ffi`. Everything that depends on
//! the platform (key storage, chain access, extrinsic signing) is provided by the app.

mod chain;
mod endpoint;
mod identity;
mod ping;
pub mod platform;
mod registry;
mod relays;
mod roles;
mod startup;

pub use endpoint::P2pEndpoint;
pub use identity::Registration;
pub use iroh::{EndpointAddr, EndpointId, RelayUrl, SecretKey, endpoint::Connection};
pub use ping::{ALPN_PING, PingPath, PingReport};
pub use relays::Relay;
pub use roles::{ROLE_CACHE_NEGATIVE_TTL, ROLE_CACHE_TTL, RoleCache, Roles};
pub use startup::start;

/// Errors of the P2P layer.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid relay URL")]
    InvalidRelayUrl,
    #[error("invalid relay endpoint id")]
    InvalidRelayId,
    #[error("the stored iroh seed is not 32 bytes")]
    InvalidSeed,
    #[error("unexpected data from the chain: {0}")]
    InvalidChainData(String),
    #[error("the endpoint registration did not show up on chain in time")]
    RegistrationTimeout,
    #[error("platform: {0}")]
    Platform(#[from] platform::PlatformError),
    #[error("cannot bind the endpoint: {0}")]
    Bind(#[from] iroh::endpoint::BindError),
    #[error("cannot connect: {0}")]
    Connect(#[from] iroh::endpoint::ConnectError),
    #[error("incoming connection failed: {0}")]
    Accept(#[from] iroh::endpoint::ConnectingError),
    #[error("ping failed: {0}")]
    Ping(String),
}
