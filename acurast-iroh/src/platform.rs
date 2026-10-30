//! Services the app provides to the library: key storage and chain access.
//!
//! They differ per platform (Android Keystore / iOS Keychain, the processor's own RPC and
//! extrinsic signing), so the library only calls them through these traits.

/// Error reported by the app.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct PlatformError(pub String);

/// Secure storage of the iroh seed.
pub trait KeyStore: Send + Sync {
    /// The stored seed, `None` before the first start.
    fn load_seed(&self) -> Result<Option<Vec<u8>>, PlatformError>;
    /// Stores the seed so it survives app restarts and updates.
    fn store_seed(&self, seed: Vec<u8>) -> Result<(), PlatformError>;
}

/// The chain, as reached by the processor.
#[async_trait::async_trait]
pub trait Chain: Send + Sync {
    /// SCALE-encoded account of this processor (32 bytes for `AccountId32`).
    fn account(&self) -> Vec<u8>;
    /// Hash of the chain's genesis block.
    async fn genesis_hash(&self) -> Result<Vec<u8>, PlatformError>;
    /// Calls a runtime API at the best block (`state_call`) and returns the SCALE-encoded result.
    async fn state_call(&self, method: String, args: Vec<u8>) -> Result<Vec<u8>, PlatformError>;
    /// Submits `AcurastP2p::register_endpoint(id, proof)` signed with the processor key and
    /// returns once it is included.
    async fn register_endpoint(&self, id: Vec<u8>, proof: Vec<u8>) -> Result<(), PlatformError>;
}
