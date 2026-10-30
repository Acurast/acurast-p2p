//! uniffi bindings of `acurast-p2p`, consumed by the Android (AAR) and iOS (xcframework)
//! processors.
//!
//! The app implements [`KeyStore`] and [`ChainClient`] and starts a [`P2pNode`] with them.

use std::sync::Arc;

use acurast_p2p::{EndpointId, P2pEndpoint, Registration, RoleCache, platform};

uniffi::setup_scaffolding!();

/// Version of this library, so the app can log which one it runs.
#[uniffi::export]
pub fn version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

/// Errors returned to the app, and thrown by the app from its callbacks.
#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum P2pError {
    /// A callback of the app failed.
    #[error("{reason}")]
    Platform { reason: String },
    /// The library failed.
    #[error("{reason}")]
    Internal { reason: String },
}

impl From<uniffi::UnexpectedUniFFICallbackError> for P2pError {
    fn from(e: uniffi::UnexpectedUniFFICallbackError) -> Self {
        Self::Platform { reason: e.reason }
    }
}

impl From<acurast_p2p::Error> for P2pError {
    fn from(e: acurast_p2p::Error) -> Self {
        match e {
            acurast_p2p::Error::Platform(platform::PlatformError(reason)) => {
                Self::Platform { reason }
            }
            other => Self::Internal {
                reason: other.to_string(),
            },
        }
    }
}

impl From<P2pError> for platform::PlatformError {
    fn from(e: P2pError) -> Self {
        Self(e.to_string())
    }
}

/// Secure storage of the iroh seed (Android Keystore, iOS Keychain).
#[uniffi::export(with_foreign)]
pub trait KeyStore: Send + Sync {
    /// The stored seed, `null`/`nil` before the first start.
    fn load_seed(&self) -> Result<Option<Vec<u8>>, P2pError>;
    /// Stores the seed so it survives app restarts and updates.
    fn store_seed(&self, seed: Vec<u8>) -> Result<(), P2pError>;
}

/// The chain, as reached by the processor.
#[uniffi::export(with_foreign)]
#[async_trait::async_trait]
pub trait ChainClient: Send + Sync {
    /// SCALE-encoded account of this processor (32 bytes).
    fn account(&self) -> Vec<u8>;
    /// Hash of the chain's genesis block (32 bytes).
    async fn genesis_hash(&self) -> Result<Vec<u8>, P2pError>;
    /// `state_call` of the runtime API `method` with SCALE-encoded `args`, at the best block.
    async fn state_call(&self, method: String, args: Vec<u8>) -> Result<Vec<u8>, P2pError>;
    /// Submits `AcurastP2p::register_endpoint(id, proof)` signed with the processor key and
    /// returns once it is included.
    async fn register_endpoint(&self, id: Vec<u8>, proof: Vec<u8>) -> Result<(), P2pError>;
}

/// The app's callbacks, seen by `acurast-p2p` as its platform services.
struct Platform {
    key_store: Arc<dyn KeyStore>,
    chain: Arc<dyn ChainClient>,
}

impl platform::KeyStore for Platform {
    fn load_seed(&self) -> Result<Option<Vec<u8>>, platform::PlatformError> {
        Ok(self.key_store.load_seed()?)
    }

    fn store_seed(&self, seed: Vec<u8>) -> Result<(), platform::PlatformError> {
        Ok(self.key_store.store_seed(seed)?)
    }
}

#[async_trait::async_trait]
impl platform::Chain for Platform {
    fn account(&self) -> Vec<u8> {
        self.chain.account()
    }

    async fn genesis_hash(&self) -> Result<Vec<u8>, platform::PlatformError> {
        Ok(self.chain.genesis_hash().await?)
    }

    async fn state_call(
        &self,
        method: String,
        args: Vec<u8>,
    ) -> Result<Vec<u8>, platform::PlatformError> {
        Ok(self.chain.state_call(method, args).await?)
    }

    async fn register_endpoint(
        &self,
        id: Vec<u8>,
        proof: Vec<u8>,
    ) -> Result<(), platform::PlatformError> {
        Ok(self.chain.register_endpoint(id, proof).await?)
    }
}

/// The processor's P2P endpoint.
#[derive(uniffi::Object)]
pub struct P2pNode {
    platform: Arc<Platform>,
    endpoint: P2pEndpoint,
    roles: RoleCache,
    registered_now: bool,
}

#[uniffi::export(async_runtime = "tokio")]
impl P2pNode {
    /// Loads or creates the iroh key, registers it on chain if needed, reads the relays listed on
    /// chain and binds the endpoint.
    #[uniffi::constructor]
    pub async fn start(
        key_store: Arc<dyn KeyStore>,
        chain: Arc<dyn ChainClient>,
    ) -> Result<Arc<Self>, P2pError> {
        let platform = Arc::new(Platform { key_store, chain });
        let (endpoint, registration) =
            acurast_p2p::start(platform.as_ref(), platform.as_ref(), Vec::new()).await?;
        Ok(Arc::new(Self {
            roles: RoleCache::new(platform.clone()),
            platform,
            endpoint,
            registered_now: registration == Registration::Registered,
        }))
    }

    /// This processor's `EndpointId`, hex-encoded.
    pub fn endpoint_id(&self) -> String {
        hex::encode(self.endpoint.id().as_bytes())
    }

    /// Whether this start registered the key on chain (first start, or a new key).
    pub fn registered_now(&self) -> bool {
        self.registered_now
    }

    /// URLs of the relays in use.
    pub fn relays(&self) -> Vec<String> {
        self.endpoint
            .relays()
            .iter()
            .map(|r| r.url.to_string())
            .collect()
    }

    /// Re-reads the relays listed on chain; returns whether they changed.
    pub async fn refresh_relays(&self) -> Result<bool, P2pError> {
        Ok(self.endpoint.refresh_relays(self.platform.as_ref()).await?)
    }

    /// Roles of the hex-encoded `endpoint_id`, as a bit set (1 registered, 2 processor, 4 manager,
    /// 8 relay), cached like for incoming connections.
    pub async fn roles(&self, endpoint_id: String) -> Result<u8, P2pError> {
        let id = parse_endpoint_id(&endpoint_id)?;
        Ok(self.roles.roles(&id).await?.0)
    }

    /// Closes the endpoint and its connections. (Not `close`, which the Kotlin bindings use to
    /// free the object.)
    pub async fn shutdown(&self) {
        self.endpoint.close().await
    }
}

fn parse_endpoint_id(hex_id: &str) -> Result<EndpointId, P2pError> {
    let invalid = || P2pError::Internal {
        reason: format!("invalid endpoint id {hex_id}"),
    };
    let bytes: [u8; 32] = hex::decode(hex_id.trim_start_matches("0x"))
        .map_err(|_| invalid())?
        .try_into()
        .map_err(|_| invalid())?;
    EndpointId::from_bytes(&bytes).map_err(|_| invalid())
}
