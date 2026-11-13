//! The processor's iroh key and its registration on chain.

use std::time::Duration;

use acurast_p2p_proto::hash::register_proof_message;
use iroh::SecretKey;

use crate::{
    Error,
    platform::{Chain, KeyStore},
};

/// How often the chain is checked while waiting for a submitted registration.
const REGISTRATION_POLL_INTERVAL: Duration = Duration::from_secs(3);
/// How long a submitted registration may take to show up on chain.
const REGISTRATION_TIMEOUT: Duration = Duration::from_secs(120);

/// Loads the iroh key, or creates and stores a new one on the first start.
pub(crate) fn load_or_create_key(store: &dyn KeyStore) -> Result<SecretKey, Error> {
    if let Some(seed) = store.load_seed()? {
        let seed: [u8; 32] = seed.try_into().map_err(|_| Error::InvalidSeed)?;
        return Ok(SecretKey::from_bytes(&seed));
    }
    let key = SecretKey::generate();
    store.store_seed(key.to_bytes().to_vec())?;
    Ok(key)
}

/// Loads the iroh key like [`load_or_create_key`], but replaces it with a new one when another
/// account registered it. The key can outlive the processor account (the iOS Keychain keeps it
/// when the app is paired again and gets a new account), while an endpoint belongs to the account
/// that registered it: the new account could never register the old key.
pub(crate) async fn load_key_for_account(
    store: &dyn KeyStore,
    chain: &dyn Chain,
) -> Result<SecretKey, Error> {
    let key = load_or_create_key(store)?;
    match crate::chain::endpoint_owner(chain, key.public().as_bytes()).await? {
        Some(owner) if owner != chain.account() => {
            let key = SecretKey::generate();
            store.store_seed(key.to_bytes().to_vec())?;
            Ok(key)
        }
        _ => Ok(key),
    }
}

/// Whether [`ensure_registered`] had to register the key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Registration {
    AlreadyRegistered,
    Registered,
}

/// Registers the key's `EndpointId` for this processor unless the chain already has it.
///
/// The proof is the key's signature over the genesis hash and the account, so it is valid only
/// for this account on this chain.
pub(crate) async fn ensure_registered(
    chain: &dyn Chain,
    key: &SecretKey,
) -> Result<Registration, Error> {
    let id = *key.public().as_bytes();
    if crate::chain::own_endpoints(chain).await?.contains(&id) {
        return Ok(Registration::AlreadyRegistered);
    }
    let genesis: [u8; 32] = chain
        .genesis_hash()
        .await?
        .try_into()
        .map_err(|_| Error::InvalidChainData("genesis hash: expected 32 bytes".into()))?;
    let proof = key.sign(&register_proof_message(&genesis, &chain.account()));
    chain
        .register_endpoint(id.to_vec(), proof.to_bytes().to_vec())
        .await?;
    wait_until_registered(chain, &id).await?;
    Ok(Registration::Registered)
}

/// Waits until the chain lists `id` for this processor, so relays admit it right after start.
async fn wait_until_registered(chain: &dyn Chain, id: &[u8; 32]) -> Result<(), Error> {
    let deadline = tokio::time::Instant::now() + REGISTRATION_TIMEOUT;
    loop {
        if crate::chain::own_endpoints(chain).await?.contains(id) {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::RegistrationTimeout);
        }
        tokio::time::sleep(REGISTRATION_POLL_INTERVAL).await;
    }
}
