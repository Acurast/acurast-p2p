//! The processor's iroh key and its registration on chain.

use acurast_p2p_proto::hash::register_proof_message;
use iroh::SecretKey;

use crate::{
    Error,
    platform::{Chain, KeyStore},
};

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
    Ok(Registration::Registered)
}
