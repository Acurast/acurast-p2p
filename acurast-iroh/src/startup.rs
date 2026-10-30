//! Startup sequence of a processor's P2P endpoint.

use crate::{
    Error, P2pEndpoint,
    identity::{Registration, ensure_registered, load_or_create_key},
    platform::{Chain, KeyStore},
};

/// Starts the processor's endpoint:
///
/// 1. loads the iroh key, or creates it on the first start;
/// 2. registers its `EndpointId` on chain if the chain does not have it yet;
/// 3. reads the relay nodes listed on chain;
/// 4. binds the endpoint with those relays and the given protocols (ALPNs).
pub async fn start(
    key_store: &dyn KeyStore,
    chain: &dyn Chain,
    alpns: Vec<Vec<u8>>,
) -> Result<(P2pEndpoint, Registration), Error> {
    let key = load_or_create_key(key_store)?;
    let registration = ensure_registered(chain, &key).await?;
    let relays = crate::chain::relay_nodes(chain).await?;
    let endpoint = P2pEndpoint::bind(key, relays, alpns).await?;
    Ok((endpoint, registration))
}
