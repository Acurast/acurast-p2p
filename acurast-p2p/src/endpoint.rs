//! The processor's iroh endpoint, connected only to the relays listed on chain.

use std::sync::{Arc, Mutex};

use iroh::{
    Endpoint, EndpointAddr, EndpointId, RelayMode, SecretKey,
    endpoint::{Connection, presets},
};
use tokio::task::JoinHandle;

use crate::{
    Error,
    registry::{RegistryLookup, RegistryWorker, SharedRelays},
    relays::Relay,
};

/// iroh endpoint of a processor (or any other registered peer).
///
/// It uses only the given relays, never iroh's public ones, so peers reach it through the Acurast
/// relays or directly. Its address is published to the registry on the relay nodes, and the
/// addresses of the peers it connects to by id are looked up there.
pub struct P2pEndpoint {
    endpoint: Endpoint,
    relays: SharedRelays,
    registry: RegistryLookup,
    registry_worker: JoinHandle<()>,
    alpns: Mutex<Vec<Vec<u8>>>,
}

impl P2pEndpoint {
    /// Binds an endpoint with the key `secret_key`, the given relays and the protocols (ALPNs) it
    /// accepts.
    pub async fn bind(
        secret_key: SecretKey,
        relays: Vec<Relay>,
        alpns: Vec<Vec<u8>>,
    ) -> Result<Self, Error> {
        let shared_relays: SharedRelays = Arc::new(Mutex::new(relays.clone()));
        let (registry, commands) = RegistryLookup::new(shared_relays.clone());
        let endpoint = Endpoint::builder(presets::Minimal)
            .secret_key(secret_key)
            .relay_mode(RelayMode::Custom(crate::relays::relay_map(&relays)))
            .address_lookup(registry.clone())
            .alpns(alpns.clone())
            .bind()
            .await?;
        let registry_worker =
            RegistryWorker::spawn(endpoint.clone(), shared_relays.clone(), commands);
        Ok(Self {
            endpoint,
            relays: shared_relays,
            registry,
            registry_worker,
            alpns: Mutex::new(alpns),
        })
    }

    /// The `EndpointId` of this endpoint, i.e. the public key registered on chain.
    pub fn id(&self) -> EndpointId {
        self.endpoint.id()
    }

    /// Address of this endpoint: its id, home relay and direct addresses known so far.
    pub fn addr(&self) -> EndpointAddr {
        self.endpoint.addr()
    }

    /// Waits until the endpoint is connected to its home relay.
    pub async fn online(&self) {
        self.endpoint.online().await
    }

    /// Replaces the relays, e.g. after the relay list changed on chain.
    pub async fn set_relays(&self, relays: Vec<Relay>) {
        let previous = std::mem::replace(
            &mut *self.relays.lock().expect("relay list lock poisoned"),
            relays.clone(),
        );
        for relay in previous
            .iter()
            .filter(|r| !relays.iter().any(|n| n.url == r.url))
        {
            self.endpoint.remove_relay(&relay.url).await;
        }
        for relay in relays.iter().filter(|r| !previous.contains(r)) {
            self.endpoint
                .insert_relay(relay.url.clone(), relay.config())
                .await;
        }
        self.registry.relays_changed();
    }

    /// Re-reads the relay nodes from the chain and applies them; returns whether they changed.
    pub async fn refresh_relays(&self, chain: &dyn crate::platform::Chain) -> Result<bool, Error> {
        let relays = crate::chain::relay_nodes(chain).await?;
        if relays == self.relays() {
            return Ok(false);
        }
        self.set_relays(relays).await;
        Ok(true)
    }

    /// Accepts the protocol `alpn` on new incoming connections, in addition to the others.
    pub(crate) fn add_alpn(&self, alpn: &[u8]) {
        let mut alpns = self.alpns.lock().expect("ALPN list lock poisoned");
        if !alpns.iter().any(|a| a == alpn) {
            alpns.push(alpn.to_vec());
            self.endpoint.set_alpns(alpns.clone());
        }
    }

    /// The relays in use.
    pub fn relays(&self) -> Vec<Relay> {
        self.relays
            .lock()
            .expect("relay list lock poisoned")
            .clone()
    }

    /// Connects to the peer at `addr` with the protocol `alpn`.
    pub async fn connect(
        &self,
        addr: impl Into<EndpointAddr>,
        alpn: &[u8],
    ) -> Result<Connection, Error> {
        Ok(self.endpoint.connect(addr, alpn).await?)
    }

    /// Waits for the next incoming connection; `None` once the endpoint is closed.
    pub async fn accept(&self) -> Option<Result<Connection, Error>> {
        let incoming = self.endpoint.accept().await?;
        Some(incoming.await.map_err(Error::from))
    }

    /// The underlying iroh endpoint.
    pub fn inner(&self) -> &Endpoint {
        &self.endpoint
    }

    /// Closes all connections and the endpoint.
    pub async fn close(&self) {
        self.registry_worker.abort();
        self.endpoint.close().await
    }
}

impl Drop for P2pEndpoint {
    fn drop(&mut self) {
        // The worker holds a handle to the endpoint, which would otherwise outlive this.
        self.registry_worker.abort();
    }
}
