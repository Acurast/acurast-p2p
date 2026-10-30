//! The processor's iroh endpoint, connected only to the relays listed on chain.

use std::sync::Mutex;

use iroh::{
    Endpoint, EndpointAddr, EndpointId, RelayMode, SecretKey,
    endpoint::{Connection, presets},
};

use crate::{Error, relays::Relay};

/// iroh endpoint of a processor (or any other registered peer).
///
/// It uses only the given relays, never iroh's public ones, so peers reach it through the Acurast
/// relays or directly.
pub struct P2pEndpoint {
    endpoint: Endpoint,
    relays: Mutex<Vec<Relay>>,
}

impl P2pEndpoint {
    /// Binds an endpoint with the key `secret_key`, the given relays and the protocols (ALPNs) it
    /// accepts.
    pub async fn bind(
        secret_key: SecretKey,
        relays: Vec<Relay>,
        alpns: Vec<Vec<u8>>,
    ) -> Result<Self, Error> {
        let endpoint = Endpoint::builder(presets::Minimal)
            .secret_key(secret_key)
            .relay_mode(RelayMode::Custom(crate::relays::relay_map(&relays)))
            .alpns(alpns)
            .bind()
            .await?;
        Ok(Self {
            endpoint,
            relays: Mutex::new(relays),
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
        self.endpoint.close().await
    }
}
