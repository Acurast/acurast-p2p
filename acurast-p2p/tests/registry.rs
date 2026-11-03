//! Endpoints publish their address to a registry served next to a local relay, and peers find
//! them knowing only their `EndpointId`.

use std::{net::Ipv4Addr, sync::Arc, time::Duration};

use acurast_p2p::{EndpointAddr, P2pEndpoint, Relay, SecretKey};
use acurast_p2p_registry::{Registry, RoleSource};
use iroh::{Endpoint, RelayMode, endpoint::presets, protocol::Router};
use iroh_relay::server::{RelayConfig, Server, ServerConfig};

const ALPN: &[u8] = b"acurast/test/echo";
const TIMEOUT: Duration = Duration::from_secs(20);
const ALPN_REGISTRY: &[u8] = b"acurast/registry/1";
const ROLE_REGISTERED: u8 = 0b0001;

/// Every endpoint counts as registered.
struct EveryoneRegistered;

impl RoleSource for EveryoneRegistered {
    fn roles(&self, _: &[u8; 32]) -> Result<u8, String> {
        Ok(ROLE_REGISTERED)
    }
}

/// A local relay with a relay node endpoint serving the registry, as the Acurast node does.
struct RelayNode {
    _server: Server,
    _router: Router,
    registry: Registry,
    relay: Relay,
}

async fn spawn_relay_node() -> RelayNode {
    let mut config = ServerConfig::default();
    config.relay = Some(RelayConfig::new((Ipv4Addr::LOCALHOST, 0)));
    let server = Server::spawn(config).await.unwrap();
    let url = format!("http://{}", server.http_addr().unwrap());

    let key = SecretKey::generate();
    let relay = Relay::from_chain(key.public().as_bytes(), url.as_bytes(), None).unwrap();
    let endpoint = Endpoint::builder(presets::Minimal)
        .secret_key(key)
        .relay_mode(RelayMode::Custom(
            [iroh::RelayConfig::new(relay.url.clone(), None)]
                .into_iter()
                .collect(),
        ))
        .bind()
        .await
        .unwrap();
    tokio::time::timeout(TIMEOUT, endpoint.online())
        .await
        .unwrap();
    let registry = Registry::new(Arc::new(EveryoneRegistered));
    let router = Router::builder(endpoint)
        .accept(ALPN_REGISTRY, registry.clone())
        .spawn();
    RelayNode {
        _server: server,
        _router: router,
        registry,
        relay,
    }
}

async fn bind(relay: &Relay) -> P2pEndpoint {
    let endpoint = P2pEndpoint::bind(
        SecretKey::generate(),
        vec![relay.clone()],
        vec![ALPN.to_vec()],
    )
    .await
    .unwrap();
    tokio::time::timeout(TIMEOUT, endpoint.online())
        .await
        .expect("home relay connection");
    endpoint
}

#[tokio::test]
async fn peers_are_found_by_id_through_the_registry() {
    let node = spawn_relay_node().await;
    let listener = bind(&node.relay).await;
    let dialer = bind(&node.relay).await;

    // Both endpoints publish once they know their home relay (after the publish debounce).
    tokio::time::timeout(TIMEOUT, async {
        while node.registry.len() < 2 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("records published");

    let listener_id = listener.id();
    let accept = tokio::spawn(async move {
        let conn = listener.accept().await.unwrap().unwrap();
        let (mut send, mut recv) = conn.accept_bi().await.unwrap();
        let msg = recv.read_to_end(1024).await.unwrap();
        send.write_all(&msg).await.unwrap();
        send.finish().unwrap();
        conn.closed().await;
        conn.remote_id()
    });

    // Only the id: the address comes from the registry.
    let conn = tokio::time::timeout(
        TIMEOUT,
        dialer.connect(EndpointAddr::new(listener_id), ALPN),
    )
    .await
    .expect("connect in time")
    .expect("connect");
    let (mut send, mut recv) = conn.open_bi().await.unwrap();
    send.write_all(b"hello").await.unwrap();
    send.finish().unwrap();
    assert_eq!(recv.read_to_end(1024).await.unwrap(), b"hello");
    conn.close(0u32.into(), b"done");

    assert_eq!(accept.await.unwrap(), dialer.id());
}
