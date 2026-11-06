//! Endpoints answer the pings of registered peers, found by id through the registry, and refuse
//! the others.

use std::{
    collections::HashMap,
    net::Ipv4Addr,
    sync::{Arc, Mutex},
    time::Duration,
};

use acurast_p2p::{
    EndpointId, P2pEndpoint, Relay, RoleCache, Roles, SecretKey,
    platform::{Chain, PlatformError},
};
use acurast_p2p_registry::{Registry, RoleSource};
use iroh::{Endpoint, RelayMode, endpoint::presets, protocol::Router};
use iroh_relay::server::{RelayConfig, Server, ServerConfig};
use parity_scale_codec::Encode;

const TIMEOUT: Duration = Duration::from_secs(20);
const ALPN_REGISTRY: &[u8] = b"acurast/registry/1";
const ROLE_REGISTERED: u8 = 0b0001;

/// Every endpoint may use the registry.
struct EveryoneRegistered;

impl RoleSource for EveryoneRegistered {
    fn roles(&self, _: &[u8; 32]) -> Result<u8, String> {
        Ok(ROLE_REGISTERED)
    }
}

/// Answers `P2pApi_roles` with the roles set by the test.
#[derive(Default)]
struct RolesChain(Mutex<HashMap<[u8; 32], Roles>>);

impl RolesChain {
    fn set(&self, id: EndpointId, roles: Roles) {
        self.0.lock().unwrap().insert(*id.as_bytes(), roles);
    }
}

#[async_trait::async_trait]
impl Chain for RolesChain {
    fn account(&self) -> Vec<u8> {
        unreachable!()
    }

    async fn genesis_hash(&self) -> Result<Vec<u8>, PlatformError> {
        unreachable!()
    }

    async fn state_call(&self, method: String, args: Vec<u8>) -> Result<Vec<u8>, PlatformError> {
        assert_eq!(method, "P2pApi_roles");
        let id: [u8; 32] = args.try_into().unwrap();
        Ok(self
            .0
            .lock()
            .unwrap()
            .get(&id)
            .copied()
            .unwrap_or_default()
            .encode())
    }

    async fn register_endpoint(&self, _: Vec<u8>, _: Vec<u8>) -> Result<(), PlatformError> {
        unreachable!()
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
    let endpoint = P2pEndpoint::bind(SecretKey::generate(), vec![relay.clone()], Vec::new())
        .await
        .unwrap();
    tokio::time::timeout(TIMEOUT, endpoint.online())
        .await
        .expect("home relay connection");
    endpoint
}

#[tokio::test]
async fn registered_peers_get_pong_and_others_are_refused() {
    let node = spawn_relay_node().await;
    let chain = Arc::new(RolesChain::default());
    let listener = bind(&node.relay).await;
    let registered = bind(&node.relay).await;
    let unknown = bind(&node.relay).await;
    chain.set(registered.id(), Roles::REGISTERED | Roles::PROCESSOR);
    let _serving = listener.serve_ping(Arc::new(RoleCache::new(chain.clone())));

    tokio::time::timeout(TIMEOUT, async {
        while node.registry.len() < 3 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("records published");

    let report = tokio::time::timeout(TIMEOUT, registered.ping(listener.id()))
        .await
        .expect("ping in time")
        .expect("pong");
    assert!(report.rtt < TIMEOUT);

    let refused = tokio::time::timeout(TIMEOUT, unknown.ping(listener.id()))
        .await
        .expect("refused in time");
    assert!(refused.is_err(), "an unregistered peer got {refused:?}");
}
