//! Processors accept the assignment notices of relay nodes, found by id through the registry, and
//! refuse those of other peers.

use std::{
    collections::HashMap,
    net::Ipv4Addr,
    sync::{Arc, Mutex},
    time::Duration,
};

use acurast_p2p::{
    ALPN_ASSIGNMENTS, AssignmentNotice, EndpointAddr, EndpointId, P2pEndpoint, Relay, RoleCache,
    Roles, SecretKey,
    platform::{Chain, PlatformError},
};
use acurast_p2p_proto::{
    MAX_FRAME,
    frame::{self, HEADER_LEN},
    messages::AssignmentNoticeReply,
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

/// Sends `notice` to `to` as a relay node does, returning the reply.
async fn send_notice(
    from: &P2pEndpoint,
    to: EndpointId,
    notice: &AssignmentNotice,
) -> Result<AssignmentNoticeReply, String> {
    let conn = from
        .connect(EndpointAddr::new(to), ALPN_ASSIGNMENTS)
        .await
        .map_err(|e| e.to_string())?;
    let (mut send, mut recv) = conn.open_bi().await.map_err(|e| e.to_string())?;
    send.write_all(&frame::encode(notice).unwrap())
        .await
        .map_err(|e| e.to_string())?;
    send.finish().map_err(|e| e.to_string())?;
    let bytes = recv
        .read_to_end(HEADER_LEN + MAX_FRAME)
        .await
        .map_err(|e| e.to_string())?;
    conn.close(0u32.into(), b"done");
    Ok(frame::decode::<AssignmentNoticeReply>(&bytes).unwrap().0)
}

#[tokio::test]
async fn relay_nodes_notify_processors_and_others_are_refused() {
    let node = spawn_relay_node().await;
    let chain = Arc::new(RolesChain::default());
    let processor = bind(&node.relay).await;
    let relay = bind(&node.relay).await;
    let registered = bind(&node.relay).await;
    chain.set(relay.id(), Roles::RELAY);
    chain.set(registered.id(), Roles::REGISTERED | Roles::PROCESSOR);

    let received = Arc::new(Mutex::new(Vec::<AssignmentNotice>::new()));
    let sink = received.clone();
    processor.serve_assignments(
        Arc::new(RoleCache::new(chain.clone())),
        Arc::new(move |notice| sink.lock().unwrap().push(notice)),
    );

    tokio::time::timeout(TIMEOUT, async {
        while node.registry.len() < 3 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("records published");

    let notice = AssignmentNotice {
        block: 42,
        jobs: vec![vec![1, 2, 3]],
    };
    let reply = tokio::time::timeout(TIMEOUT, send_notice(&relay, processor.id(), &notice))
        .await
        .expect("notice in time");
    assert_eq!(reply, Ok(AssignmentNoticeReply::Ok));
    assert_eq!(*received.lock().unwrap(), vec![notice.clone()]);

    let refused = tokio::time::timeout(TIMEOUT, send_notice(&registered, processor.id(), &notice))
        .await
        .expect("refused in time");
    assert!(refused.is_err(), "a non-relay peer got {refused:?}");
    assert_eq!(received.lock().unwrap().len(), 1);
}
