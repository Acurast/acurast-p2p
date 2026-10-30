//! Role cache and role check of incoming connections.

use std::{
    collections::HashMap,
    net::Ipv4Addr,
    sync::{Arc, Mutex},
    time::Duration,
};

use acurast_p2p::{
    EndpointAddr, EndpointId, P2pEndpoint, ROLE_CACHE_NEGATIVE_TTL, ROLE_CACHE_TTL, Relay,
    RoleCache, Roles, SecretKey,
    platform::{Chain, PlatformError},
};
use acurast_p2p_proto::ERR_ROLE;
use iroh::endpoint::ConnectionError;
use iroh_relay::server::{RelayConfig, Server, ServerConfig};
use parity_scale_codec::Encode;

/// Answers `P2pApi_roles` from memory and counts the calls.
#[derive(Default)]
struct RolesChain {
    roles: Mutex<HashMap<[u8; 32], Roles>>,
    calls: Mutex<usize>,
}

impl RolesChain {
    fn set(&self, id: EndpointId, roles: Roles) {
        self.roles.lock().unwrap().insert(*id.as_bytes(), roles);
    }

    fn calls(&self) -> usize {
        *self.calls.lock().unwrap()
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
        *self.calls.lock().unwrap() += 1;
        let id: [u8; 32] = args
            .try_into()
            .expect("the argument is the 32-byte EndpointId");
        Ok(self
            .roles
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

#[test]
fn role_bits_match_the_pallet() {
    // An endpoint of a manager, as answered by the Canary runtime.
    let roles = Roles(0x05);
    assert!(roles.contains(Roles::REGISTERED | Roles::MANAGER));
    assert!(!roles.intersects(Roles::PROCESSOR | Roles::RELAY));
}

#[tokio::test(start_paused = true)]
async fn roles_are_cached_for_the_ttl() {
    let chain = Arc::new(RolesChain::default());
    let id = SecretKey::generate().public();
    chain.set(id, Roles::REGISTERED | Roles::PROCESSOR);
    let cache = RoleCache::new(chain.clone());

    assert_eq!(
        cache.roles(&id).await.unwrap(),
        Roles::REGISTERED | Roles::PROCESSOR
    );
    // Removed on chain: the cached roles are used until the TTL expires.
    chain.set(id, Roles::empty());
    tokio::time::advance(ROLE_CACHE_TTL - Duration::from_secs(1)).await;
    assert_eq!(
        cache.roles(&id).await.unwrap(),
        Roles::REGISTERED | Roles::PROCESSOR
    );
    assert_eq!(chain.calls(), 1);

    tokio::time::advance(Duration::from_secs(2)).await;
    assert_eq!(cache.roles(&id).await.unwrap(), Roles::empty());
    assert_eq!(chain.calls(), 2);
}

#[tokio::test(start_paused = true)]
async fn missing_roles_are_cached_shorter() {
    let chain = Arc::new(RolesChain::default());
    let id = SecretKey::generate().public();
    let cache = RoleCache::new(chain.clone());

    assert_eq!(cache.roles(&id).await.unwrap(), Roles::empty());
    // Registered on chain meanwhile: noticed after the negative TTL.
    chain.set(id, Roles::REGISTERED);
    assert_eq!(cache.roles(&id).await.unwrap(), Roles::empty());
    tokio::time::advance(ROLE_CACHE_NEGATIVE_TTL + Duration::from_secs(1)).await;
    assert_eq!(cache.roles(&id).await.unwrap(), Roles::REGISTERED);
    assert_eq!(chain.calls(), 2);
}

#[tokio::test(start_paused = true)]
async fn invalidate_forgets_the_cached_roles() {
    let chain = Arc::new(RolesChain::default());
    let id = SecretKey::generate().public();
    chain.set(id, Roles::REGISTERED);
    let cache = RoleCache::new(chain.clone());

    cache.roles(&id).await.unwrap();
    chain.set(id, Roles::RELAY);
    cache.invalidate(&id).await;
    assert_eq!(cache.roles(&id).await.unwrap(), Roles::RELAY);
}

const ALPN: &[u8] = b"acurast/test/processors-only";
const TIMEOUT: Duration = Duration::from_secs(10);

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
        .unwrap();
    endpoint
}

/// Connects to `server` and waits for the connection to end; returns how it was closed.
async fn dial(dialer: &P2pEndpoint, server: EndpointId, relay: &Relay) -> ConnectionError {
    let addr = EndpointAddr::new(server).with_relay_url(relay.url.clone());
    let conn = tokio::time::timeout(TIMEOUT, dialer.connect(addr, ALPN))
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(TIMEOUT, conn.closed()).await.unwrap()
}

#[tokio::test]
async fn processor_only_service_refuses_other_roles() {
    let mut config = ServerConfig::default();
    config.relay = Some(RelayConfig::new((Ipv4Addr::LOCALHOST, 0)));
    let relay_server = Server::spawn(config).await.unwrap();
    let relay = Relay::from_chain(
        format!("http://{}", relay_server.http_addr().unwrap()).as_bytes(),
        None,
    )
    .unwrap();

    let server = bind(&relay).await;
    let processor = bind(&relay).await;
    let registered = bind(&relay).await;
    let chain = Arc::new(RolesChain::default());
    chain.set(processor.id(), Roles::REGISTERED | Roles::PROCESSOR);
    chain.set(registered.id(), Roles::REGISTERED);
    let cache = RoleCache::new(chain);

    // The service accepts processors only and closes accepted connections normally.
    let server_id = server.id();
    tokio::spawn(async move {
        while let Some(Ok(conn)) = server.accept().await {
            if cache.authorize(&conn, Roles::PROCESSOR).await.is_some() {
                conn.close(0u32.into(), b"bye");
            }
        }
    });

    match dial(&registered, server_id, &relay).await {
        ConnectionError::ApplicationClosed(close) => {
            assert_eq!(close.error_code, ERR_ROLE.into(), "closed for its role")
        }
        other => panic!("unexpected close: {other:?}"),
    }
    match dial(&processor, server_id, &relay).await {
        ConnectionError::ApplicationClosed(close) => {
            assert_eq!(
                close.error_code,
                0u32.into(),
                "accepted, then closed normally"
            )
        }
        other => panic!("unexpected close: {other:?}"),
    }
}
