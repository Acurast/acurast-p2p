//! Processors run the commands of their own manager, found by id through the registry, and refuse
//! those of other managers, of peers without the manager role and old commands.

use std::{
    collections::HashMap,
    net::Ipv4Addr,
    sync::{Arc, Mutex},
    time::Duration,
};

use acurast_p2p::{
    ALPN_MANAGE, EndpointAddr, EndpointId, ManageCommand, ManageError, ManageHandler, ManageReply,
    ManageRequest, P2pEndpoint, ProcessorStatus, Relay, RoleCache, Roles, SecretKey,
    platform::{Chain, PlatformError},
};
use acurast_p2p_proto::{
    MAX_FRAME,
    frame::{self, HEADER_LEN},
};
use acurast_p2p_registry::{Registry, RoleSource};
use iroh::{Endpoint, RelayMode, endpoint::presets, protocol::Router};
use iroh_relay::server::{RelayConfig, Server, ServerConfig};
use parity_scale_codec::Encode;

const TIMEOUT: Duration = Duration::from_secs(20);
const ALPN_REGISTRY: &[u8] = b"acurast/registry/1";
const ROLE_REGISTERED: u8 = 0b0001;
const MANAGER: [u8; 32] = [7; 32];
const OTHER_MANAGER: [u8; 32] = [8; 32];

/// Every endpoint may use the registry.
struct EveryoneRegistered;

impl RoleSource for EveryoneRegistered {
    fn roles(&self, _: &[u8; 32]) -> Result<u8, String> {
        Ok(ROLE_REGISTERED)
    }
}

/// Roles and owner account of each endpoint.
type Endpoints = HashMap<[u8; 32], (Roles, [u8; 32])>;

/// Answers `P2pApi_roles` and `P2pApi_endpoint_owner` with the values set by the test.
#[derive(Default)]
struct TestChain(Mutex<Endpoints>);

impl TestChain {
    fn set(&self, id: EndpointId, roles: Roles, owner: [u8; 32]) {
        self.0
            .lock()
            .unwrap()
            .insert(*id.as_bytes(), (roles, owner));
    }
}

#[async_trait::async_trait]
impl Chain for TestChain {
    fn account(&self) -> Vec<u8> {
        unreachable!()
    }

    async fn genesis_hash(&self) -> Result<Vec<u8>, PlatformError> {
        unreachable!()
    }

    async fn state_call(&self, method: String, args: Vec<u8>) -> Result<Vec<u8>, PlatformError> {
        let id: [u8; 32] = args.try_into().unwrap();
        let entry = self.0.lock().unwrap().get(&id).copied();
        Ok(match method.as_str() {
            "P2pApi_roles" => entry.map(|(roles, _)| roles).unwrap_or_default().encode(),
            "P2pApi_endpoint_owner" => entry.map(|(_, owner)| owner).encode(),
            _ => unreachable!(),
        })
    }

    async fn register_endpoint(&self, _: Vec<u8>, _: Vec<u8>) -> Result<(), PlatformError> {
        unreachable!()
    }
}

/// A processor managed by [`MANAGER`] that is busy: unforced restarts are refused.
#[derive(Default)]
struct BusyProcessor {
    executed: Mutex<Vec<ManageCommand>>,
}

fn status() -> ProcessorStatus {
    ProcessorStatus {
        app_version: "1.2.3".into(),
        os: "Android 16".into(),
        battery_percent: Some(80),
        charging: Some(false),
        busy: true,
        uptime_secs: 60,
    }
}

#[async_trait::async_trait]
impl ManageHandler for BusyProcessor {
    async fn manager(&self) -> Option<Vec<u8>> {
        Some(MANAGER.to_vec())
    }

    async fn prepare(&self, command: ManageCommand) -> ManageReply {
        match command {
            ManageCommand::Status => ManageReply::Status(status()),
            ManageCommand::Reboot { force: true } | ManageCommand::RestartApp { force: true } => {
                ManageReply::Accepted
            }
            _ => ManageReply::Err(ManageError::Busy),
        }
    }

    async fn execute(&self, command: ManageCommand) {
        self.executed.lock().unwrap().push(command);
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

/// Sends `request` as it is, e.g. with an old timestamp, returning the reply.
async fn send_raw(
    from: &P2pEndpoint,
    to: EndpointId,
    request: &ManageRequest,
) -> Result<ManageReply, String> {
    let conn = from
        .connect(EndpointAddr::new(to), ALPN_MANAGE)
        .await
        .map_err(|e| e.to_string())?;
    let (mut send, mut recv) = conn.open_bi().await.map_err(|e| e.to_string())?;
    send.write_all(&frame::encode(request).unwrap())
        .await
        .map_err(|e| e.to_string())?;
    send.finish().map_err(|e| e.to_string())?;
    let bytes = recv
        .read_to_end(HEADER_LEN + MAX_FRAME)
        .await
        .map_err(|e| e.to_string())?;
    conn.close(0u32.into(), b"done");
    Ok(frame::decode::<ManageReply>(&bytes).unwrap().0)
}

async fn manage(from: &P2pEndpoint, to: EndpointId, command: ManageCommand) -> ManageReply {
    tokio::time::timeout(TIMEOUT, from.manage(to, command))
        .await
        .expect("reply in time")
        .expect("reply")
}

#[tokio::test]
async fn processors_run_the_commands_of_their_own_manager_only() {
    let node = spawn_relay_node().await;
    let chain = Arc::new(TestChain::default());
    let processor = bind(&node.relay).await;
    let manager = bind(&node.relay).await;
    let other_manager = bind(&node.relay).await;
    let not_a_manager = bind(&node.relay).await;
    chain.set(manager.id(), Roles::REGISTERED | Roles::MANAGER, MANAGER);
    chain.set(
        other_manager.id(),
        Roles::REGISTERED | Roles::MANAGER,
        OTHER_MANAGER,
    );
    chain.set(
        not_a_manager.id(),
        Roles::REGISTERED | Roles::PROCESSOR,
        MANAGER,
    );

    let handler = Arc::new(BusyProcessor::default());
    processor.serve_manage(
        Arc::new(RoleCache::new(chain.clone())),
        chain.clone(),
        handler.clone(),
    );

    tokio::time::timeout(TIMEOUT, async {
        while node.registry.len() < 4 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("records published");

    // The own manager.
    let pid = processor.id();
    assert_eq!(
        manage(&manager, pid, ManageCommand::Status).await,
        ManageReply::Status(status())
    );
    assert_eq!(
        manage(&manager, pid, ManageCommand::RestartApp { force: false }).await,
        ManageReply::Err(ManageError::Busy)
    );
    assert_eq!(
        manage(&manager, pid, ManageCommand::Reboot { force: true }).await,
        ManageReply::Accepted
    );
    tokio::time::timeout(TIMEOUT, async {
        while handler.executed.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("accepted command executed");
    assert_eq!(
        *handler.executed.lock().unwrap(),
        vec![ManageCommand::Reboot { force: true }]
    );

    // An old command of the own manager.
    let old = ManageRequest {
        sent_at_ms: 1,
        command: ManageCommand::Reboot { force: true },
    };
    assert_eq!(
        tokio::time::timeout(TIMEOUT, send_raw(&manager, pid, &old))
            .await
            .expect("reply in time"),
        Ok(ManageReply::Err(ManageError::Stale))
    );

    // Another manager.
    assert_eq!(
        manage(&other_manager, pid, ManageCommand::Reboot { force: true }).await,
        ManageReply::Err(ManageError::Forbidden)
    );

    // A peer without the manager role, even if registered by the manager's account.
    let refused = tokio::time::timeout(
        TIMEOUT,
        not_a_manager.manage(pid, ManageCommand::Reboot { force: true }),
    )
    .await
    .expect("refused in time");
    assert!(
        refused.is_err(),
        "a peer without the manager role got {refused:?}"
    );

    assert_eq!(handler.executed.lock().unwrap().len(), 1);
}
