//! Processors send their network report to the relay node of their home relay, with the
//! connections to other processors counted by path.

use std::{
    net::Ipv4Addr,
    sync::{Arc, Mutex},
    time::Duration,
};

use acurast_p2p::{
    ALPN_TELEMETRY, ConnectionStats, EndpointId, P2pEndpoint, Relay, RoleCache, Roles, SecretKey,
    TelemetryPlatform, TelemetryReply, TelemetryReport,
    platform::{Chain, PlatformError},
};
use acurast_p2p_proto::{
    MAX_FRAME,
    frame::{self, HEADER_LEN},
};
use acurast_p2p_registry::{Registry, RoleSource};
use iroh::{
    Endpoint, RelayMode,
    endpoint::{Connection, presets},
    protocol::{AcceptError, ProtocolHandler, Router},
};
use iroh_relay::server::{RelayConfig, Server, ServerConfig};
use parity_scale_codec::Encode;

const TIMEOUT: Duration = Duration::from_secs(20);
const ALPN_REGISTRY: &[u8] = b"acurast/registry/1";
const ROLE_REGISTERED: u8 = 0b0001;

struct EveryoneRegistered;

impl RoleSource for EveryoneRegistered {
    fn roles(&self, _: &[u8; 32]) -> Result<u8, String> {
        Ok(ROLE_REGISTERED)
    }
}

/// Every endpoint is a registered processor.
struct ProcessorsChain;

#[async_trait::async_trait]
impl Chain for ProcessorsChain {
    fn account(&self) -> Vec<u8> {
        unreachable!()
    }

    async fn genesis_hash(&self) -> Result<Vec<u8>, PlatformError> {
        unreachable!()
    }

    async fn state_call(&self, method: String, args: Vec<u8>) -> Result<Vec<u8>, PlatformError> {
        assert_eq!(method, "P2pApi_roles");
        assert_eq!(args.len(), 32);
        Ok((Roles::REGISTERED | Roles::PROCESSOR).encode())
    }

    async fn register_endpoint(&self, _: Vec<u8>, _: Vec<u8>) -> Result<(), PlatformError> {
        unreachable!()
    }
}

/// Stores the reports it gets, as the relay node does, and answers `Ok`.
#[derive(Debug, Clone, Default)]
struct Collector(Arc<Mutex<Vec<(EndpointId, TelemetryReport)>>>);

impl ProtocolHandler for Collector {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        let (mut send, mut recv) = conn.accept_bi().await?;
        let bytes = recv
            .read_to_end(HEADER_LEN + MAX_FRAME)
            .await
            .map_err(AcceptError::from_err)?;
        let (report, _) = frame::decode::<TelemetryReport>(&bytes).unwrap();
        self.0.lock().unwrap().push((conn.remote_id(), report));
        send.write_all(&frame::encode(&TelemetryReply::Ok).unwrap())
            .await
            .map_err(AcceptError::from_err)?;
        send.finish().map_err(AcceptError::from_err)?;
        conn.closed().await;
        Ok(())
    }
}

struct RelayNode {
    _server: Server,
    _router: Router,
    registry: Registry,
    collector: Collector,
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
    let collector = Collector::default();
    let router = Router::builder(endpoint)
        .accept(ALPN_REGISTRY, registry.clone())
        .accept(ALPN_TELEMETRY, collector.clone())
        .spawn();
    RelayNode {
        _server: server,
        _router: router,
        registry,
        collector,
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
async fn reports_reach_the_home_relay_node_with_the_connections_by_path() {
    let node = spawn_relay_node().await;
    let a = bind(&node.relay).await;
    let b = bind(&node.relay).await;
    a.serve_ping(Arc::new(RoleCache::new(Arc::new(ProcessorsChain))));
    tokio::time::timeout(TIMEOUT, async {
        while node.registry.len() < 2 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("records published");

    // Both endpoints run on this machine, so the connection gets a direct path.
    tokio::time::timeout(TIMEOUT, b.ping(a.id()))
        .await
        .expect("ping in time")
        .unwrap();
    // The counting runs in its own task.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let reply = tokio::time::timeout(TIMEOUT, b.send_telemetry("1.2.3 (456)"))
        .await
        .expect("report in time")
        .unwrap();
    assert_eq!(reply, TelemetryReply::Ok);

    let reports = node.collector.0.lock().unwrap().clone();
    assert_eq!(reports.len(), 1);
    let (from, report) = &reports[0];
    assert_eq!(*from, b.id());
    assert_eq!(report.app_version, "1.2.3 (456)");
    assert_eq!(report.platform, TelemetryPlatform::Other);
    assert!(report.home_relay_connected);
    assert_eq!(report.connections.direct, 1);
    assert_eq!(report.connections.relayed, 0);
    assert!(report.connections.avg_time_to_direct_ms.is_some());

    // Counted connections are reported once; the registry connections to the relay node are
    // never counted.
    tokio::time::timeout(TIMEOUT, b.send_telemetry("1.2.3 (456)"))
        .await
        .expect("second report in time")
        .unwrap();
    let reports = node.collector.0.lock().unwrap().clone();
    assert_eq!(reports[1].1.connections, ConnectionStats::default());
}
