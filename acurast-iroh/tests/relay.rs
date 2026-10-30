//! Two endpoints reach each other through a local relay, knowing only the `EndpointId` and the
//! relay URL.

use std::{net::Ipv4Addr, time::Duration};

use acurast_iroh::{EndpointAddr, P2pEndpoint, Relay, SecretKey};
use iroh_relay::server::{RelayConfig, Server, ServerConfig};

const ALPN: &[u8] = b"acurast/test/echo";
const TIMEOUT: Duration = Duration::from_secs(10);

/// A relay over plain HTTP on localhost that admits everyone.
async fn spawn_relay() -> (Server, Relay) {
    let mut config = ServerConfig::default();
    config.relay = Some(RelayConfig::new((Ipv4Addr::LOCALHOST, 0)));
    let server = Server::spawn(config).await.unwrap();
    let url = format!("http://{}", server.http_addr().unwrap());
    let relay = Relay::from_chain(url.as_bytes(), None).unwrap();
    (server, relay)
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
async fn endpoints_reach_each_other_by_id_and_relay_url() {
    let (_server, relay) = spawn_relay().await;
    let listener = bind(&relay).await;
    let dialer = bind(&relay).await;

    // Echo one message back.
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

    // Only the id and the relay URL: no direct addresses.
    let addr = EndpointAddr::new(listener_id).with_relay_url(relay.url.clone());
    let conn = tokio::time::timeout(TIMEOUT, dialer.connect(addr, ALPN))
        .await
        .unwrap()
        .unwrap();
    let (mut send, mut recv) = conn.open_bi().await.unwrap();
    send.write_all(b"hello").await.unwrap();
    send.finish().unwrap();
    assert_eq!(recv.read_to_end(1024).await.unwrap(), b"hello");
    conn.close(0u32.into(), b"done");

    let remote = tokio::time::timeout(TIMEOUT, accept)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        remote,
        dialer.id(),
        "the listener sees the dialer's EndpointId"
    );
}

#[tokio::test]
async fn relays_can_be_replaced() {
    let (_server_a, relay_a) = spawn_relay().await;
    let (_server_b, relay_b) = spawn_relay().await;
    let endpoint = bind(&relay_a).await;

    endpoint.set_relays(vec![relay_b.clone()]).await;
    assert_eq!(endpoint.relays(), vec![relay_b.clone()]);

    // The endpoint is still reachable, now through the new relay.
    let dialer = bind(&relay_b).await;
    let addr = EndpointAddr::new(endpoint.id()).with_relay_url(relay_b.url.clone());
    let accept = tokio::spawn(async move { endpoint.accept().await.unwrap().unwrap().remote_id() });
    let conn = tokio::time::timeout(TIMEOUT, dialer.connect(addr, ALPN))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        tokio::time::timeout(TIMEOUT, accept)
            .await
            .unwrap()
            .unwrap(),
        dialer.id()
    );
    conn.close(0u32.into(), b"done");
}
