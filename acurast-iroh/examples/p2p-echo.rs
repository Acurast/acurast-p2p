//! Reaches one endpoint from another through an Acurast relay, knowing only its `EndpointId`.
//!
//! The relay admits only endpoints registered on chain (or allowlisted on the node), so register
//! both keys first. Each run prints its secret and endpoint id; pass `--secret` to reuse a key.
//!
//! ```text
//! cargo run -p acurast-iroh --example p2p-echo -- listen http://127.0.0.1:3340 [--secret <hex>]
//! cargo run -p acurast-iroh --example p2p-echo -- dial http://127.0.0.1:3340 <endpoint-id> [--secret <hex>]
//! ```

use std::time::Duration;

use acurast_iroh::{EndpointAddr, EndpointId, P2pEndpoint, Relay, SecretKey};

const ALPN: &[u8] = b"acurast/example/echo";
const TIMEOUT: Duration = Duration::from_secs(20);

#[tokio::main]
async fn main() -> Result<(), String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let usage =
        "usage: p2p-echo listen <relay-url> | dial <relay-url> <endpoint-id> [--secret <hex>]";
    let (mode, relay_url) = match args.as_slice() {
        [mode, relay_url, ..] => (mode.as_str(), relay_url.as_str()),
        _ => return Err(usage.into()),
    };

    let secret = match args.iter().position(|a| a == "--secret") {
        Some(i) => {
            let hex = args.get(i + 1).ok_or(usage)?;
            let bytes: [u8; 32] = hex::decode(hex.trim_start_matches("0x"))
                .map_err(|e| format!("invalid --secret: {e}"))?
                .try_into()
                .map_err(|_| "invalid --secret: expected 32 bytes")?;
            SecretKey::from_bytes(&bytes)
        }
        None => SecretKey::generate(),
    };
    println!("secret:      {}", hex::encode(secret.to_bytes()));
    println!("endpoint id: {}", hex::encode(secret.public().as_bytes()));

    let relay = Relay::from_chain(relay_url.as_bytes(), None).map_err(|e| e.to_string())?;
    let endpoint = P2pEndpoint::bind(secret, vec![relay.clone()], vec![ALPN.to_vec()])
        .await
        .map_err(|e| e.to_string())?;
    tokio::time::timeout(TIMEOUT, endpoint.online())
        .await
        .map_err(|_| "the relay did not admit this endpoint (is it registered?)")?;
    println!("[ok]   connected to relay {}", relay.url);

    match mode {
        "listen" => loop {
            let Some(conn) = endpoint.accept().await else {
                return Ok(());
            };
            let conn = conn.map_err(|e| e.to_string())?;
            println!(
                "[ok]   connection from {}",
                hex::encode(conn.remote_id().as_bytes())
            );
            let (mut send, mut recv) = conn.accept_bi().await.map_err(|e| e.to_string())?;
            let msg = recv.read_to_end(1024).await.map_err(|e| e.to_string())?;
            send.write_all(&msg).await.map_err(|e| e.to_string())?;
            send.finish().map_err(|e| e.to_string())?;
            conn.closed().await;
        },
        "dial" => {
            let target = args.get(2).ok_or(usage)?;
            let id: [u8; 32] = hex::decode(target.trim_start_matches("0x"))
                .map_err(|e| format!("invalid endpoint id: {e}"))?
                .try_into()
                .map_err(|_| "invalid endpoint id: expected 32 bytes")?;
            let id = EndpointId::from_bytes(&id).map_err(|e| e.to_string())?;
            let addr = EndpointAddr::new(id).with_relay_url(relay.url.clone());
            let conn = tokio::time::timeout(TIMEOUT, endpoint.connect(addr, ALPN))
                .await
                .map_err(|_| "connection timed out")?
                .map_err(|e| e.to_string())?;
            let (mut send, mut recv) = conn.open_bi().await.map_err(|e| e.to_string())?;
            send.write_all(b"hello from acurast-iroh")
                .await
                .map_err(|e| e.to_string())?;
            send.finish().map_err(|e| e.to_string())?;
            let reply = recv.read_to_end(1024).await.map_err(|e| e.to_string())?;
            println!("[ok]   echo: {}", String::from_utf8_lossy(&reply));
            conn.close(0u32.into(), b"done");
            endpoint.close().await;
            Ok(())
        }
        _ => Err(usage.into()),
    }
}
