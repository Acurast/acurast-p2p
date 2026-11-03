//! Reaches one endpoint from another through the Acurast relays, knowing only its `EndpointId`:
//! the listener publishes its address to the registry of the relay nodes, where the dialer finds
//! it.
//!
//! Relays are given as on chain, `<relay-id>@<relay-url>`, comma-separated. The relays admit only
//! endpoints registered on chain (or allowlisted on the node), so register both keys first. Each
//! run prints its secret and endpoint id; pass `--secret` to reuse a key.
//!
//! ```text
//! cargo run -p acurast-p2p --example p2p-echo -- listen <relays> [--secret <hex>]
//! cargo run -p acurast-p2p --example p2p-echo -- dial <relays> <endpoint-id> [--secret <hex>]
//! ```

use std::time::Duration;

use acurast_p2p::{EndpointAddr, EndpointId, P2pEndpoint, Relay, SecretKey};

const ALPN: &[u8] = b"acurast/example/echo";
const TIMEOUT: Duration = Duration::from_secs(20);

#[tokio::main]
async fn main() -> Result<(), String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let usage = "usage: p2p-echo listen <relays> | dial <relays> <endpoint-id> [--secret <hex>]";
    let (mode, relays) = match args.as_slice() {
        [mode, relays, ..] => (mode.as_str(), parse_relays(relays)?),
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

    let endpoint = P2pEndpoint::bind(secret, relays, vec![ALPN.to_vec()])
        .await
        .map_err(|e| e.to_string())?;
    tokio::time::timeout(TIMEOUT, endpoint.online())
        .await
        .map_err(|_| "the relays did not admit this endpoint (is it registered?)")?;
    let home_relay = endpoint.addr().relay_urls().next().cloned();
    println!("[ok]   connected to home relay {home_relay:?}");

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
            // Only the id: the address comes from the registry.
            let conn = tokio::time::timeout(TIMEOUT, endpoint.connect(EndpointAddr::new(id), ALPN))
                .await
                .map_err(|_| "connection timed out")?
                .map_err(|e| e.to_string())?;
            let (mut send, mut recv) = conn.open_bi().await.map_err(|e| e.to_string())?;
            send.write_all(b"hello from acurast-p2p")
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

/// `<relay-id>@<relay-url>`, comma-separated.
fn parse_relays(arg: &str) -> Result<Vec<Relay>, String> {
    arg.split(',')
        .map(|entry| {
            let (id, url) = entry.split_once('@').ok_or(format!(
                "invalid relay {entry}: expected <relay-id>@<relay-url>"
            ))?;
            let id: [u8; 32] = hex::decode(id.trim_start_matches("0x"))
                .map_err(|e| format!("invalid relay id {id}: {e}"))?
                .try_into()
                .map_err(|_| format!("invalid relay id {id}: expected 32 bytes"))?;
            Relay::from_chain(&id, url.as_bytes(), None).map_err(|e| format!("{entry}: {e}"))
        })
        .collect()
}
