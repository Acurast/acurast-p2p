//! Sends a manager command to one of the manager's processors, knowing only its `EndpointId`.
//!
//! The processor accepts commands only from an endpoint registered on chain by its manager, so
//! register the manager's key first: `proof` prints the endpoint id and the proof for
//! `acurastP2p.registerEndpoint(id, proof)`, to submit with the manager's account (e.g. from
//! polkadot.js apps). Relays are given as on chain, `<relay-id>@<relay-url>`, comma-separated.
//!
//! ```text
//! cargo run -p acurast-p2p --example p2p-manage -- proof <manager-account-hex> <genesis-hash-hex> [--secret <hex>]
//! cargo run -p acurast-p2p --example p2p-manage -- send <relays> <processor-endpoint-id> status|reboot|restart-app [--force] --secret <hex>
//! ```

use std::time::Duration;

use acurast_p2p::{EndpointId, ManageCommand, P2pEndpoint, Relay, SecretKey};
use acurast_p2p_proto::hash::register_proof_message;

const TIMEOUT: Duration = Duration::from_secs(30);
const USAGE: &str = "usage: p2p-manage proof <manager-account-hex> <genesis-hash-hex> [--secret <hex>]\n       \
                     p2p-manage send <relays> <processor-endpoint-id> status|reboot|restart-app [--force] --secret <hex>";

#[tokio::main]
async fn main() -> Result<(), String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let secret = match flag_value(&args, "--secret")? {
        Some(hex) => Some(SecretKey::from_bytes(&bytes32(&hex, "--secret")?)),
        None => None,
    };
    match args.first().map(String::as_str) {
        Some("proof") => proof(&args, secret.unwrap_or_else(SecretKey::generate)),
        Some("send") => send(&args, secret.ok_or(USAGE)?).await,
        _ => Err(USAGE.into()),
    }
}

/// Prints the key, its endpoint id and the proof that registers it for `account`.
fn proof(args: &[String], secret: SecretKey) -> Result<(), String> {
    let [_, account, genesis, ..] = args else {
        return Err(USAGE.into());
    };
    let account = hex::decode(account.trim_start_matches("0x"))
        .map_err(|e| format!("invalid account: {e}"))?;
    let genesis = bytes32(genesis, "genesis hash")?;
    let proof = secret.sign(&register_proof_message(&genesis, &account));
    println!("secret:      {}", hex::encode(secret.to_bytes()));
    println!("endpoint id: 0x{}", hex::encode(secret.public().as_bytes()));
    println!("proof:       0x{}", hex::encode(proof.to_bytes()));
    Ok(())
}

/// Sends the command to the processor and prints its reply.
async fn send(args: &[String], secret: SecretKey) -> Result<(), String> {
    let [_, relays, processor, command, ..] = args else {
        return Err(USAGE.into());
    };
    let force = args.iter().any(|a| a == "--force");
    let command = match command.as_str() {
        "status" => ManageCommand::Status,
        "reboot" => ManageCommand::Reboot { force },
        "restart-app" => ManageCommand::RestartApp { force },
        _ => return Err(USAGE.into()),
    };
    let processor = EndpointId::from_bytes(&bytes32(processor, "processor endpoint id")?)
        .map_err(|e| e.to_string())?;

    println!("endpoint id: {}", hex::encode(secret.public().as_bytes()));
    let endpoint = P2pEndpoint::bind(secret, parse_relays(relays)?, Vec::new())
        .await
        .map_err(|e| e.to_string())?;
    tokio::time::timeout(TIMEOUT, endpoint.online())
        .await
        .map_err(|_| "the relays did not admit this endpoint (is it registered?)")?;

    let reply = tokio::time::timeout(TIMEOUT, endpoint.manage(processor, command))
        .await
        .map_err(|_| "no reply in time")?
        .map_err(|e| e.to_string())?;
    println!("reply:       {reply:?}");
    endpoint.close().await;
    Ok(())
}

fn flag_value(args: &[String], flag: &str) -> Result<Option<String>, String> {
    match args.iter().position(|a| a == flag) {
        Some(i) => args.get(i + 1).cloned().map(Some).ok_or(USAGE.into()),
        None => Ok(None),
    }
}

fn bytes32(hex: &str, what: &str) -> Result<[u8; 32], String> {
    hex::decode(hex.trim_start_matches("0x"))
        .map_err(|e| format!("invalid {what}: {e}"))?
        .try_into()
        .map_err(|_| format!("invalid {what}: expected 32 bytes"))
}

/// `<relay-id>@<relay-url>`, comma-separated.
fn parse_relays(arg: &str) -> Result<Vec<Relay>, String> {
    arg.split(',')
        .map(|entry| {
            let (id, url) = entry.split_once('@').ok_or(format!(
                "invalid relay {entry}: expected <relay-id>@<relay-url>"
            ))?;
            Relay::from_chain(&bytes32(id, "relay id")?, url.as_bytes(), None)
                .map_err(|e| format!("{entry}: {e}"))
        })
        .collect()
}
