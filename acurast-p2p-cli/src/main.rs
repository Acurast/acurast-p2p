//! `acurast-p2p`: command line tool for managers of Acurast processors.
//!
//! The manager talks to its processors over the P2P layer with its own P2P key, kept in a file.
//! Processors accept commands only from a key that their manager registered on chain:
//! `init` creates the key, `register` prepares that registration for the manager to sign in its
//! wallet. The tool never sees the manager's account key.

mod account;
mod chain;
mod key;

use std::{path::PathBuf, process::ExitCode};

use acurast_p2p::SecretKey;
use acurast_p2p_proto::hash::register_proof_message;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "acurast-p2p",
    version,
    about = "Manage your Acurast processors over the P2P layer"
)]
struct Cli {
    /// File with the manager's P2P key [default: ~/.config/acurast-p2p/manager.key].
    #[arg(long, global = true, env = "ACURAST_P2P_KEY")]
    key: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Create the manager's P2P key and store it in the key file.
    Init {
        /// Replace an existing key. The old key stops working once the new one is registered.
        #[arg(long)]
        force: bool,
    },
    /// Prepare the on-chain registration of the P2P key, to sign with the manager's account.
    Register {
        /// The manager's account (SS58 address or 0x hex).
        #[arg(long)]
        manager: String,
        /// RPC URL of a node of the network, e.g. wss://public-rpc.canary.acurast.com.
        #[arg(long, env = "ACURAST_RPC")]
        rpc: String,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<(), String> {
    let key_path = match cli.key {
        Some(path) => path,
        None => key::default_path()?,
    };
    match cli.command {
        Command::Init { force } => {
            let key = key::create(&key_path, force)?;
            println!("Created the P2P key in {}", key_path.display());
            println!("Endpoint id: {}", endpoint_id(&key));
            println!();
            println!(
                "Next: register it on chain with `acurast-p2p register --manager <address> --rpc <url>`."
            );
            Ok(())
        }
        Command::Register { manager, rpc } => {
            let key = key::load(&key_path)?;
            let manager = account::parse(&manager)?;
            let node = chain::Rpc::new(&rpc);
            let genesis = node.genesis_hash().await?;
            let index =
                chain::call_index(&node.metadata().await?, "AcurastP2p", "register_endpoint")?;
            let call = register_call(index, &key, &manager, &genesis);

            println!("Endpoint id: {}", endpoint_id(&key));
            println!();
            println!("Sign and submit this call with the manager's account:");
            println!("0x{}", hex::encode(&call));
            println!();
            println!(
                "In polkadot.js apps: Developer > Extrinsics > Decode, paste the call, then Submit."
            );
            if let Some(link) = polkadot_js_link(&rpc, &call) {
                println!("Or open: {link}");
            }
            Ok(())
        }
    }
}

fn endpoint_id(key: &SecretKey) -> String {
    format!("0x{}", hex::encode(key.public().as_bytes()))
}

/// `AcurastP2p::register_endpoint(id, proof)`: the call index, then the endpoint id and the proof
/// that `manager` holds the key on the network of `genesis`.
fn register_call(
    index: [u8; 2],
    key: &SecretKey,
    manager: &[u8; 32],
    genesis: &[u8; 32],
) -> Vec<u8> {
    // An AccountId32 is SCALE-encoded as its 32 bytes.
    let proof = key.sign(&register_proof_message(genesis, manager));
    let mut call = Vec::with_capacity(2 + 32 + 64);
    call.extend_from_slice(&index);
    call.extend_from_slice(key.public().as_bytes());
    call.extend_from_slice(&proof.to_bytes());
    call
}

/// Link that opens the call in polkadot.js apps on the network of `rpc`, ready to submit. The call
/// is in the fragment, which the browser does not send to the server.
fn polkadot_js_link(rpc: &str, call: &[u8]) -> Option<String> {
    if !(rpc.starts_with("wss://") || rpc.starts_with("ws://")) {
        return None;
    }
    let rpc = rpc.replace(':', "%3A").replace('/', "%2F");
    Some(format!(
        "https://polkadot.js.org/apps/?rpc={rpc}#/extrinsics/decode/0x{}",
        hex::encode(call)
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_call_carries_a_proof_the_chain_accepts() {
        let key = SecretKey::from_bytes(&[7; 32]);
        let (manager, genesis) = ([1; 32], [2; 32]);
        let call = register_call([57, 0], &key, &manager, &genesis);

        assert_eq!(call.len(), 2 + 32 + 64);
        assert_eq!(&call[..2], &[57, 0]);
        assert_eq!(&call[2..34], key.public().as_bytes());
        let proof: [u8; 64] = call[34..].try_into().unwrap();
        let signature = iroh::Signature::from_bytes(&proof);
        let message = register_proof_message(&genesis, &manager);
        assert!(key.public().verify(&message, &signature).is_ok());
        // Bound to the account and the network.
        assert!(
            key.public()
                .verify(&register_proof_message(&genesis, &[3; 32]), &signature)
                .is_err()
        );
        assert!(
            key.public()
                .verify(&register_proof_message(&[4; 32], &manager), &signature)
                .is_err()
        );
    }

    #[test]
    fn polkadot_js_link_only_for_websocket_urls() {
        let link = polkadot_js_link("wss://node.example", &[0xab]).unwrap();
        assert_eq!(
            link,
            "https://polkadot.js.org/apps/?rpc=wss%3A%2F%2Fnode.example#/extrinsics/decode/0xab"
        );
        assert!(polkadot_js_link("https://node.example", &[0xab]).is_none());
    }
}
