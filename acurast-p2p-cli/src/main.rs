//! `acurast-p2p`: command line tool for managers of Acurast processors.
//!
//! The manager talks to its processors over the P2P layer with its own P2P key, kept in a file.
//! Processors accept commands only from a key that their manager registered on chain:
//! `init` creates the key, `register` prepares that registration for the manager to sign in its
//! wallet. The tool never sees the manager's account key. `processors` lists the manager's
//! processors; `status`, `reboot` and `restart-app` send them commands.

mod account;
mod chain;
mod key;
mod manage;

use std::{io::Write, path::PathBuf, process::ExitCode};

use acurast_p2p::{ManageCommand, SecretKey};
use acurast_p2p_proto::hash::register_proof_message;
use clap::{Args, Parser, Subcommand};

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
    Register(Manager),
    /// List the manager's processors and their P2P endpoints.
    Processors(Manager),
    /// Ask processors for their state: app version, OS, battery, busy, uptime.
    Status(Targets),
    /// Restart the device of processors (Android with device owner rights only).
    Reboot(Disruptive),
    /// Restart the processor app.
    RestartApp(Disruptive),
}

/// The processors a command goes to.
#[derive(Args)]
struct Targets {
    /// A processor of the manager (SS58 address or 0x hex).
    #[arg(required_unless_present = "all", conflicts_with = "all")]
    processor: Option<String>,
    /// All processors of the manager.
    #[arg(long)]
    all: bool,
    #[command(flatten)]
    manager: Manager,
}

/// A command that interrupts the processors.
#[derive(Args)]
struct Disruptive {
    #[command(flatten)]
    targets: Targets,
    /// Run it even while a job is running on the processor.
    #[arg(long)]
    force: bool,
    /// Do not ask for confirmation with --all.
    #[arg(long, short)]
    yes: bool,
}

/// The manager and the network it is on.
#[derive(Args)]
struct Manager {
    /// The manager's account (SS58 address or 0x hex).
    #[arg(long, env = "ACURAST_MANAGER")]
    manager: String,
    /// RPC URL of a node of the network, e.g. wss://public-rpc.canary.acurast.com.
    #[arg(long, env = "ACURAST_RPC")]
    rpc: String,
}

impl Manager {
    fn connect(&self) -> Result<([u8; 32], chain::Rpc), String> {
        Ok((account::parse(&self.manager)?, chain::Rpc::new(&self.rpc)))
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    // Logs of the P2P library only with RUST_LOG (e.g. RUST_LOG=debug), on stderr.
    if std::env::var_os("RUST_LOG").is_some() {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_writer(std::io::stderr)
            .init();
    }
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
        Command::Register(args) => {
            let key = key::load(&key_path)?;
            let (manager, node) = args.connect()?;
            let id = *key.public().as_bytes();
            match node.endpoint_owner(&id).await {
                Ok(Some(owner)) if owner == manager => {
                    println!("Endpoint id: {}", endpoint_id(&key));
                    println!("Already registered for this manager: nothing to do.");
                    return Ok(());
                }
                Ok(Some(owner)) => {
                    return Err(format!(
                        "this key is registered by another account ({}): create a new one with `acurast-p2p init --force`",
                        account::to_ss58(&owner)
                    ));
                }
                // Not registered, or a network without the P2P layer: the metadata check says so.
                Ok(None) | Err(_) => {}
            }
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
            if let Some(link) = polkadot_js_link(&args.rpc, &call) {
                println!("Or open: {link}");
            }
            Ok(())
        }
        Command::Processors(args) => {
            let (manager, node) = args.connect()?;
            let processors = processors(&node, &manager).await?;
            if processors.is_empty() {
                println!("No processors paired with this manager.");
                return Ok(());
            }
            println!("{:<50} P2P ENDPOINT", "PROCESSOR");
            for p in &processors {
                let endpoint = match p.endpoint {
                    Some(id) => format!("0x{}", hex::encode(id)),
                    None => "none (app without P2P, or not started yet)".into(),
                };
                println!("{:<50} {endpoint}", account::to_ss58(&p.account));
            }
            Ok(())
        }
        Command::Status(targets) => {
            send_command(&key_path, &targets, ManageCommand::Status, None).await
        }
        Command::Reboot(args) => {
            let command = ManageCommand::Reboot { force: args.force };
            let confirm = (!args.yes).then_some("Reboot");
            send_command(&key_path, &args.targets, command, confirm).await
        }
        Command::RestartApp(args) => {
            let command = ManageCommand::RestartApp { force: args.force };
            let confirm = (!args.yes).then_some("Restart the app on");
            send_command(&key_path, &args.targets, command, confirm).await
        }
    }
}

/// Sends `command` to the processors of `targets` and prints a line per processor. Commands that
/// interrupt the processors ask for confirmation first when they go to all of them.
async fn send_command(
    key_path: &std::path::Path,
    targets: &Targets,
    command: ManageCommand,
    confirm: Option<&str>,
) -> Result<(), String> {
    let key = key::load(key_path)?;
    let (manager, node) = targets.manager.connect()?;
    let list: Vec<manage::Target> = match &targets.processor {
        Some(processor) => {
            let account = account::parse(processor)?;
            let endpoint = node.endpoints(&account).await?.first().copied();
            vec![manage::Target { account, endpoint }]
        }
        None => processors(&node, &manager)
            .await?
            .into_iter()
            .map(|p| manage::Target {
                account: p.account,
                endpoint: p.endpoint,
            })
            .collect(),
    };
    if list.is_empty() {
        println!("No processors paired with this manager.");
        return Ok(());
    }
    if let Some(action) = confirm
        && targets.all
        && !ask(&format!("{action} {} processors?", list.len()))?
    {
        return Err("cancelled".into());
    }
    let chain = chain::ManagerChain {
        rpc: node,
        account: manager,
    };
    let outcomes = manage::send(key, &chain, &list, command).await?;
    println!("{:<50} RESULT", "PROCESSOR");
    for (target, outcome) in list.iter().zip(&outcomes) {
        println!(
            "{:<50} {}",
            account::to_ss58(&target.account),
            manage::describe(outcome)
        );
    }
    Ok(())
}

/// Asks `question` on the terminal; only "y" or "yes" confirms.
fn ask(question: &str) -> Result<bool, String> {
    print!("{question} [y/N] ");
    std::io::stdout().flush().map_err(|e| e.to_string())?;
    let mut answer = String::new();
    std::io::stdin()
        .read_line(&mut answer)
        .map_err(|e| e.to_string())?;
    Ok(matches!(answer.trim().to_lowercase().as_str(), "y" | "yes"))
}

/// A processor of the manager, with its P2P endpoint if it registered one.
struct Processor {
    account: [u8; 32],
    endpoint: Option<[u8; 32]>,
}

async fn processors(node: &chain::Rpc, manager: &[u8; 32]) -> Result<Vec<Processor>, String> {
    let id = node
        .manager_id(manager)
        .await?
        .ok_or("this account is not a manager on this network")?;
    let mut processors = Vec::new();
    for account in node.managed_processors(id).await? {
        // A processor has a single endpoint.
        let endpoint = node.endpoints(&account).await?.first().copied();
        processors.push(Processor { account, endpoint });
    }
    Ok(processors)
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
