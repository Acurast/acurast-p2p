//! Sends a command to several processors at once over the P2P layer and describes the replies.

use std::{sync::Arc, time::Duration};

use acurast_p2p::{
    EndpointId, ManageCommand, ManageError, ManageReply, P2pEndpoint, ProcessorStatus, SecretKey,
};
use tokio::task::JoinSet;

use crate::chain::ManagerChain;

/// How long to wait for the relays to admit the manager's endpoint.
const ONLINE_TIMEOUT: Duration = Duration::from_secs(30);
/// How long to wait for the reply of one processor.
const REPLY_TIMEOUT: Duration = Duration::from_secs(30);

/// A processor to send the command to, with its endpoint if it registered one.
pub struct Target {
    pub account: [u8; 32],
    pub endpoint: Option<[u8; 32]>,
}

/// What came back from one processor.
pub enum Outcome {
    Reply(ManageReply),
    /// The processor has no P2P endpoint on chain.
    NoEndpoint,
    /// No reply within [`REPLY_TIMEOUT`]: offline, asleep or unreachable.
    NoReply,
    /// The connection failed.
    Failed(String),
}

/// Sends `command` to every target in parallel with the manager's `key`, reading the relays from
/// `chain`, and returns the outcomes in the order of `targets`.
pub async fn send(
    key: SecretKey,
    chain: &ManagerChain,
    targets: &[Target],
    command: ManageCommand,
) -> Result<Vec<Outcome>, String> {
    let endpoint = P2pEndpoint::bind(key, Vec::new(), Vec::new())
        .await
        .map_err(|e| format!("cannot start the P2P endpoint: {e}"))?;
    endpoint
        .refresh_relays(chain)
        .await
        .map_err(|e| format!("cannot read the relays from the chain: {e}"))?;
    if endpoint.relays().is_empty() {
        return Err("no relays are listed on chain on this network".into());
    }
    if tokio::time::timeout(ONLINE_TIMEOUT, endpoint.online())
        .await
        .is_err()
    {
        return Err(
            "the relays did not admit this key: register it with `acurast-p2p register`".into(),
        );
    }

    let endpoint = Arc::new(endpoint);
    let mut tasks = JoinSet::new();
    for (i, target) in targets.iter().enumerate() {
        let Some(id) = target.endpoint else { continue };
        let endpoint = endpoint.clone();
        tasks.spawn(async move {
            let outcome = match EndpointId::from_bytes(&id) {
                Err(e) => Outcome::Failed(format!("invalid endpoint id: {e}")),
                Ok(id) => {
                    match tokio::time::timeout(REPLY_TIMEOUT, endpoint.manage(id, command)).await {
                        Ok(Ok(reply)) => Outcome::Reply(reply),
                        Ok(Err(e)) => Outcome::Failed(e.to_string()),
                        Err(_) => Outcome::NoReply,
                    }
                }
            };
            (i, outcome)
        });
    }
    let mut outcomes: Vec<Outcome> = targets.iter().map(|_| Outcome::NoEndpoint).collect();
    while let Some(result) = tasks.join_next().await {
        let (i, outcome) = result.map_err(|e| format!("task failed: {e}"))?;
        outcomes[i] = outcome;
    }
    endpoint.close().await;
    Ok(outcomes)
}

/// One line for the outcome of a processor.
pub fn describe(outcome: &Outcome) -> String {
    match outcome {
        Outcome::Reply(ManageReply::Status(status)) => describe_status(status),
        Outcome::Reply(ManageReply::Accepted) => "accepted".into(),
        Outcome::Reply(ManageReply::Err(error)) => describe_error(*error).into(),
        Outcome::NoEndpoint => "no P2P endpoint (app without P2P, or not started yet)".into(),
        Outcome::NoReply => "no reply (offline, asleep or unreachable)".into(),
        Outcome::Failed(e) => format!("failed: {e}"),
    }
}

fn describe_status(s: &ProcessorStatus) -> String {
    let battery = match (s.battery_percent, s.charging) {
        (Some(level), Some(true)) => format!("battery {level}% charging"),
        (Some(level), _) => format!("battery {level}%"),
        (None, _) => "battery unknown".into(),
    };
    let state = if s.busy { "busy" } else { "idle" };
    format!(
        "{} · {} · {battery} · {state} · up {}",
        s.app_version,
        s.os,
        uptime(s.uptime_secs)
    )
}

fn describe_error(error: ManageError) -> &'static str {
    match error {
        ManageError::Forbidden => "refused: this key is not registered by its manager",
        ManageError::Stale => "refused: the clock of this computer is off by more than a minute",
        ManageError::Busy => "refused: a job is running (use --force)",
        ManageError::Unsupported => "not supported by this processor",
        ManageError::Failed => "failed on the processor",
    }
}

fn uptime(secs: u64) -> String {
    let (days, hours, minutes) = (secs / 86_400, secs % 86_400 / 3600, secs % 3600 / 60);
    match (days, hours) {
        (0, 0) => format!("{minutes}m"),
        (0, _) => format!("{hours}h{minutes:02}m"),
        _ => format!("{days}d{hours:02}h"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn describes_a_status() {
        let status = ProcessorStatus {
            app_version: "1.42.0".into(),
            os: "Android 16".into(),
            battery_percent: Some(80),
            charging: Some(true),
            busy: false,
            uptime_secs: 3 * 3600 + 5 * 60,
        };
        assert_eq!(
            describe(&Outcome::Reply(ManageReply::Status(status))),
            "1.42.0 · Android 16 · battery 80% charging · idle · up 3h05m"
        );
    }

    #[test]
    fn uptime_is_short() {
        assert_eq!(uptime(59), "0m");
        assert_eq!(uptime(3600), "1h00m");
        assert_eq!(uptime(2 * 86_400 + 7 * 3600), "2d07h");
    }
}
