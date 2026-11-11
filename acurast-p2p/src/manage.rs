//! Commands of a manager to its own processors: status, reboot, restart of the app.
//!
//! The connection authenticates the caller's `EndpointId`. A processor accepts a command only from
//! an endpoint with the manager role that was registered by its own manager, and only if the
//! command is recent.

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub use acurast_p2p_proto::{
    ALPN_MANAGE,
    messages::{ManageCommand, ManageError, ManageReply, ManageRequest, ProcessorStatus},
};
use acurast_p2p_proto::{
    ERR_PROTOCOL, MANAGE_MAX_AGE_SECS, MAX_FRAME, MAX_STATUS_TEXT_LEN,
    frame::{self, HEADER_LEN},
};
use iroh::{EndpointAddr, EndpointId, endpoint::Connection};

use crate::{Error, P2pEndpoint, RoleCache, Roles, chain, platform::Chain};

/// How long a handled connection waits for the manager to close it.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(10);

/// What the processor does with the commands of its manager.
#[async_trait::async_trait]
pub trait ManageHandler: Send + Sync {
    /// SCALE-encoded account of this processor's manager, `None` while it is not paired.
    async fn manager(&self) -> Option<Vec<u8>>;
    /// Answer to `command`: the status, [`ManageReply::Accepted`] if it will run, or why not.
    async fn prepare(&self, command: ManageCommand) -> ManageReply;
    /// Runs a command answered with [`ManageReply::Accepted`], once the manager got the reply.
    async fn execute(&self, command: ManageCommand);
}

impl P2pEndpoint {
    /// Accepts the commands of this processor's manager and passes them to `handler`, until the
    /// endpoint closes. Commands of other peers are refused.
    pub fn serve_manage(
        &self,
        roles: Arc<RoleCache>,
        chain: Arc<dyn Chain>,
        handler: Arc<dyn ManageHandler>,
    ) {
        self.serve(
            ALPN_MANAGE,
            Arc::new(move |conn| {
                let roles = roles.clone();
                let chain = chain.clone();
                let handler = handler.clone();
                Box::pin(
                    async move { receive(&conn, &roles, chain.as_ref(), handler.as_ref()).await },
                )
            }),
        );
    }

    /// Sends `command` to the processor `id`, whose address comes from the registry, and returns
    /// its reply.
    pub async fn manage(
        &self,
        id: EndpointId,
        command: ManageCommand,
    ) -> Result<ManageReply, Error> {
        let manage_error = |e: &dyn std::fmt::Display| Error::Manage(e.to_string());
        let conn = self.connect(EndpointAddr::new(id), ALPN_MANAGE).await?;
        let request = ManageRequest {
            sent_at_ms: now_ms(),
            command,
        };
        let reply = async {
            let (mut send, mut recv) = conn.open_bi().await.map_err(|e| manage_error(&e))?;
            let bytes = frame::encode(&request).map_err(|e| manage_error(&e))?;
            send.write_all(&bytes).await.map_err(|e| manage_error(&e))?;
            send.finish().map_err(|e| manage_error(&e))?;
            let bytes = recv
                .read_to_end(HEADER_LEN + MAX_FRAME)
                .await
                .map_err(|e| manage_error(&e))?;
            frame::decode::<ManageReply>(&bytes)
                .map(|(reply, _)| reply)
                .map_err(|e| manage_error(&e))
        }
        .await;
        conn.close(0u32.into(), b"done");
        reply
    }
}

async fn receive(
    conn: &Connection,
    roles: &RoleCache,
    chain: &dyn Chain,
    handler: &dyn ManageHandler,
) {
    if roles.authorize(conn, Roles::MANAGER).await.is_none() {
        return;
    }
    let Ok((mut send, mut recv)) = conn.accept_bi().await else {
        return;
    };
    let Ok(bytes) = recv.read_to_end(HEADER_LEN + MAX_FRAME).await else {
        conn.close(ERR_PROTOCOL.into(), b"protocol");
        return;
    };
    let request = match frame::decode::<ManageRequest>(&bytes) {
        Ok((request, used)) if used == bytes.len() => request,
        _ => {
            conn.close(ERR_PROTOCOL.into(), b"protocol");
            return;
        }
    };

    let reply = answer(conn.remote_id(), &request, now_ms(), chain, handler).await;
    if let Ok(bytes) = frame::encode(&reply) {
        let _ = send.write_all(&bytes).await;
        let _ = send.finish();
    }
    tracing::debug!(
        "manager command {:?} from {}: {:?}",
        request.command,
        conn.remote_id(),
        reply
    );
    // The manager closes the connection once it has read the reply, so a reboot cannot cut it.
    let _ = tokio::time::timeout(CLOSE_TIMEOUT, conn.closed()).await;
    if reply == ManageReply::Accepted {
        handler.execute(request.command).await;
    }
}

/// The reply to `request` of the peer `from`.
async fn answer(
    from: EndpointId,
    request: &ManageRequest,
    now_ms: u64,
    chain: &dyn Chain,
    handler: &dyn ManageHandler,
) -> ManageReply {
    if !is_own_manager(from, chain, handler).await {
        return ManageReply::Err(ManageError::Forbidden);
    }
    if !is_recent(request.sent_at_ms, now_ms) {
        return ManageReply::Err(ManageError::Stale);
    }
    match handler.prepare(request.command).await {
        ManageReply::Status(status) => ManageReply::Status(bounded(status)),
        reply => reply,
    }
}

/// Whether the endpoint `from` was registered by this processor's manager.
async fn is_own_manager(from: EndpointId, chain: &dyn Chain, handler: &dyn ManageHandler) -> bool {
    let (Some(manager), Ok(Some(owner))) = (
        handler.manager().await,
        chain::endpoint_owner(chain, from.as_bytes()).await,
    ) else {
        return false;
    };
    manager == owner
}

/// Whether a command sent at `sent_at_ms` is recent enough at `now_ms`, in either direction to
/// allow for clock differences.
fn is_recent(sent_at_ms: u64, now_ms: u64) -> bool {
    sent_at_ms.abs_diff(now_ms) <= MANAGE_MAX_AGE_SECS * 1000
}

/// `status` with its texts cut to [`MAX_STATUS_TEXT_LEN`].
fn bounded(status: ProcessorStatus) -> ProcessorStatus {
    ProcessorStatus {
        app_version: truncate(status.app_version),
        os: truncate(status.os),
        ..status
    }
}

pub(crate) fn truncate(mut text: String) -> String {
    if text.len() > MAX_STATUS_TEXT_LEN {
        let end = (0..=MAX_STATUS_TEXT_LEN)
            .rev()
            .find(|&i| text.is_char_boundary(i))
            .unwrap_or(0);
        text.truncate(end);
    }
    text
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commands_are_recent_within_the_maximum_age_in_both_directions() {
        let now = 1_762_700_000_000;
        let max = MANAGE_MAX_AGE_SECS * 1000;

        assert!(is_recent(now, now));
        assert!(is_recent(now - max, now));
        assert!(is_recent(now + max, now));
        assert!(!is_recent(now - max - 1, now));
        assert!(!is_recent(now + max + 1, now));
        assert!(!is_recent(0, now));
    }

    #[test]
    fn status_texts_are_cut_at_a_character_boundary() {
        let long = "é".repeat(MAX_STATUS_TEXT_LEN);
        let status = bounded(ProcessorStatus {
            app_version: long,
            os: "Android 16".into(),
            battery_percent: Some(50),
            charging: Some(true),
            busy: false,
            uptime_secs: 1,
        });

        assert_eq!(status.app_version, "é".repeat(MAX_STATUS_TEXT_LEN / 2));
        assert_eq!(status.os, "Android 16");
        assert_eq!(status.battery_percent, Some(50));
    }
}
