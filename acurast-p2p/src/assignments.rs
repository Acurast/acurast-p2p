//! Notices of new job assignments, sent by relay nodes so that processors need not poll the
//! chain. A notice is only a hint: the processor reads its assignments from the chain.

use std::{sync::Arc, time::Duration};

pub use acurast_p2p_proto::{ALPN_ASSIGNMENTS, messages::AssignmentNotice};
use acurast_p2p_proto::{
    ERR_PROTOCOL, MAX_FRAME, MAX_NOTICE_JOBS,
    frame::{self, HEADER_LEN},
    messages::{AssignmentNoticeError, AssignmentNoticeReply},
};
use iroh::endpoint::Connection;

use crate::{P2pEndpoint, RoleCache, Roles};

/// How long a handled connection waits for the relay node to close it.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(10);

/// Called with every notice accepted from a relay node.
pub type OnNotice = Arc<dyn Fn(AssignmentNotice) + Send + Sync>;

impl P2pEndpoint {
    /// Accepts the assignment notices of relay nodes and passes each to `on_notice`, until the
    /// endpoint closes. Notices from other peers are refused.
    pub fn serve_assignments(&self, roles: Arc<RoleCache>, on_notice: OnNotice) {
        self.serve(
            ALPN_ASSIGNMENTS,
            Arc::new(move |conn| {
                let roles = roles.clone();
                let on_notice = on_notice.clone();
                Box::pin(async move { receive(&conn, &roles, &on_notice).await })
            }),
        );
    }
}

async fn receive(conn: &Connection, roles: &RoleCache, on_notice: &OnNotice) {
    if roles.authorize(conn, Roles::RELAY).await.is_none() {
        return;
    }
    let Ok((mut send, mut recv)) = conn.accept_bi().await else {
        return;
    };
    let Ok(bytes) = recv.read_to_end(HEADER_LEN + MAX_FRAME).await else {
        conn.close(ERR_PROTOCOL.into(), b"protocol");
        return;
    };
    let notice = match frame::decode::<AssignmentNotice>(&bytes) {
        Ok((notice, used)) if used == bytes.len() => notice,
        _ => {
            conn.close(ERR_PROTOCOL.into(), b"protocol");
            return;
        }
    };
    let reply = if notice.jobs.len() > MAX_NOTICE_JOBS {
        AssignmentNoticeReply::Err(AssignmentNoticeError::Invalid)
    } else {
        AssignmentNoticeReply::Ok
    };
    if let Ok(bytes) = frame::encode(&reply) {
        let _ = send.write_all(&bytes).await;
        let _ = send.finish();
    }
    if reply == AssignmentNoticeReply::Ok {
        tracing::debug!(
            "assignment notice from {} for block {}",
            conn.remote_id(),
            notice.block
        );
        on_notice(notice);
    }
    // The relay node closes the connection once it has read the reply.
    let _ = tokio::time::timeout(CLOSE_TIMEOUT, conn.closed()).await;
}
