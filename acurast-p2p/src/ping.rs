//! Ping between endpoints, to check that two endpoints reach each other: the caller opens a
//! connection with [`ALPN_PING`], sends `ping` and gets `pong` back.

use std::{sync::Arc, time::Duration};

use acurast_p2p_proto::{ERR_LIMIT, ERR_PROTOCOL};
use iroh::{Endpoint, EndpointAddr, EndpointId, endpoint::Connection};
use tokio::{sync::Semaphore, task::JoinHandle, time::Instant};

use crate::{Error, P2pEndpoint, RoleCache, Roles};

/// ALPN of the ping protocol.
pub const ALPN_PING: &[u8] = b"acurast/ping/1";

const PING: &[u8] = b"ping";
const PONG: &[u8] = b"pong";
/// Pings answered at the same time; further ones are refused with [`ERR_LIMIT`].
const MAX_CONCURRENT_PINGS: usize = 16;
/// How long an answered connection waits for the caller to close it.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(10);

/// How the ping reached the peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PingPath {
    /// Directly over IP.
    Direct,
    /// Through a relay.
    Relay,
}

/// Outcome of [`P2pEndpoint::ping`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PingReport {
    /// Time to set up the connection.
    pub connect: Duration,
    /// Time from sending `ping` to receiving `pong`.
    pub rtt: Duration,
    /// Path the connection used when `pong` arrived.
    pub path: PingPath,
}

impl P2pEndpoint {
    /// Answers the pings of registered peers until the endpoint closes.
    ///
    /// The returned task accepts all incoming connections of this endpoint and closes those that
    /// are not pings.
    pub fn serve_ping(&self, roles: Arc<RoleCache>) -> JoinHandle<()> {
        self.add_alpn(ALPN_PING);
        tokio::spawn(accept_pings(self.inner().clone(), roles))
    }

    /// Pings the endpoint `id`, whose address comes from the registry.
    pub async fn ping(&self, id: EndpointId) -> Result<PingReport, Error> {
        let started = Instant::now();
        let conn = self.connect(EndpointAddr::new(id), ALPN_PING).await?;
        let connect = started.elapsed();

        let sent = Instant::now();
        let reply = exchange(&conn).await;
        let rtt = sent.elapsed();
        let path = selected_path(&conn);
        conn.close(0u32.into(), b"done");

        match reply? {
            pong if pong == PONG => Ok(PingReport { connect, rtt, path }),
            _ => Err(Error::Ping("unexpected reply".into())),
        }
    }
}

async fn accept_pings(endpoint: Endpoint, roles: Arc<RoleCache>) {
    let limit = Arc::new(Semaphore::new(MAX_CONCURRENT_PINGS));
    while let Some(incoming) = endpoint.accept().await {
        let roles = roles.clone();
        let limit = limit.clone();
        tokio::spawn(async move {
            let Ok(conn) = incoming.await else {
                return;
            };
            if conn.alpn() != ALPN_PING {
                conn.close(ERR_PROTOCOL.into(), b"protocol");
                return;
            }
            let Ok(_permit) = limit.try_acquire() else {
                conn.close(ERR_LIMIT.into(), b"limit");
                return;
            };
            answer(&conn, &roles).await;
        });
    }
}

/// Answers one ping of a registered peer.
async fn answer(conn: &Connection, roles: &RoleCache) {
    if roles.authorize(conn, Roles::REGISTERED).await.is_none() {
        return;
    }
    let Ok((mut send, mut recv)) = conn.accept_bi().await else {
        return;
    };
    match recv.read_to_end(PING.len()).await {
        Ok(msg) if msg == PING => {
            if send.write_all(PONG).await.is_ok() && send.finish().is_ok() {
                tracing::debug!("answered the ping of {}", conn.remote_id());
            }
        }
        _ => {
            conn.close(ERR_PROTOCOL.into(), b"protocol");
            return;
        }
    }
    // The caller closes the connection once it has read the reply.
    let _ = tokio::time::timeout(CLOSE_TIMEOUT, conn.closed()).await;
}

async fn exchange(conn: &Connection) -> Result<Vec<u8>, Error> {
    let ping_error = |e: &dyn std::fmt::Display| Error::Ping(e.to_string());
    let (mut send, mut recv) = conn.open_bi().await.map_err(|e| ping_error(&e))?;
    send.write_all(PING).await.map_err(|e| ping_error(&e))?;
    send.finish().map_err(|e| ping_error(&e))?;
    recv.read_to_end(PONG.len())
        .await
        .map_err(|e| ping_error(&e))
}

fn selected_path(conn: &Connection) -> PingPath {
    let paths = conn.paths();
    let direct = paths.iter().any(|path| path.is_selected() && path.is_ip());
    if direct {
        PingPath::Direct
    } else {
        PingPath::Relay
    }
}
