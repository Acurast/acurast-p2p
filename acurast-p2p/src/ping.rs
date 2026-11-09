//! Ping between endpoints, to check that two endpoints reach each other: the caller opens a
//! connection with [`ALPN_PING`], sends `ping` and gets `pong` back.

use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use acurast_p2p_proto::ERR_PROTOCOL;
use iroh::{EndpointAddr, EndpointId, TransportAddr, endpoint::Connection};
use tokio::time::Instant;

use crate::{Error, P2pEndpoint, RoleCache, Roles};

/// ALPN of the ping protocol.
pub const ALPN_PING: &[u8] = b"acurast/ping/1";

const PING: &[u8] = b"ping";
const PONG: &[u8] = b"pong";
/// How long an answered connection waits for the caller to close it.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(10);

/// How the ping reached the peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PingPath {
    /// Directly over IP, to the peer's address.
    Direct(SocketAddr),
    /// Through a relay.
    Relay,
}

impl PingPath {
    /// Whether the path stays in the local network: a direct path to a private, link-local,
    /// unique local or loopback address.
    pub fn is_local(&self) -> bool {
        match self {
            Self::Direct(addr) => is_local_ip(addr.ip()),
            Self::Relay => false,
        }
    }
}

fn is_local_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => ip.is_private() || ip.is_link_local() || ip.is_loopback(),
        IpAddr::V6(ip) => ip.is_unique_local() || ip.is_unicast_link_local() || ip.is_loopback(),
    }
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
    pub fn serve_ping(&self, roles: Arc<RoleCache>) {
        self.serve(
            ALPN_PING,
            Arc::new(move |conn| {
                let roles = roles.clone();
                Box::pin(async move { answer(&conn, &roles).await })
            }),
        );
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
    let direct = paths
        .iter()
        .filter(|path| path.is_selected())
        .find_map(|path| match path.remote_addr() {
            TransportAddr::Ip(addr) => Some(*addr),
            _ => None,
        });
    direct.map_or(PingPath::Relay, PingPath::Direct)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_paths_are_the_direct_ones_to_local_addresses() {
        let direct = |addr: &str| PingPath::Direct(addr.parse().unwrap());

        assert!(direct("192.168.1.20:4000").is_local());
        assert!(direct("10.0.0.5:4000").is_local());
        assert!(direct("169.254.3.4:4000").is_local());
        assert!(direct("[fd12::1]:4000").is_local());
        assert!(direct("[fe80::1]:4000").is_local());
        assert!(!direct("85.1.2.3:4000").is_local());
        assert!(!direct("[2a02::1]:4000").is_local());
        assert!(!PingPath::Relay.is_local());
    }
}
