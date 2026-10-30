//! Roles of peers, read from the chain and cached, and the role check of incoming connections.

use std::{collections::HashMap, sync::Arc, time::Duration};

use acurast_p2p_proto::ERR_ROLE;
use iroh::{EndpointId, endpoint::Connection};
use parity_scale_codec::{Decode, Encode};
use tokio::{sync::Mutex, time::Instant};

use crate::{Error, platform::Chain};

/// How long a peer's roles are reused before asking the chain again.
pub const ROLE_CACHE_TTL: Duration = Duration::from_secs(10 * 60);
/// Same for peers without any role, so a new registration is noticed sooner.
pub const ROLE_CACHE_NEGATIVE_TTL: Duration = Duration::from_secs(60);
/// Maximum number of cached peers.
const ROLE_CACHE_CAPACITY: usize = 10_000;

/// Roles of an endpoint, with the same bits as the P2P pallet.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub struct Roles(pub u8);

impl Roles {
    /// The endpoint is registered by an account.
    pub const REGISTERED: Self = Self(0b0001);
    /// The endpoint is registered by an attested processor.
    pub const PROCESSOR: Self = Self(0b0010);
    /// The endpoint is registered by a manager.
    pub const MANAGER: Self = Self(0b0100);
    /// The endpoint is a relay node.
    pub const RELAY: Self = Self(0b1000);

    pub const fn empty() -> Self {
        Self(0)
    }

    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// Whether all roles of `other` are in `self`.
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    /// Whether `self` and `other` have at least one role in common.
    pub const fn intersects(self, other: Self) -> bool {
        self.0 & other.0 != 0
    }
}

impl std::ops::BitOr for Roles {
    type Output = Self;

    fn bitor(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

/// Roles of peers, read through `P2pApi::roles` and kept for [`ROLE_CACHE_TTL`]
/// ([`ROLE_CACHE_NEGATIVE_TTL`] without roles).
///
/// A role removed on chain therefore stops working within the TTL.
pub struct RoleCache {
    chain: Arc<dyn Chain>,
    entries: Mutex<HashMap<EndpointId, (Roles, Instant)>>,
}

impl RoleCache {
    pub fn new(chain: Arc<dyn Chain>) -> Self {
        Self {
            chain,
            entries: Default::default(),
        }
    }

    /// Roles of `id`, from the cache or the chain.
    pub async fn roles(&self, id: &EndpointId) -> Result<Roles, Error> {
        let now = Instant::now();
        if let Some((roles, expires)) = self.entries.lock().await.get(id)
            && *expires > now
        {
            return Ok(*roles);
        }

        let output = self
            .chain
            .state_call("P2pApi_roles".into(), id.as_bytes().to_vec())
            .await?;
        let roles = Roles::decode(&mut &output[..])
            .map_err(|e| Error::InvalidChainData(format!("P2pApi_roles: {e}")))?;

        let ttl = if roles.is_empty() {
            ROLE_CACHE_NEGATIVE_TTL
        } else {
            ROLE_CACHE_TTL
        };
        let mut entries = self.entries.lock().await;
        if entries.len() >= ROLE_CACHE_CAPACITY {
            evict(&mut entries, now);
        }
        entries.insert(*id, (roles, now + ttl));
        Ok(roles)
    }

    /// Forgets the cached roles of `id`, e.g. after a protocol error.
    pub async fn invalidate(&self, id: &EndpointId) {
        self.entries.lock().await.remove(id);
    }

    /// Checks that the peer of `conn` has at least one of the roles in `allowed`.
    ///
    /// Returns its roles, or closes the connection with [`ERR_ROLE`] and returns `None`, also when
    /// its roles cannot be read.
    pub async fn authorize(&self, conn: &Connection, allowed: Roles) -> Option<Roles> {
        match self.roles(&conn.remote_id()).await {
            Ok(roles) if roles.intersects(allowed) => Some(roles),
            _ => {
                conn.close(ERR_ROLE.into(), b"role");
                None
            }
        }
    }
}

/// Makes room for one entry: drops expired entries, or else the one that expires first.
fn evict(entries: &mut HashMap<EndpointId, (Roles, Instant)>, now: Instant) {
    entries.retain(|_, (_, expires)| *expires > now);
    if entries.len() >= ROLE_CACHE_CAPACITY
        && let Some(id) = entries
            .iter()
            .min_by_key(|(_, (_, expires))| *expires)
            .map(|(id, _)| *id)
    {
        entries.remove(&id);
    }
}
