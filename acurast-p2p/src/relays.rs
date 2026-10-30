//! Relay nodes listed on chain, turned into the relay configuration of the iroh endpoint.

use std::sync::Arc;

use iroh::{RelayConfig, RelayMap, RelayUrl};
use iroh_relay::RelayQuicConfig;

use crate::Error;

/// A relay node as listed on chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relay {
    pub url: RelayUrl,
    /// UDP port of the relay's QUIC address discovery, if it offers it.
    pub quic_port: Option<u16>,
}

impl Relay {
    /// Builds a relay from its on-chain entry: the URL bytes (`https://<host>[:<port>]`) and the
    /// optional QUIC port.
    pub fn from_chain(url: &[u8], quic_port: Option<u16>) -> Result<Self, Error> {
        let url = std::str::from_utf8(url).map_err(|_| Error::InvalidRelayUrl)?;
        let url = url.parse().map_err(|_| Error::InvalidRelayUrl)?;
        Ok(Self { url, quic_port })
    }

    pub(crate) fn config(&self) -> Arc<RelayConfig> {
        let quic = self.quic_port.map(RelayQuicConfig::new);
        Arc::new(RelayConfig::new(self.url.clone(), quic))
    }
}

/// The relay map with exactly `relays`.
pub(crate) fn relay_map(relays: &[Relay]) -> RelayMap {
    relays.iter().map(Relay::config).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relay_from_chain() {
        let relay = Relay::from_chain(b"https://relay.example.com", Some(7842)).unwrap();
        assert_eq!(relay.url.as_str(), "https://relay.example.com/");
        assert_eq!(relay.quic_port, Some(7842));

        let map = relay_map(std::slice::from_ref(&relay));
        assert_eq!(map.len(), 1);
        assert_eq!(
            map.get(&relay.url).unwrap().quic.as_ref().map(|q| q.port),
            Some(7842)
        );
    }

    #[test]
    fn relay_from_chain_rejects_invalid_urls() {
        assert!(Relay::from_chain(&[0xff, 0xfe], None).is_err());
        assert!(Relay::from_chain(b"not a url", None).is_err());
    }
}
