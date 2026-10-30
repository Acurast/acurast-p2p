//! Reads of the P2P runtime API (`P2pApi`) through [`Chain`].

use parity_scale_codec::Decode;

use crate::{Error, Relay, platform::Chain};

/// On-chain relay node, as encoded by the P2P pallet.
#[derive(Decode)]
struct RelayNode {
    url: Vec<u8>,
    quic_port: Option<u16>,
    _country: u16,
    _operator: [u8; 32],
}

async fn call<R: Decode>(chain: &dyn Chain, method: &str, args: Vec<u8>) -> Result<R, Error> {
    let output = chain.state_call(method.into(), args).await?;
    R::decode(&mut &output[..]).map_err(|e| Error::InvalidChainData(format!("{method}: {e}")))
}

/// Endpoints registered by this processor's account.
pub(crate) async fn own_endpoints(chain: &dyn Chain) -> Result<Vec<[u8; 32]>, Error> {
    call(chain, "P2pApi_endpoints", chain.account()).await
}

/// Relay nodes listed on chain. Entries with an invalid URL are skipped.
pub(crate) async fn relay_nodes(chain: &dyn Chain) -> Result<Vec<Relay>, Error> {
    let nodes: Vec<([u8; 32], RelayNode)> = call(chain, "P2pApi_relay_nodes", Vec::new()).await?;
    Ok(nodes
        .into_iter()
        .filter_map(|(_, node)| Relay::from_chain(&node.url, node.quic_port).ok())
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::platform::PlatformError;

    const ALICE: &str = "d43593c715fdd31c61141abd04a99fd6822c8558854ccde39a5684e7a56da27d";
    const ENDPOINT: &str = "ea4a6c63e29c520abef5507b132ec5f9954776aebebe7b92421eea691446d22c";

    /// Answers recorded from a local network running the Canary runtime, after //Alice registered
    /// `ENDPOINT` and Root listed the relay `https://relay.local.test:8443` (QUIC port 7842).
    struct RecordedChain;

    #[async_trait::async_trait]
    impl Chain for RecordedChain {
        fn account(&self) -> Vec<u8> {
            hex::decode(ALICE).unwrap()
        }

        async fn genesis_hash(&self) -> Result<Vec<u8>, PlatformError> {
            unreachable!()
        }

        async fn state_call(
            &self,
            method: String,
            args: Vec<u8>,
        ) -> Result<Vec<u8>, PlatformError> {
            let answer = match method.as_str() {
                "P2pApi_endpoints" => {
                    assert_eq!(hex::encode(args), ALICE);
                    format!("04{ENDPOINT}")
                }
                "P2pApi_relay_nodes" => format!(
                    "04{}74{}01a21ef402{ALICE}",
                    "02".repeat(32),
                    hex::encode("https://relay.local.test:8443"),
                ),
                _ => unreachable!(),
            };
            Ok(hex::decode(answer).unwrap())
        }

        async fn register_endpoint(&self, _: Vec<u8>, _: Vec<u8>) -> Result<(), PlatformError> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn decodes_runtime_answers() {
        let endpoints = own_endpoints(&RecordedChain).await.unwrap();
        assert_eq!(
            endpoints,
            vec![<[u8; 32]>::try_from(hex::decode(ENDPOINT).unwrap()).unwrap()]
        );

        let relays = relay_nodes(&RecordedChain).await.unwrap();
        assert_eq!(relays.len(), 1);
        assert_eq!(relays[0].url.as_str(), "https://relay.local.test:8443/");
        assert_eq!(relays[0].quic_port, Some(7842));
    }
}
