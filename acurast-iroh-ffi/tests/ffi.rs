//! The FFI surface, with the app's callbacks implemented in Rust.

use std::sync::{Arc, Mutex};

use acurast_iroh_ffi::{ChainClient, KeyStore, P2pError, P2pNode};
use parity_scale_codec::Encode;

#[derive(Default)]
struct MemoryKeyStore(Mutex<Option<Vec<u8>>>);

impl KeyStore for MemoryKeyStore {
    fn load_seed(&self) -> Result<Option<Vec<u8>>, P2pError> {
        Ok(self.0.lock().unwrap().clone())
    }

    fn store_seed(&self, seed: Vec<u8>) -> Result<(), P2pError> {
        *self.0.lock().unwrap() = Some(seed);
        Ok(())
    }
}

/// A chain with one relay that accepts every registration, or rejects them with
/// `reject_registration`.
#[derive(Default)]
struct MockChain {
    endpoints: Mutex<Vec<[u8; 32]>>,
    reject_registration: bool,
}

#[async_trait::async_trait]
impl ChainClient for MockChain {
    fn account(&self) -> Vec<u8> {
        [5u8; 32].to_vec()
    }

    async fn genesis_hash(&self) -> Result<Vec<u8>, P2pError> {
        Ok([9u8; 32].to_vec())
    }

    async fn state_call(&self, method: String, args: Vec<u8>) -> Result<Vec<u8>, P2pError> {
        Ok(match method.as_str() {
            "P2pApi_endpoints" => self.endpoints.lock().unwrap().encode(),
            "P2pApi_relay_nodes" => {
                let node = (
                    b"https://relay.example.com".to_vec(),
                    Some(7842u16),
                    756u16,
                    [0u8; 32],
                );
                vec![([1u8; 32], node)].encode()
            }
            "P2pApi_roles" => {
                let registered = self
                    .endpoints
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|id| id[..] == args[..]);
                vec![if registered { 0b0011u8 } else { 0 }]
            }
            other => panic!("unexpected runtime API {other}"),
        })
    }

    async fn register_endpoint(&self, id: Vec<u8>, _proof: Vec<u8>) -> Result<(), P2pError> {
        if self.reject_registration {
            return Err(P2pError::Platform {
                reason: "Module(AcurastP2p::InvalidProof)".into(),
            });
        }
        *self.endpoints.lock().unwrap() = vec![id.try_into().unwrap()];
        Ok(())
    }
}

#[tokio::test]
async fn start_registers_and_uses_the_chain_relays() {
    let key_store = Arc::new(MemoryKeyStore::default());
    let chain = Arc::new(MockChain::default());

    let node = P2pNode::start(key_store.clone(), chain.clone())
        .await
        .unwrap();

    assert!(node.registered_now());
    assert_eq!(
        hex::decode(node.endpoint_id()).unwrap(),
        chain.endpoints.lock().unwrap()[0]
    );
    assert_eq!(node.relays(), vec!["https://relay.example.com/"]);
    assert!(!node.refresh_relays().await.unwrap());
    assert_eq!(node.roles(node.endpoint_id()).await.unwrap(), 0b0011);
    assert_eq!(node.roles("00".repeat(32)).await.unwrap(), 0);
    assert!(matches!(
        node.roles("xyz".into()).await,
        Err(P2pError::Internal { .. })
    ));
    node.shutdown().await;

    // Same key on the next start, no new registration.
    let node = P2pNode::start(key_store, chain).await.unwrap();
    assert!(!node.registered_now());
    node.shutdown().await;
}

#[tokio::test]
async fn errors_of_the_app_are_returned_as_platform_errors() {
    let chain = Arc::new(MockChain {
        reject_registration: true,
        ..Default::default()
    });

    let result = P2pNode::start(Arc::new(MemoryKeyStore::default()), chain).await;

    match result {
        Err(P2pError::Platform { reason }) => assert!(reason.contains("InvalidProof")),
        Err(other) => panic!("unexpected error {other:?}"),
        Ok(_) => panic!("the start must fail"),
    }
}
