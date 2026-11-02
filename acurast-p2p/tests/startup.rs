//! Startup sequence against an in-memory key store and chain.

use std::sync::Mutex;

use acurast_p2p::{
    Error, Registration, SecretKey,
    platform::{Chain, KeyStore, PlatformError},
};
use acurast_p2p_proto::hash::register_proof_message;
use iroh::{PublicKey, Signature};
use parity_scale_codec::Encode;

const ACCOUNT: [u8; 32] = [5; 32];
const GENESIS: [u8; 32] = [9; 32];

#[derive(Default)]
struct MemoryKeyStore(Mutex<Option<Vec<u8>>>);

impl KeyStore for MemoryKeyStore {
    fn load_seed(&self) -> Result<Option<Vec<u8>>, PlatformError> {
        Ok(self.0.lock().unwrap().clone())
    }

    fn store_seed(&self, seed: Vec<u8>) -> Result<(), PlatformError> {
        *self.0.lock().unwrap() = Some(seed);
        Ok(())
    }
}

/// Answers `P2pApi` calls from memory and checks registrations like the pallet does.
#[derive(Default)]
struct MockChain {
    endpoints: Mutex<Vec<[u8; 32]>>,
    relay_urls: Mutex<Vec<&'static str>>,
    registrations: Mutex<usize>,
    fail_registration: bool,
    /// Endpoint reads before a registration shows up, like blocks until inclusion.
    inclusion_delay: usize,
    pending: Mutex<Option<([u8; 32], usize)>>,
}

impl MockChain {
    fn include_pending(&self) {
        let mut pending = self.pending.lock().unwrap();
        let Some((id, reads_left)) = pending.as_mut() else {
            return;
        };
        if *reads_left > 0 {
            *reads_left -= 1;
            return;
        }
        // A processor holds exactly one endpoint.
        *self.endpoints.lock().unwrap() = vec![*id];
        *pending = None;
    }
}

#[async_trait::async_trait]
impl Chain for MockChain {
    fn account(&self) -> Vec<u8> {
        ACCOUNT.encode()
    }

    async fn genesis_hash(&self) -> Result<Vec<u8>, PlatformError> {
        Ok(GENESIS.to_vec())
    }

    async fn state_call(&self, method: String, args: Vec<u8>) -> Result<Vec<u8>, PlatformError> {
        match method.as_str() {
            "P2pApi_endpoints" => {
                assert_eq!(args, ACCOUNT.encode());
                self.include_pending();
                Ok(self.endpoints.lock().unwrap().encode())
            }
            "P2pApi_relay_nodes" => {
                // (id, RelayNode { url, quic_port, country, operator })
                let nodes: Vec<([u8; 32], (Vec<u8>, Option<u16>, u16, [u8; 32]))> = self
                    .relay_urls
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|url| ([1; 32], (url.as_bytes().to_vec(), Some(7842), 756, [0; 32])))
                    .collect();
                Ok(nodes.encode())
            }
            other => Err(PlatformError(format!("unknown runtime API {other}"))),
        }
    }

    async fn register_endpoint(&self, id: Vec<u8>, proof: Vec<u8>) -> Result<(), PlatformError> {
        if self.fail_registration {
            return Err(PlatformError("insufficient balance".into()));
        }
        let id: [u8; 32] = id.try_into().unwrap();
        let proof: [u8; 64] = proof.try_into().unwrap();
        let message = register_proof_message(&GENESIS, &ACCOUNT.encode());
        PublicKey::from_bytes(&id)
            .unwrap()
            .verify(&message, &Signature::from_bytes(&proof))
            .map_err(|_| PlatformError("InvalidProof".into()))?;
        *self.pending.lock().unwrap() = Some((id, self.inclusion_delay));
        *self.registrations.lock().unwrap() += 1;
        Ok(())
    }
}

#[tokio::test]
async fn first_start_creates_and_registers_the_key() {
    let store = MemoryKeyStore::default();
    let chain = MockChain::default();
    chain
        .relay_urls
        .lock()
        .unwrap()
        .push("https://relay.example.com");

    let (endpoint, registration) = acurast_p2p::start(&store, &chain, vec![]).await.unwrap();

    assert_eq!(registration, Registration::Registered);
    let seed: [u8; 32] = store.0.lock().unwrap().clone().unwrap().try_into().unwrap();
    assert_eq!(
        endpoint.id(),
        SecretKey::from_bytes(&seed).public(),
        "the stored key is used"
    );
    assert_eq!(
        *chain.endpoints.lock().unwrap(),
        vec![*endpoint.id().as_bytes()]
    );
    let relays = endpoint.relays();
    assert_eq!(relays.len(), 1);
    assert_eq!(relays[0].url.as_str(), "https://relay.example.com/");
    assert_eq!(relays[0].quic_port, Some(7842));
    endpoint.close().await;
}

#[tokio::test]
async fn restart_keeps_the_key_and_does_not_register_again() {
    let store = MemoryKeyStore::default();
    let chain = MockChain::default();
    let (first, _) = acurast_p2p::start(&store, &chain, vec![]).await.unwrap();
    let id = first.id();
    first.close().await;

    let (second, registration) = acurast_p2p::start(&store, &chain, vec![]).await.unwrap();

    assert_eq!(registration, Registration::AlreadyRegistered);
    assert_eq!(second.id(), id);
    assert_eq!(*chain.registrations.lock().unwrap(), 1);
    second.close().await;
}

#[tokio::test]
async fn new_key_replaces_the_registered_one() {
    // E.g. after a reinstall: the chain still has the old endpoint.
    let store = MemoryKeyStore::default();
    let chain = MockChain::default();
    chain.endpoints.lock().unwrap().push([3; 32]);

    let (endpoint, registration) = acurast_p2p::start(&store, &chain, vec![]).await.unwrap();

    assert_eq!(registration, Registration::Registered);
    assert_eq!(
        *chain.endpoints.lock().unwrap(),
        vec![*endpoint.id().as_bytes()]
    );
    endpoint.close().await;
}

#[tokio::test(start_paused = true)]
async fn start_waits_until_the_registration_is_on_chain() {
    let store = MemoryKeyStore::default();
    let chain = MockChain {
        inclusion_delay: 3,
        ..Default::default()
    };

    let (endpoint, registration) = acurast_p2p::start(&store, &chain, vec![]).await.unwrap();

    assert_eq!(registration, Registration::Registered);
    assert_eq!(
        *chain.endpoints.lock().unwrap(),
        vec![*endpoint.id().as_bytes()]
    );
    endpoint.close().await;
}

#[tokio::test(start_paused = true)]
async fn registration_that_never_lands_times_out() {
    let store = MemoryKeyStore::default();
    let chain = MockChain {
        inclusion_delay: usize::MAX,
        ..Default::default()
    };

    let result = acurast_p2p::start(&store, &chain, vec![]).await;

    assert!(matches!(result, Err(Error::RegistrationTimeout)));
}

#[tokio::test]
async fn invalid_stored_seed_is_an_error() {
    let store = MemoryKeyStore(Mutex::new(Some(vec![1; 31])));
    let chain = MockChain::default();

    let result = acurast_p2p::start(&store, &chain, vec![]).await;

    assert!(matches!(result, Err(Error::InvalidSeed)));
}

#[tokio::test]
async fn failed_registration_is_reported() {
    let store = MemoryKeyStore::default();
    let chain = MockChain {
        fail_registration: true,
        ..Default::default()
    };

    let result = acurast_p2p::start(&store, &chain, vec![]).await;

    assert!(
        matches!(result, Err(Error::Platform(PlatformError(e))) if e == "insufficient balance")
    );
    // The key is kept, so the next start registers the same one.
    assert!(store.0.lock().unwrap().is_some());
}

#[tokio::test]
async fn relays_follow_the_chain() {
    let store = MemoryKeyStore::default();
    let chain = MockChain::default();
    chain
        .relay_urls
        .lock()
        .unwrap()
        .push("https://a.example.com");
    let (endpoint, _) = acurast_p2p::start(&store, &chain, vec![]).await.unwrap();

    assert!(
        !endpoint.refresh_relays(&chain).await.unwrap(),
        "unchanged list"
    );

    *chain.relay_urls.lock().unwrap() = vec!["https://b.example.com", "not a url"];
    assert!(endpoint.refresh_relays(&chain).await.unwrap());
    let urls: Vec<_> = endpoint
        .relays()
        .iter()
        .map(|r| r.url.to_string())
        .collect();
    assert_eq!(
        urls,
        vec!["https://b.example.com/"],
        "invalid entries are skipped"
    );
    endpoint.close().await;
}
