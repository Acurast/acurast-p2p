//! Reads from an Acurast node over its JSON-RPC (HTTP): runtime metadata, runtime APIs and the
//! storage that lists a manager's processors.

use acurast_p2p::platform::{Chain, PlatformError};
use blake2::{Blake2b, Digest, digest::consts::U16};
use frame_metadata::{RuntimeMetadata, RuntimeMetadataPrefixed};
use parity_scale_codec::Decode;
use scale_info::{PortableRegistry, TypeDef};
use serde_json::{Value, json};

/// Collection of the manager NFTs: a manager's id is the item it owns in it.
const MANAGER_COLLECTION: u128 = 0;
/// Keys read per `state_getKeysPaged` request.
const KEYS_PAGE: u32 = 1000;

/// JSON-RPC client of one node.
#[derive(Clone)]
pub struct Rpc {
    url: String,
    http: reqwest::Client,
}

impl Rpc {
    /// `url` as given for a websocket connection (`wss://`, `ws://`) or over HTTP: nodes serve
    /// JSON-RPC over HTTP on the same port.
    pub fn new(url: &str) -> Self {
        let url = if let Some(rest) = url.strip_prefix("wss://") {
            format!("https://{rest}")
        } else if let Some(rest) = url.strip_prefix("ws://") {
            format!("http://{rest}")
        } else {
            url.to_owned()
        };
        // TLS with ring, as iroh. Fails only if a provider is already installed, which is then
        // used instead.
        let _ = rustls::crypto::ring::default_provider().install_default();
        Self {
            url,
            http: reqwest::Client::new(),
        }
    }

    async fn call(&self, method: &str, params: Value) -> Result<Value, String> {
        let request = json!({ "id": 1, "jsonrpc": "2.0", "method": method, "params": params });
        let response: Value = self
            .http
            .post(&self.url)
            .json(&request)
            .send()
            .await
            .map_err(|e| format!("{}: {e}", self.url))?
            .error_for_status()
            .map_err(|e| format!("{}: {e}", self.url))?
            .json()
            .await
            .map_err(|e| format!("{}: invalid response: {e}", self.url))?;
        if let Some(error) = response.get("error") {
            return Err(format!("{method}: {error}"));
        }
        response
            .get("result")
            .cloned()
            .ok_or_else(|| format!("{method}: no result"))
    }

    async fn call_bytes(&self, method: &str, params: Value) -> Result<Vec<u8>, String> {
        let result = self.call(method, params).await?;
        let hex = result
            .as_str()
            .ok_or_else(|| format!("{method}: expected a hex string"))?;
        hex::decode(hex.trim_start_matches("0x")).map_err(|e| format!("{method}: {e}"))
    }

    /// Hash of block 0, which identifies the network.
    pub async fn genesis_hash(&self) -> Result<[u8; 32], String> {
        self.call_bytes("chain_getBlockHash", json!([0]))
            .await?
            .try_into()
            .map_err(|_| "chain_getBlockHash: expected 32 bytes".into())
    }

    /// The runtime metadata of the best block.
    pub async fn metadata(&self) -> Result<RuntimeMetadata, String> {
        let bytes = self.call_bytes("state_getMetadata", json!([])).await?;
        RuntimeMetadataPrefixed::decode(&mut &bytes[..])
            .map(|m| m.1)
            .map_err(|e| format!("state_getMetadata: {e}"))
    }

    /// Calls the runtime API `method` at the best block with SCALE-encoded `args`.
    pub async fn state_call(&self, method: &str, args: &[u8]) -> Result<Vec<u8>, String> {
        let args = format!("0x{}", hex::encode(args));
        self.call_bytes("state_call", json!([method, args])).await
    }

    async fn runtime_api<R: Decode>(&self, method: &str, args: &[u8]) -> Result<R, String> {
        let output = self.state_call(method, args).await?;
        R::decode(&mut &output[..]).map_err(|e| format!("{method}: {e}"))
    }

    /// All storage keys starting with `prefix`.
    async fn keys(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, String> {
        let prefix = format!("0x{}", hex::encode(prefix));
        let mut keys = Vec::new();
        let mut start: Option<String> = None;
        loop {
            let page = self
                .call("state_getKeysPaged", json!([prefix, KEYS_PAGE, start]))
                .await?;
            let page: Vec<String> =
                serde_json::from_value(page).map_err(|e| format!("state_getKeysPaged: {e}"))?;
            let full = page.len() == KEYS_PAGE as usize;
            start = page.last().cloned();
            for key in page {
                keys.push(
                    hex::decode(key.trim_start_matches("0x"))
                        .map_err(|e| format!("state_getKeysPaged: {e}"))?,
                );
            }
            if !full {
                return Ok(keys);
            }
        }
    }

    /// Id of the manager `account`, `None` if it is not a manager.
    pub async fn manager_id(&self, account: &[u8; 32]) -> Result<Option<u128>, String> {
        // Uniques::Account: (account, collection, item) with Blake2_128Concat keys.
        let mut prefix = storage_prefix("Uniques", "Account");
        prefix.extend(blake2_128_concat(account));
        prefix.extend(blake2_128_concat(&MANAGER_COLLECTION.to_le_bytes()));
        let keys = self.keys(&prefix).await?;
        // The item is the last key: its Blake2_128 hash, then the u128.
        Ok(keys.iter().find_map(|key| {
            let item = key.get(prefix.len() + 16..)?;
            Some(u128::from_le_bytes(item.try_into().ok()?))
        }))
    }

    /// Processors paired with the manager `id`.
    pub async fn managed_processors(&self, id: u128) -> Result<Vec<[u8; 32]>, String> {
        // AcurastProcessorManager::ManagedProcessors: (manager id, processor), Blake2_128Concat.
        let mut prefix = storage_prefix("AcurastProcessorManager", "ManagedProcessors");
        prefix.extend(blake2_128_concat(&id.to_le_bytes()));
        let keys = self.keys(&prefix).await?;
        Ok(keys
            .iter()
            .filter_map(|key| key.get(prefix.len() + 16..)?.try_into().ok())
            .collect())
    }

    /// Endpoints registered by `account`.
    pub async fn endpoints(&self, account: &[u8; 32]) -> Result<Vec<[u8; 32]>, String> {
        self.runtime_api("P2pApi_endpoints", account).await
    }

    /// Account that registered the endpoint `id`.
    pub async fn endpoint_owner(&self, id: &[u8; 32]) -> Result<Option<[u8; 32]>, String> {
        self.runtime_api("P2pApi_endpoint_owner", id).await
    }
}

/// The chain as the library sees it, for the manager `account`: the library reads the relays
/// through it. The manager registers its endpoint from its wallet, never through the library.
pub struct ManagerChain {
    pub rpc: Rpc,
    pub account: [u8; 32],
}

#[async_trait::async_trait]
impl Chain for ManagerChain {
    fn account(&self) -> Vec<u8> {
        self.account.to_vec()
    }

    async fn genesis_hash(&self) -> Result<Vec<u8>, PlatformError> {
        self.rpc
            .genesis_hash()
            .await
            .map(|h| h.to_vec())
            .map_err(PlatformError)
    }

    async fn state_call(&self, method: String, args: Vec<u8>) -> Result<Vec<u8>, PlatformError> {
        self.rpc
            .state_call(&method, &args)
            .await
            .map_err(PlatformError)
    }

    async fn register_endpoint(&self, _id: Vec<u8>, _proof: Vec<u8>) -> Result<(), PlatformError> {
        Err(PlatformError(
            "register the key with `acurast-p2p register`".into(),
        ))
    }
}

fn storage_prefix(pallet: &str, item: &str) -> Vec<u8> {
    let mut prefix = twox_128(pallet.as_bytes()).to_vec();
    prefix.extend(twox_128(item.as_bytes()));
    prefix
}

fn twox_128(data: &[u8]) -> [u8; 16] {
    let mut out = [0; 16];
    out[..8].copy_from_slice(&twox_hash::XxHash64::oneshot(0, data).to_le_bytes());
    out[8..].copy_from_slice(&twox_hash::XxHash64::oneshot(1, data).to_le_bytes());
    out
}

fn blake2_128_concat(data: &[u8]) -> Vec<u8> {
    let mut key = Blake2b::<U16>::digest(data).to_vec();
    key.extend_from_slice(data);
    key
}

/// The pallet and call index of `pallet::call`, as the first two bytes of the encoded call.
pub fn call_index(metadata: &RuntimeMetadata, pallet: &str, call: &str) -> Result<[u8; 2], String> {
    let (index, calls, types) = match metadata {
        RuntimeMetadata::V14(m) => {
            let p = m.pallets.iter().find(|p| p.name == pallet);
            (
                p.map(|p| p.index),
                p.and_then(|p| p.calls.as_ref()).map(|c| c.ty.id),
                &m.types,
            )
        }
        RuntimeMetadata::V15(m) => {
            let p = m.pallets.iter().find(|p| p.name == pallet);
            (
                p.map(|p| p.index),
                p.and_then(|p| p.calls.as_ref()).map(|c| c.ty.id),
                &m.types,
            )
        }
        _ => return Err("unsupported metadata version: expected V14 or V15".into()),
    };
    let index = index.ok_or_else(|| {
        format!("this network has no {pallet} pallet: the P2P layer is not available there")
    })?;
    let call_index = calls
        .and_then(|ty| variant_index(types, ty, call))
        .ok_or_else(|| format!("{pallet} has no call {call}"))?;
    Ok([index, call_index])
}

fn variant_index(types: &PortableRegistry, ty: u32, name: &str) -> Option<u8> {
    match &types.resolve(ty)?.type_def {
        TypeDef::Variant(def) => def
            .variants
            .iter()
            .find(|v| v.name == name)
            .map(|v| v.index),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn websocket_urls_become_http() {
        assert_eq!(
            Rpc::new("wss://node.example:443").url,
            "https://node.example:443"
        );
        assert_eq!(Rpc::new("ws://127.0.0.1:9944").url, "http://127.0.0.1:9944");
        assert_eq!(Rpc::new("https://node.example").url, "https://node.example");
    }

    #[test]
    fn storage_prefix_matches_substrate() {
        // Well-known key of System::Account.
        assert_eq!(
            hex::encode(storage_prefix("System", "Account")),
            "26aa394eea5630e07c48ae0c9558cef7b99d880ec681799c0cf30e8886371da9"
        );
    }

    #[test]
    fn blake2_128_concat_appends_the_data() {
        let key = blake2_128_concat(&[1, 2, 3]);
        assert_eq!(key.len(), 16 + 3);
        assert_eq!(&key[16..], &[1, 2, 3]);
    }
}
