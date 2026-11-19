//! Reads from an Acurast node over its JSON-RPC (HTTP): the genesis hash and the index of a call.

use frame_metadata::{RuntimeMetadata, RuntimeMetadataPrefixed};
use parity_scale_codec::Decode;
use scale_info::{PortableRegistry, TypeDef};
use serde_json::{Value, json};

/// JSON-RPC client of one node.
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
}
