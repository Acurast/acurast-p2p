//! Client of the registry served by the relay nodes. It publishes where this endpoint can be
//! reached and finds where other endpoints can be reached, as iroh's address lookup.
//!
//! Each record is held by the two relays chosen by rendezvous hashing over the relays listed on
//! chain: this endpoint registers at both and refreshes its record before it expires; a lookup
//! asks both and keeps the newest record.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use acurast_p2p_proto::{
    ALPN_REGISTRY, MAX_DIRECT_ADDRS, MAX_FRAME, PUBLISH_DEBOUNCE_SECS, REGISTRY_MAX_TTL_SECS,
    REGISTRY_REFRESH_SECS, REGISTRY_REPLICAS,
    frame::{self, HEADER_LEN},
    hash::home_registries,
    messages::{Record, RegistryRequest, RegistryResponse},
};
use iroh::{
    Endpoint, EndpointAddr, EndpointId,
    address_lookup::{AddressLookup, EndpointData, EndpointInfo, Error as LookupError, Item},
    endpoint::Connection,
};
use n0_future::{
    boxed::BoxStream,
    stream::{self, StreamExt},
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

use crate::relays::Relay;

/// Name of this lookup in iroh's results.
const PROVENANCE: &str = "acurast-registry";

/// The relays listed on chain, shared by the endpoint and its registry client.
pub(crate) type SharedRelays = Arc<Mutex<Vec<Relay>>>;

enum Command {
    Publish(EndpointData),
    RelaysChanged,
    Resolve(EndpointId, oneshot::Sender<Option<Record>>),
}

/// The address lookup installed on the endpoint. It only forwards to the [`RegistryWorker`],
/// which owns the connections, so that the endpoint does not hold itself.
#[derive(Debug, Clone)]
pub(crate) struct RegistryLookup {
    commands: mpsc::UnboundedSender<Command>,
    relays: SharedRelays,
}

impl RegistryLookup {
    /// The lookup and the receiver that [`RegistryWorker::spawn`] serves.
    pub(crate) fn new(relays: SharedRelays) -> (Self, RegistryCommands) {
        let (commands, receiver) = mpsc::unbounded_channel();
        (Self { commands, relays }, RegistryCommands(receiver))
    }

    /// Re-registers at the home registries of the new relay set.
    pub(crate) fn relays_changed(&self) {
        let _ = self.commands.send(Command::RelaysChanged);
    }

    /// A relay node is reached through its own relay, as listed on chain.
    fn relay_item(&self, id: EndpointId) -> Option<Item> {
        let relays = self.relays.lock().expect("relay list lock poisoned");
        let relay = relays.iter().find(|relay| relay.id == id)?;
        let info = EndpointInfo::new(id).with_relay_url(relay.url.clone());
        Some(Item::new(info, PROVENANCE, None))
    }
}

impl AddressLookup for RegistryLookup {
    fn publish(&self, data: &EndpointData) {
        let _ = self.commands.send(Command::Publish(data.clone()));
    }

    fn resolve(&self, id: EndpointId) -> Option<BoxStream<Result<Item, LookupError>>> {
        if let Some(item) = self.relay_item(id) {
            return Some(Box::pin(stream::once(Ok(item))));
        }
        let (reply, answer) = oneshot::channel();
        self.commands.send(Command::Resolve(id, reply)).ok()?;
        let item = stream::once_future(answer).filter_map(move |record| {
            let item = record
                .ok()
                .flatten()
                .and_then(|record| record_item(id, record));
            item.map(Ok)
        });
        Some(Box::pin(item))
    }
}

/// Commands of a [`RegistryLookup`], served by a [`RegistryWorker`].
pub(crate) struct RegistryCommands(mpsc::UnboundedReceiver<Command>);

/// Publishes this endpoint's record and answers lookups, on its own task.
pub(crate) struct RegistryWorker {
    endpoint: Endpoint,
    relays: SharedRelays,
    connections: Arc<tokio::sync::Mutex<HashMap<EndpointId, Connection>>>,
    latest: Option<EndpointData>,
    last_seq: u64,
}

impl RegistryWorker {
    /// Serves `commands` with `endpoint` until the lookup is dropped or the task is aborted.
    pub(crate) fn spawn(
        endpoint: Endpoint,
        relays: SharedRelays,
        commands: RegistryCommands,
    ) -> JoinHandle<()> {
        let worker = Self {
            endpoint,
            relays,
            connections: Default::default(),
            latest: None,
            last_seq: 0,
        };
        tokio::spawn(worker.run(commands.0))
    }

    async fn run(mut self, mut commands: mpsc::UnboundedReceiver<Command>) {
        let refresh = Duration::from_secs(REGISTRY_REFRESH_SECS.into());
        let debounce = Duration::from_secs(PUBLISH_DEBOUNCE_SECS.into());
        // When the next Register is due: shortly after a change, else at the next refresh.
        let mut next_publish: Option<tokio::time::Instant> = None;
        loop {
            let due = next_publish.unwrap_or_else(|| tokio::time::Instant::now() + refresh);
            tokio::select! {
                command = commands.recv() => match command {
                    None => return,
                    Some(Command::Publish(data)) => {
                        self.latest = Some(data);
                        next_publish = Some(tokio::time::Instant::now() + debounce);
                    },
                    Some(Command::RelaysChanged) => {
                        self.connections.lock().await.clear();
                        next_publish = Some(tokio::time::Instant::now());
                    },
                    Some(Command::Resolve(id, reply)) => self.resolve(id, reply),
                },
                _ = tokio::time::sleep_until(due) => {
                    self.publish().await;
                    next_publish = Some(tokio::time::Instant::now() + refresh);
                },
            }
        }
    }

    async fn publish(&mut self) {
        let Some(data) = self.latest.clone() else {
            return;
        };
        let request = RegistryRequest::Register {
            seq: self.next_seq(),
            relay_url: data.relay_urls().next().map(|url| url.to_string()),
            direct: data.ip_addrs().copied().take(MAX_DIRECT_ADDRS).collect(),
            namespaces: Vec::new(),
            ttl_secs: REGISTRY_MAX_TTL_SECS,
        };
        let client = self.client();
        for registry in client.home_registries(self.endpoint.id()) {
            match client.request(&registry, &request).await {
                Ok(RegistryResponse::Registered { .. }) => {}
                Ok(other) => tracing::warn!(%registry.id, ?other, "registry refused the record"),
                Err(e) => tracing::warn!(%registry.id, "cannot register: {e}"),
            }
        }
    }

    /// Looks `id` up at both home registries on a task of its own, so that a slow registry does
    /// not hold up publishing.
    fn resolve(&self, id: EndpointId, reply: oneshot::Sender<Option<Record>>) {
        let client = Arc::new(self.client());
        tokio::spawn(async move {
            let mut lookups = tokio::task::JoinSet::new();
            for registry in client.home_registries(id) {
                let client = client.clone();
                lookups.spawn(async move {
                    let lookup = RegistryRequest::Lookup { id: *id.as_bytes() };
                    client.request(&registry, &lookup).await
                });
            }
            let mut newest: Option<Record> = None;
            while let Some(answer) = lookups.join_next().await {
                if let Ok(Ok(RegistryResponse::Found(Some(record)))) = answer
                    && newest.as_ref().is_none_or(|n| record.seq > n.seq)
                {
                    newest = Some(record);
                }
            }
            let _ = reply.send(newest);
        });
    }

    /// Unix time in milliseconds, strictly increasing across calls.
    fn next_seq(&mut self) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as u64);
        self.last_seq = now.max(self.last_seq + 1);
        self.last_seq
    }

    fn client(&self) -> RegistryClient {
        RegistryClient {
            endpoint: self.endpoint.clone(),
            relays: self
                .relays
                .lock()
                .expect("relay list lock poisoned")
                .clone(),
            connections: self.connections.clone(),
        }
    }
}

/// Sends registry requests over one cached connection per relay.
struct RegistryClient {
    endpoint: Endpoint,
    relays: Vec<Relay>,
    connections: Arc<tokio::sync::Mutex<HashMap<EndpointId, Connection>>>,
}

impl RegistryClient {
    /// The relays that hold the record of `id`.
    fn home_registries(&self, id: EndpointId) -> Vec<Relay> {
        let ids: Vec<[u8; 32]> = self
            .relays
            .iter()
            .map(|relay| *relay.id.as_bytes())
            .collect();
        home_registries(id.as_bytes(), &ids, REGISTRY_REPLICAS)
            .into_iter()
            .filter_map(|home| {
                self.relays
                    .iter()
                    .find(|r| r.id.as_bytes() == &home)
                    .cloned()
            })
            .collect()
    }

    /// Sends `request` to `registry`, reconnecting once if the cached connection is gone.
    async fn request(
        &self,
        registry: &Relay,
        request: &RegistryRequest,
    ) -> Result<RegistryResponse, String> {
        let connection = self.connection(registry, false).await?;
        match exchange(&connection, request).await {
            Ok(response) => Ok(response),
            Err(_) => exchange(&self.connection(registry, true).await?, request).await,
        }
    }

    async fn connection(&self, registry: &Relay, fresh: bool) -> Result<Connection, String> {
        let mut connections = self.connections.lock().await;
        if !fresh
            && let Some(connection) = connections.get(&registry.id)
            && connection.close_reason().is_none()
        {
            return Ok(connection.clone());
        }
        let addr = EndpointAddr::new(registry.id).with_relay_url(registry.url.clone());
        let connection = self
            .endpoint
            .connect(addr, ALPN_REGISTRY)
            .await
            .map_err(|e| e.to_string())?;
        connections.insert(registry.id, connection.clone());
        Ok(connection)
    }
}

/// One request and its response on a new bi stream.
async fn exchange(
    connection: &Connection,
    request: &RegistryRequest,
) -> Result<RegistryResponse, String> {
    let (mut send, mut recv) = connection.open_bi().await.map_err(|e| e.to_string())?;
    let frame = frame::encode(request).map_err(|e| e.to_string())?;
    send.write_all(&frame).await.map_err(|e| e.to_string())?;
    send.finish().map_err(|e| e.to_string())?;
    let bytes = recv
        .read_to_end(HEADER_LEN + MAX_FRAME)
        .await
        .map_err(|e| e.to_string())?;
    let (response, _) = frame::decode(&bytes).map_err(|e| e.to_string())?;
    Ok(response)
}

/// The lookup result of a record; `None` if it does not belong to `id`.
fn record_item(id: EndpointId, record: Record) -> Option<Item> {
    if &record.id != id.as_bytes() {
        return None;
    }
    let mut info = EndpointInfo::new(id).with_ip_addrs(record.direct.clone());
    if let Some(url) = record.relay_url.and_then(|url| url.parse().ok()) {
        info = info.with_relay_url(url);
    }
    Some(Item::new(info, PROVENANCE, Some(record.seq * 1000)))
}
