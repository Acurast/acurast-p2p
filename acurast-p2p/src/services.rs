//! Services of an endpoint: incoming connections go to the handler of their ALPN.

use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

use acurast_p2p_proto::{ERR_LIMIT, ERR_PROTOCOL};
use iroh::{Endpoint, endpoint::Connection};
use tokio::{sync::Semaphore, task::JoinHandle};

use crate::P2pEndpoint;

/// Connections handled at the same time, over all services; further ones are refused with
/// [`ERR_LIMIT`].
const MAX_CONCURRENT_CONNECTIONS: usize = 32;

/// Handles one incoming connection of a service.
pub(crate) type Handler =
    Arc<dyn Fn(Connection) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

type Handlers = Arc<Mutex<HashMap<Vec<u8>, Handler>>>;

/// The services of an endpoint and the task accepting their connections, started with the first
/// service.
#[derive(Default)]
pub(crate) struct Services {
    handlers: Handlers,
    acceptor: Mutex<Option<JoinHandle<()>>>,
}

impl Services {
    /// Stops accepting connections.
    pub(crate) fn stop(&self) {
        if let Some(acceptor) = self.acceptor.lock().expect("acceptor lock poisoned").take() {
            acceptor.abort();
        }
    }
}

impl P2pEndpoint {
    /// Passes the incoming connections with protocol `alpn` to `handler`, replacing a previous
    /// handler of `alpn`.
    pub(crate) fn serve(&self, alpn: &[u8], handler: Handler) {
        let services = self.services();
        services
            .handlers
            .lock()
            .expect("handler lock poisoned")
            .insert(alpn.to_vec(), handler);
        self.add_alpn(alpn);

        let mut acceptor = services.acceptor.lock().expect("acceptor lock poisoned");
        if acceptor.is_none() {
            *acceptor = Some(tokio::spawn(accept(
                self.inner().clone(),
                services.handlers.clone(),
            )));
        }
    }
}

async fn accept(endpoint: Endpoint, handlers: Handlers) {
    let limit = Arc::new(Semaphore::new(MAX_CONCURRENT_CONNECTIONS));
    while let Some(incoming) = endpoint.accept().await {
        let handlers = handlers.clone();
        let limit = limit.clone();
        tokio::spawn(async move {
            let Ok(conn) = incoming.await else {
                return;
            };
            let handler = handlers
                .lock()
                .expect("handler lock poisoned")
                .get(conn.alpn())
                .cloned();
            let Some(handler) = handler else {
                conn.close(ERR_PROTOCOL.into(), b"protocol");
                return;
            };
            let Ok(_permit) = limit.try_acquire() else {
                conn.close(ERR_LIMIT.into(), b"limit");
                return;
            };
            handler(conn).await;
        });
    }
}
