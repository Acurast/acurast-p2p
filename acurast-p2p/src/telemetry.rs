//! Network reports of the processor, sent periodically to the relay node of its home relay, so
//! that the P2P coverage of the fleet can be followed: how many processors reach a relay, how many
//! can only use the relay, how often connections get a direct path.

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

pub use acurast_p2p_proto::{
    ALPN_TELEMETRY,
    messages::{
        ConnectionStats, NetworkReport, TelemetryError, TelemetryPlatform, TelemetryReply,
        TelemetryReport,
    },
};
use acurast_p2p_proto::{
    MAX_FRAME, TELEMETRY_FIRST_DELAY_SECS, TELEMETRY_INTERVAL_SECS,
    frame::{self, HEADER_LEN},
};
use iroh::{Endpoint, EndpointAddr, Watcher, endpoint::Connection};
use n0_future::StreamExt;
use tokio::task::JoinHandle;

use crate::{Error, P2pEndpoint, registry::SharedRelays};

/// Connections counted since the last report.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ConnectionCounts {
    direct: u32,
    relayed: u32,
    /// Sum of the times to the first direct path of the `direct` connections.
    time_to_direct: Duration,
}

impl ConnectionCounts {
    fn add(&mut self, other: ConnectionCounts) {
        self.direct = self.direct.saturating_add(other.direct);
        self.relayed = self.relayed.saturating_add(other.relayed);
        self.time_to_direct += other.time_to_direct;
    }

    fn stats(&self) -> ConnectionStats {
        ConnectionStats {
            direct: self.direct,
            relayed: self.relayed,
            avg_time_to_direct_ms: (self.direct > 0).then(|| {
                let avg = self.time_to_direct.as_millis() / u128::from(self.direct);
                u32::try_from(avg).unwrap_or(u32::MAX)
            }),
        }
    }
}

/// Counts the connections of an endpoint to other processors and managers by their path.
#[derive(Default)]
pub(crate) struct ConnectionCounter(Mutex<ConnectionCounts>);

impl ConnectionCounter {
    fn record(&self, counts: ConnectionCounts) {
        self.0.lock().expect("counter lock poisoned").add(counts);
    }

    /// The counts since the last call, reset to zero.
    fn take(&self) -> ConnectionCounts {
        std::mem::take(&mut *self.0.lock().expect("counter lock poisoned"))
    }

    /// Follows `conn` until it uses a direct path or closes, and counts it then. Connections to
    /// relay nodes are not counted: those nodes have public addresses, so they say nothing about
    /// the paths between processors.
    pub(crate) fn track(self: &Arc<Self>, relays: &SharedRelays, conn: Connection) {
        let to_relay_node = relays
            .lock()
            .expect("relay list lock poisoned")
            .iter()
            .any(|relay| relay.id == conn.remote_id());
        if to_relay_node {
            return;
        }
        let counter = self.clone();
        tokio::spawn(async move {
            let opened = Instant::now();
            let mut paths = conn.paths_stream();
            while let Some(list) = paths.next().await {
                if list.iter().any(|path| path.is_ip()) {
                    counter.record(ConnectionCounts {
                        direct: 1,
                        time_to_direct: opened.elapsed(),
                        ..Default::default()
                    });
                    return;
                }
            }
            counter.record(ConnectionCounts {
                relayed: 1,
                ..Default::default()
            });
        });
    }
}

impl P2pEndpoint {
    /// Sends a network report every [`TELEMETRY_INTERVAL_SECS`], the first one after
    /// [`TELEMETRY_FIRST_DELAY_SECS`], until the endpoint closes. A later call replaces the
    /// previous one.
    pub fn start_telemetry(&self, app_version: String) {
        let task = tokio::spawn(run(
            self.inner().clone(),
            self.shared_relays(),
            self.connection_counter(),
            app_version,
        ));
        if let Some(previous) = self.replace_telemetry(task) {
            previous.abort();
        }
    }

    /// Sends a network report now to the relay node of the home relay and returns its reply.
    pub async fn send_telemetry(&self, app_version: &str) -> Result<TelemetryReply, Error> {
        send(
            self.inner(),
            &self.shared_relays(),
            &self.connection_counter(),
            app_version,
        )
        .await
    }
}

async fn run(
    endpoint: Endpoint,
    relays: SharedRelays,
    counter: Arc<ConnectionCounter>,
    app_version: String,
) {
    tokio::time::sleep(Duration::from_secs(TELEMETRY_FIRST_DELAY_SECS)).await;
    let mut interval = tokio::time::interval(Duration::from_secs(TELEMETRY_INTERVAL_SECS));
    loop {
        interval.tick().await;
        match send(&endpoint, &relays, &counter, &app_version).await {
            Ok(reply) => tracing::debug!("network report sent: {reply:?}"),
            Err(e) => tracing::debug!("network report not sent: {e}"),
        }
    }
}

/// Sends the report; the connection counts are kept for the next report if it does not arrive.
async fn send(
    endpoint: &Endpoint,
    relays: &SharedRelays,
    counter: &ConnectionCounter,
    app_version: &str,
) -> Result<TelemetryReply, Error> {
    let counts = counter.take();
    let reply = exchange(endpoint, relays, report(endpoint, counts, app_version)).await;
    if reply.is_err() {
        counter.record(counts);
    }
    reply
}

fn report(endpoint: &Endpoint, counts: ConnectionCounts, app_version: &str) -> TelemetryReport {
    TelemetryReport {
        platform: platform(),
        app_version: crate::manage::truncate(app_version.to_owned()),
        network: endpoint.net_report().get().map(|report| NetworkReport {
            udp_v4: report.udp_v4,
            udp_v6: report.udp_v6,
            global_v4: report.global_v4.is_some(),
            global_v6: report.global_v6.is_some(),
            symmetric_nat: report.mapping_varies_by_dest(),
        }),
        home_relay_connected: endpoint
            .home_relay_status()
            .get()
            .iter()
            .any(|status| status.is_connected()),
        connections: counts.stats(),
    }
}

async fn exchange(
    endpoint: &Endpoint,
    relays: &SharedRelays,
    report: TelemetryReport,
) -> Result<TelemetryReply, Error> {
    let telemetry_error = |e: &dyn std::fmt::Display| Error::Telemetry(e.to_string());
    let node = home_relay_node(endpoint, relays)
        .ok_or_else(|| Error::Telemetry("no home relay".into()))?;
    let conn = endpoint
        .connect(EndpointAddr::new(node), ALPN_TELEMETRY)
        .await?;
    let reply = async {
        let (mut send, mut recv) = conn.open_bi().await.map_err(|e| telemetry_error(&e))?;
        let bytes = frame::encode(&report).map_err(|e| telemetry_error(&e))?;
        send.write_all(&bytes)
            .await
            .map_err(|e| telemetry_error(&e))?;
        send.finish().map_err(|e| telemetry_error(&e))?;
        let bytes = recv
            .read_to_end(HEADER_LEN + MAX_FRAME)
            .await
            .map_err(|e| telemetry_error(&e))?;
        frame::decode::<TelemetryReply>(&bytes)
            .map(|(reply, _)| reply)
            .map_err(|e| telemetry_error(&e))
    }
    .await;
    conn.close(0u32.into(), b"done");
    reply
}

/// The endpoint of the relay node serving the home relay.
fn home_relay_node(endpoint: &Endpoint, relays: &SharedRelays) -> Option<iroh::EndpointId> {
    let home = endpoint.home_relay_status().get();
    let relays = relays.lock().expect("relay list lock poisoned");
    home.iter()
        .find_map(|status| relays.iter().find(|relay| &relay.url == status.url()))
        .map(|relay| relay.id)
}

fn platform() -> TelemetryPlatform {
    if cfg!(target_os = "android") {
        TelemetryPlatform::Android
    } else if cfg!(target_os = "ios") {
        TelemetryPlatform::Ios
    } else {
        TelemetryPlatform::Other
    }
}

/// The task sending the reports, if started.
#[derive(Default)]
pub(crate) struct TelemetryTask(Mutex<Option<JoinHandle<()>>>);

impl TelemetryTask {
    pub(crate) fn replace(&self, task: JoinHandle<()>) -> Option<JoinHandle<()>> {
        self.0
            .lock()
            .expect("telemetry lock poisoned")
            .replace(task)
    }

    pub(crate) fn stop(&self) {
        if let Some(task) = self.0.lock().expect("telemetry lock poisoned").take() {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_give_the_average_time_to_a_direct_path() {
        let counter = ConnectionCounter::default();
        counter.record(ConnectionCounts {
            direct: 1,
            time_to_direct: Duration::from_millis(300),
            ..Default::default()
        });
        counter.record(ConnectionCounts {
            direct: 1,
            time_to_direct: Duration::from_millis(900),
            ..Default::default()
        });
        counter.record(ConnectionCounts {
            relayed: 1,
            ..Default::default()
        });

        let counts = counter.take();
        assert_eq!(
            counts.stats(),
            ConnectionStats {
                direct: 2,
                relayed: 1,
                avg_time_to_direct_ms: Some(600),
            }
        );
        assert_eq!(counter.take(), ConnectionCounts::default());
    }

    #[test]
    fn no_average_without_direct_connections() {
        let counts = ConnectionCounts {
            relayed: 3,
            ..Default::default()
        };
        assert_eq!(counts.stats().avg_time_to_direct_ms, None);
    }

    #[test]
    fn counts_of_a_failed_report_are_kept() {
        let counter = ConnectionCounter::default();
        let counts = ConnectionCounts {
            direct: 2,
            relayed: 1,
            time_to_direct: Duration::from_millis(100),
        };
        counter.record(counts);
        let taken = counter.take();
        counter.record(ConnectionCounts {
            relayed: 1,
            ..Default::default()
        });
        // What `send` does when the report did not arrive.
        counter.record(taken);
        assert_eq!(
            counter.take(),
            ConnectionCounts {
                direct: 2,
                relayed: 2,
                time_to_direct: Duration::from_millis(100),
            }
        );
    }
}
