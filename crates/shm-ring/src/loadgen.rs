//! Process A: creates the region, sends requests, and checks every response.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use std::{io, thread};

use clap::Parser;
use thiserror::Error;
use tokio::net::UnixStream;
use tokio::task::{self, JoinError};
use tokio::time::sleep;
use tracing::{Level, Span, event};

use crate::padded::CachePadded;
use crate::region::{Region, RegionError};
use crate::ring::{Consumer, Producer, RecvError, SendError};
use crate::wait::WaitCell;
use crate::{DEFAULT_SPIN, handshake};

#[derive(Parser)]
#[command(
    name = "shm-loadgen",
    about = "Drives load through shared-memory rings (process A)"
)]
pub struct LoadgenConfig {
    /// Unix socket for the setup handshake.
    #[arg(long, env = "SHM_SOCKET", default_value = "/tmp/shared.socket")]
    pub socket: PathBuf,

    /// Number of requests to send.
    #[arg(long, default_value_t = 1_000_000)]
    pub requests: u64,

    /// Maximum number of requests sent without a response.
    #[arg(long, default_value_t = 1024, value_parser = clap::value_parser!(u32).range(1..=1 << 30))]
    pub max_in_flight: u32,

    /// Payload bytes per request.
    #[arg(long, default_value_t = 64)]
    pub payload_size: usize,

    /// Spin iterations before a waiting thread parks.
    #[arg(long, default_value_t = DEFAULT_SPIN)]
    pub spin: u32,

    /// How long to retry connecting to the socket, in milliseconds.
    #[arg(long, default_value_t = 5000)]
    pub connect_timeout_ms: u64,
}

pub struct Latency {
    pub min: Duration,
    pub p50: Duration,
    pub p99: Duration,
    pub p999: Duration,
    pub max: Duration,
}

impl Latency {
    fn from_samples(samples: &mut [u64]) -> Self {
        samples.sort_unstable();

        let at = |per_mille: usize| {
            let nanos = match samples.len() {
                0 => 0,
                len => samples[(len - 1) * per_mille / 1000],
            };

            Duration::from_nanos(nanos)
        };

        Self {
            min: at(0),
            p50: at(500),
            p99: at(990),
            p999: at(999),
            max: at(1000),
        }
    }
}

pub struct Report {
    pub requests: u64,
    /// From the first send to the last response.
    pub elapsed: Duration,
    /// Highest number of requests observed in flight at once.
    pub max_in_flight: u64,
    /// Round trip per request.
    pub latency: Latency,
}

impl Report {
    #[must_use]
    pub fn requests_per_second(&self) -> u128 {
        match self.elapsed.as_nanos() {
            0 => 0,
            nanos => u128::from(self.requests) * 1_000_000_000 / nanos,
        }
    }
}

#[derive(Debug, Error)]
pub enum LoadError {
    #[error("connecting to the server socket failed")]
    Connect(#[source] io::Error),
    #[error("handshake with the server failed")]
    Handshake(#[source] io::Error),
    #[error("setting up the shared region failed")]
    Region(#[from] RegionError),
    #[error("spawning a worker thread failed")]
    Spawn(#[source] io::Error),
    #[error("the {} thread panicked", .0)]
    WorkerPanicked(&'static str),
    #[error("joining the worker threads failed")]
    Join(#[from] JoinError),
    /// The server disconnected, or the other worker failed.
    #[error("the run was aborted")]
    Aborted,
    #[error("the request ring is full, the server holds more than `max_in_flight` requests")]
    RingFull,
    #[error("the response ring closed after {} of {} responses", .received, .expected)]
    ClosedEarly { received: u64, expected: u64 },
    #[error("response {} arrived where response {} belongs", .received, .expected)]
    OutOfOrder { expected: u64, received: u64 },
    #[error("response {} has a corrupted payload", .id)]
    CorruptedPayload { id: u64 },
    #[error("response {} arrived after the last request", .id)]
    UnexpectedResponse { id: u64 },
}

impl From<SendError> for LoadError {
    fn from(send_error: SendError) -> Self {
        match send_error {
            SendError::Full => LoadError::RingFull,
            SendError::Aborted => LoadError::Aborted,
        }
    }
}

/// Bounds `sent - received` to `cap`. The writer parks here when the cap is reached.
struct CreditGate {
    received: CachePadded<AtomicU64>,
    waiter: CachePadded<WaitCell>,
    aborted: AtomicBool,
    cap: u64,
}

impl CreditGate {
    fn new(cap: u64) -> Self {
        Self {
            received: CachePadded::new(AtomicU64::new(0)),
            waiter: CachePadded::new(WaitCell::new()),
            aborted: AtomicBool::new(false),
            cap,
        }
    }

    /// Waits until request `sent` fits under the cap. Returns the in-flight count including it.
    fn acquire(&self, sent: u64, spin: u32) -> Result<u64, LoadError> {
        let mut ready = || {
            sent - self.received.load(Ordering::Acquire) < self.cap
                || self.aborted.load(Ordering::Acquire)
        };

        if !ready() {
            self.waiter.wait_until(spin, &mut ready);
        }

        if self.aborted.load(Ordering::Acquire) {
            return Err(LoadError::Aborted);
        }

        Ok(sent - self.received.load(Ordering::Acquire) + 1)
    }

    /// Called by the reader after each response.
    fn release(&self) {
        self.received.fetch_add(1, Ordering::Release);
        self.waiter.notify();
    }

    fn abort(&self) {
        self.aborted.store(true, Ordering::Release);
        self.waiter.notify();
    }
}

struct Run {
    region: Arc<Region>,
    credit: CreditGate,
    start: Instant,
    requests: u64,
    payload_size: usize,
    spin: u32,
}

impl Run {
    fn abort(&self) {
        self.region.abort();
        self.credit.abort();
    }
}

struct ReadOutcome {
    samples: Vec<u64>,
    elapsed: Duration,
}

/// Connects to the server, sends `config.requests` requests, and verifies order and content of every response.
///
/// # Errors
///
/// When the handshake fails, the server disconnects mid-run, or a response arrives out of order or corrupted.
pub async fn run(config: &LoadgenConfig) -> Result<Report, LoadError> {
    let mut stream = connect(
        &config.socket,
        Duration::from_millis(config.connect_timeout_ms),
    )
    .await
    .map_err(LoadError::Connect)?;

    let region = Arc::new(Region::create(config.max_in_flight, config.payload_size)?);

    handshake::send_fd(&stream, region.fd())
        .await
        .map_err(LoadError::Handshake)?;
    handshake::recv_ack(&mut stream)
        .await
        .map_err(LoadError::Handshake)?;

    event!(
        Level::INFO,
        capacity = region.capacity(),
        slot_size = region.slot_size(),
        max_in_flight = config.max_in_flight,
        requests = config.requests,
        "region attached"
    );

    let (requests, responses) = region.client_endpoints(config.spin);

    let run = Arc::new(Run {
        region,
        credit: CreditGate::new(u64::from(config.max_in_flight)),
        start: Instant::now(),
        requests: config.requests,
        payload_size: config.payload_size,
        spin: config.spin,
    });

    let writer = spawn_worker("loadgen-writer", &run, move |run| write(run, requests))
        .map_err(LoadError::Spawn)?;
    let reader = spawn_worker("loadgen-reader", &run, move |run| read(run, responses))
        .map_err(LoadError::Spawn)?;

    let mut workers = task::spawn_blocking(move || (writer.join(), reader.join()));

    let (written, read) = tokio::select! {
        joined = &mut workers => joined?,
        closed = handshake::closed(&mut stream) => {
            event!(Level::WARN, ?closed, "server disconnected mid-run");

            run.abort();

            workers.await?
        },
    };

    let max_in_flight = written.map_err(|_panic| LoadError::WorkerPanicked("writer"))??;
    let mut outcome = read.map_err(|_panic| LoadError::WorkerPanicked("reader"))??;

    Ok(Report {
        requests: config.requests,
        elapsed: outcome.elapsed,
        max_in_flight,
        latency: Latency::from_samples(&mut outcome.samples),
    })
}

async fn connect(path: &Path, timeout: Duration) -> io::Result<UnixStream> {
    let deadline = Instant::now() + timeout;

    loop {
        match UnixStream::connect(path).await {
            Ok(stream) => return Ok(stream),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
                ) && Instant::now() < deadline =>
            {
                sleep(Duration::from_millis(10)).await;
            },
            Err(error) => return Err(error),
        }
    }
}

/// Runs `work` on a named thread inside the current span, and aborts the run when `work` fails, so the other worker cannot park forever.
fn spawn_worker<T: Send + 'static, F: FnOnce(&Run) -> Result<T, LoadError> + Send + 'static>(
    name: &str,
    run: &Arc<Run>,
    work: F,
) -> io::Result<thread::JoinHandle<Result<T, LoadError>>> {
    let run = Arc::clone(run);
    let span = Span::current();

    thread::Builder::new().name(name.to_owned()).spawn(move || {
        let _entered = span.enter();

        let result = work(&run);

        if let Err(ref error) = result {
            event!(Level::ERROR, ?error, "worker failed, aborting the run");

            run.abort();
        }

        result
    })
}

/// Returns the highest in-flight count observed.
fn write(run: &Run, mut requests: Producer) -> Result<u64, LoadError> {
    let mut max_in_flight = 0;

    for id in 0..run.requests {
        let in_flight = run.credit.acquire(id, run.spin)?;

        max_in_flight = max_in_flight.max(in_flight);

        let mut slot = requests.reserve()?;

        slot.set_id(id);
        slot.set_sent_at_ns(nanos_since(run.start));
        fill_payload(id, slot.payload_mut(run.payload_size));
        slot.commit();
    }

    event!(Level::DEBUG, sent = run.requests, "all requests sent");

    // dropping `requests` closes the request ring
    Ok(max_in_flight)
}

fn read(run: &Run, mut responses: Consumer) -> Result<ReadOutcome, LoadError> {
    let mut samples =
        Vec::with_capacity(usize::try_from(run.requests).expect("`u64` fits in `usize`"));

    for expected in 0..run.requests {
        let response = match responses.recv() {
            Ok(response) => response,
            Err(RecvError::Closed) => {
                return Err(LoadError::ClosedEarly {
                    received: expected,
                    expected: run.requests,
                });
            },
            Err(RecvError::Aborted) => return Err(LoadError::Aborted),
        };

        if response.id() != expected {
            return Err(LoadError::OutOfOrder {
                expected,
                received: response.id(),
            });
        }

        if !is_reversed_payload(expected, response.payload(), run.payload_size) {
            return Err(LoadError::CorruptedPayload { id: expected });
        }

        samples.push(nanos_since(run.start) - response.sent_at_ns());

        // releasing before granting credit keeps the response ring at or below `max_in_flight` messages
        response.release();
        run.credit.release();
    }

    let elapsed = run.start.elapsed();

    match responses.recv() {
        Err(RecvError::Closed) => Ok(ReadOutcome { samples, elapsed }),
        Err(RecvError::Aborted) => Err(LoadError::Aborted),
        Ok(response) => Err(LoadError::UnexpectedResponse { id: response.id() }),
    }
}

fn nanos_since(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_nanos()).expect("a run lasts less than 584 years")
}

fn pattern_byte(id: u64, offset: u64) -> u8 {
    u8::try_from(id.wrapping_add(offset) & 0xFF).expect("masked to 8 bits")
}

fn fill_payload(id: u64, payload: &mut [u8]) {
    for (byte, offset) in payload.iter_mut().zip(0_u64..) {
        *byte = pattern_byte(id, offset);
    }
}

fn is_reversed_payload(id: u64, payload: &[u8], len: usize) -> bool {
    payload.len() == len
        && payload
            .iter()
            .rev()
            .zip(0_u64..)
            .all(|(byte, offset)| *byte == pattern_byte(id, offset))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use pretty_assertions::assert_eq;

    use super::{Latency, fill_payload, is_reversed_payload};

    #[test]
    fn reversed_payload_round_trips() {
        let mut payload = [0_u8; 300];

        fill_payload(1_234, &mut payload);
        payload.reverse();

        assert!(
            is_reversed_payload(1_234, &payload, 300),
            "reversed pattern is accepted"
        );
        assert!(
            !is_reversed_payload(1_235, &payload, 300),
            "pattern of another id is rejected"
        );
        assert!(
            !is_reversed_payload(1_234, &payload[..299], 300),
            "short payload is rejected"
        );
    }

    #[test]
    fn latency_percentiles_index_sorted_samples() {
        let mut samples: Vec<u64> = (1..=1000).rev().collect();

        let latency = Latency::from_samples(&mut samples);

        assert_eq!(latency.min, Duration::from_nanos(1));
        assert_eq!(latency.p50, Duration::from_nanos(500));
        assert_eq!(latency.p99, Duration::from_nanos(990));
        assert_eq!(latency.p999, Duration::from_nanos(999));
        assert_eq!(latency.max, Duration::from_nanos(1000));
    }
}
