//! Process B: maps each client's region and answers its requests in order.

use std::path::{Path, PathBuf};
use std::pin::pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{io, thread};

use clap::Parser;
use color_eyre::eyre::{self, eyre};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::watch;
use tokio::task::{self, JoinError, JoinSet};
use tracing::{Instrument as _, Level, Span, event, span};

use crate::region::Region;
use crate::ring::{Consumer, Producer, RecvError};
use crate::{DEFAULT_SPIN, handshake};

#[derive(Parser)]
#[command(
    name = "shm-server",
    about = "Answers shared-memory ring requests (process B)"
)]
pub struct ServerConfig {
    /// Unix socket for the setup handshake.
    #[arg(long, env = "SHM_SOCKET", default_value = "/tmp/shared.socket")]
    pub socket: PathBuf,

    /// Busy-wait this many nanoseconds per request before responding.
    #[arg(long, default_value_t = 0)]
    pub handle_delay_ns: u64,

    /// Spin iterations before the handler parks on an empty request ring.
    #[arg(long, default_value_t = DEFAULT_SPIN)]
    pub spin: u32,
}

#[derive(Clone, Copy)]
struct HandlerSettings {
    spin: u32,
    delay: Duration,
}

/// Accepts clients on `config.socket` until `shutdown` resolves.
///
/// # Errors
///
/// When the socket cannot be bound, accepting fails, or the socket file cannot be removed on shutdown.
pub async fn run<S: Future<Output = ()>>(
    config: ServerConfig,
    shutdown: S,
) -> Result<(), eyre::Report> {
    remove_stale_socket(&config.socket)?;

    let listener = UnixListener::bind(&config.socket)?;

    event!(Level::INFO, socket = %config.socket.display(), "listening");

    let settings = HandlerSettings {
        spin: config.spin,
        delay: Duration::from_nanos(config.handle_delay_ns),
    };

    let (stop_sender, stop_receiver) = watch::channel(false);
    let mut connections = JoinSet::new();
    let mut next_id = 0_u64;
    let mut shutdown = pin!(shutdown);

    loop {
        tokio::select! {
            () = &mut shutdown => break,
            accepted = listener.accept() => {
                let (stream, _address) = accepted?;
                let connection = span!(Level::INFO, "connection", id = next_id);

                next_id += 1;

                connections.spawn(serve(stream, settings, stop_receiver.clone()).instrument(connection));
            },
            Some(joined) = connections.join_next() => log_connection_end(joined),
        }
    }

    event!(Level::INFO, "shutting down");

    stop_sender.send_replace(true);

    while let Some(joined) = connections.join_next().await {
        log_connection_end(joined);
    }

    remove_stale_socket(&config.socket)?;

    Ok(())
}

fn remove_stale_socket(path: &Path) -> io::Result<()> {
    match std::fs::remove_file(path) {
        Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
        Ok(()) | Err(_) => Ok(()),
    }
}

fn log_connection_end(joined: Result<Result<u64, eyre::Report>, JoinError>) {
    match joined {
        Ok(Ok(handled)) => event!(Level::INFO, handled, "connection finished"),
        Ok(Err(error)) => event!(Level::WARN, ?error, "connection failed"),
        Err(error) => event!(Level::ERROR, ?error, "connection task panicked"),
    }
}

async fn serve(
    mut stream: UnixStream,
    settings: HandlerSettings,
    mut stop: watch::Receiver<bool>,
) -> Result<u64, eyre::Report> {
    let fd = handshake::recv_fd(&stream).await?;
    let region = Arc::new(Region::open(fd)?);

    event!(
        Level::INFO,
        capacity = region.capacity(),
        slot_size = region.slot_size(),
        "region mapped"
    );

    let (requests, responses) = region.server_endpoints(settings.spin);
    let span = Span::current();

    let handler_thread = thread::Builder::new()
        .name("shm-handler".to_owned())
        .spawn(move || {
            let _entered = span.enter();

            handle(requests, responses, settings.delay)
        })?;

    let session = async {
        handshake::send_ack(&mut stream).await?;

        tokio::select! {
            closed = handshake::closed(&mut stream) => {
                event!(Level::DEBUG, ?closed, "client disconnected");
            },
            _stopped = stop.wait_for(|stop| *stop) => {
                event!(Level::INFO, "stopping connection");
            },
        }

        Ok::<(), io::Error>(())
    }
    .await;

    // a finished handler ignores the abort, a parked one wakes up
    region.abort();

    let handled = task::spawn_blocking(move || handler_thread.join())
        .await?
        .map_err(|_panic| eyre!("handler thread panicked"))??;

    session?;

    Ok(handled)
}

/// Answers requests in arrival order until the client closes the request ring.
fn handle(
    mut requests: Consumer,
    mut responses: Producer,
    delay: Duration,
) -> Result<u64, eyre::Report> {
    let mut handled = 0_u64;

    loop {
        let request = match requests.recv() {
            Ok(request) => request,
            Err(RecvError::Closed) => {
                event!(Level::DEBUG, handled, "request ring closed");

                // dropping `responses` closes the response ring
                return Ok(handled);
            },
            Err(recv_error @ RecvError::Aborted) => return Err(recv_error.into()),
        };

        if !delay.is_zero() {
            busy_wait(delay);
        }

        let mut response = responses.reserve()?;
        let payload = request.payload();

        response.set_id(request.id());
        response.set_sent_at_ns(request.sent_at_ns());

        for (target, source) in response
            .payload_mut(payload.len())
            .iter_mut()
            .zip(payload.iter().rev())
        {
            *target = *source;
        }

        // releasing before publishing keeps the request ring at or below `max_in_flight` messages: the client can only send request N + `max_in_flight` after it receives response N
        request.release();
        response.commit();

        handled += 1;
    }
}

fn busy_wait(delay: Duration) {
    let start = Instant::now();

    while start.elapsed() < delay {
        std::hint::spin_loop();
    }
}
