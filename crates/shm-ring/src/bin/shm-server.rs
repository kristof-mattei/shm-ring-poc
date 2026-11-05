use clap::Parser as _;
use color_eyre::eyre;
use shm_ring::server::{self, ServerConfig};
use shm_ring::telemetry;
use tokio::signal::ctrl_c;
use tokio::signal::unix::{SignalKind, signal};
use tracing::{Level, event};

#[tokio::main]
async fn main() -> Result<(), eyre::Report> {
    color_eyre::install()?;

    // parse before tracing is set up, so clap's help and usage errors are not preceded by log output
    let config = ServerConfig::parse();

    telemetry::init()?;

    let mut terminate = signal(SignalKind::terminate())?;

    server::run(config, async move {
        tokio::select! {
            interrupted = ctrl_c() => event!(Level::INFO, ?interrupted, "received SIGINT"),
            _terminated = terminate.recv() => event!(Level::INFO, "received SIGTERM"),
        }
    })
    .await
}
