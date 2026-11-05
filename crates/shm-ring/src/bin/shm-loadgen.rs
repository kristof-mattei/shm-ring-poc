use clap::Parser as _;
use color_eyre::eyre;
use shm_ring::loadgen::{self, LoadgenConfig};
use shm_ring::telemetry;
use tracing::{Level, event};

#[tokio::main]
async fn main() -> Result<(), eyre::Report> {
    color_eyre::install()?;

    // parse before tracing is set up, so clap's help and usage errors are not preceded by log output
    let config = LoadgenConfig::parse();

    telemetry::init()?;

    let report = loadgen::run(&config).await?;

    event!(
        Level::INFO,
        requests = report.requests,
        elapsed = ?report.elapsed,
        requests_per_second = report.requests_per_second(),
        max_in_flight = report.max_in_flight,
        min = ?report.latency.min,
        p50 = ?report.latency.p50,
        p99 = ?report.latency.p99,
        p999 = ?report.latency.p999,
        max = ?report.latency.max,
        "load run complete"
    );

    Ok(())
}
