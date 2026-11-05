//! `tracing` setup shared by both binaries.

use std::io;

use color_eyre::eyre;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;
use tracing_subscriber::{EnvFilter, Layer as _, fmt};

/// Installs a stderr subscriber filtered by `RUST_LOG`, defaulting to `INFO`.
///
/// # Errors
///
/// When `RUST_LOG` does not parse, or a global subscriber is already installed.
pub fn init() -> Result<(), eyre::Report> {
    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env()?;

    tracing_subscriber::registry()
        .with(
            fmt::layer()
                .with_writer(io::stderr)
                .with_thread_names(true)
                .with_filter(filter),
        )
        .try_init()?;

    Ok(())
}
