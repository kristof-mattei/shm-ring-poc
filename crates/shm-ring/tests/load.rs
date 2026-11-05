#![expect(clippy::tests_outside_test_module, reason = "Integration tests")]

use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::Once;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use pretty_assertions::assert_eq;
use shm_ring::loadgen::{self, LoadError, LoadgenConfig};
use tokio::time::{sleep, timeout};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

fn init_tracing() {
    static INIT: Once = Once::new();

    INIT.call_once(|| {
        tracing_subscriber::fmt()
            .with_test_writer()
            .with_thread_names(true)
            .with_env_filter(
                EnvFilter::builder()
                    .with_default_directive(LevelFilter::INFO.into())
                    .from_env_lossy(),
            )
            .init();
    });
}

fn unique_socket_path() -> PathBuf {
    static NEXT: AtomicU32 = AtomicU32::new(0);

    std::env::temp_dir().join(format!(
        "shm-ring-test-{}-{}.socket",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}

/// A spawned `shm-server`, killed on drop.
struct ServerProcess {
    child: Option<Child>,
    socket: PathBuf,
}

impl ServerProcess {
    fn spawn(extra_args: &[&str]) -> Self {
        let socket = unique_socket_path();

        let child = Command::new(env!("CARGO_BIN_EXE_shm-server"))
            .arg("--socket")
            .arg(&socket)
            .args(extra_args)
            .spawn()
            .expect("`shm-server` starts");

        Self {
            child: Some(child),
            socket,
        }
    }

    fn socket(&self) -> &Path {
        &self.socket
    }

    /// Sends `SIGKILL`, so the server gets no chance to clean up.
    fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            child.kill().expect("`shm-server` is killed");
            child.wait().expect("`shm-server` is reaped");
        }
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        self.kill();

        if let Err(error) = std::fs::remove_file(&self.socket)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            eprintln!("failed to remove {}: {}", self.socket.display(), error);
        }
    }
}

fn config(socket: &Path, requests: u64, max_in_flight: u32) -> LoadgenConfig {
    LoadgenConfig {
        socket: socket.to_path_buf(),
        requests,
        max_in_flight,
        payload_size: 64,
        spin: shm_ring::DEFAULT_SPIN,
        connect_timeout_ms: 10_000,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn load_run_preserves_order_under_the_cap() {
    init_tracing();

    let server = ServerProcess::spawn(&[]);

    let report = loadgen::run(&config(server.socket(), 200_000, 64))
        .await
        .expect("load run succeeds");

    assert_eq!(report.requests, 200_000);
    assert!(
        report.max_in_flight <= 64,
        "observed {} in flight with a cap of 64",
        report.max_in_flight
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn slow_server_saturates_the_in_flight_cap() {
    init_tracing();

    let server = ServerProcess::spawn(&["--handle-delay-ns", "20000"]);

    let report = loadgen::run(&config(server.socket(), 2_000, 8))
        .await
        .expect("load run succeeds");

    assert_eq!(report.max_in_flight, 8);
}

#[tokio::test(flavor = "multi_thread")]
async fn sequential_clients_get_separate_regions() {
    init_tracing();

    let server = ServerProcess::spawn(&[]);

    for max_in_flight in [1, 16, 1024] {
        let report = loadgen::run(&config(server.socket(), 20_000, max_in_flight))
            .await
            .expect("load run succeeds");

        assert_eq!(report.requests, 20_000);
    }
}

#[test]
fn binaries_run_end_to_end() {
    let server = ServerProcess::spawn(&[]);

    let status = Command::new(env!("CARGO_BIN_EXE_shm-loadgen"))
        .arg("--socket")
        .arg(server.socket())
        .args(["--requests", "100000", "--max-in-flight", "256"])
        .status()
        .expect("`shm-loadgen` starts");

    assert!(status.success(), "`shm-loadgen` exits with {}", status);
}

#[tokio::test(flavor = "multi_thread")]
async fn killing_the_server_fails_the_run_instead_of_hanging() {
    init_tracing();

    // 100 µs per request, so 1,000,000 requests outlast the kill by far
    let mut server = ServerProcess::spawn(&["--handle-delay-ns", "100000"]);

    let config = config(server.socket(), 1_000_000, 32);
    let run = tokio::spawn(async move { loadgen::run(&config).await });

    sleep(Duration::from_millis(500)).await;
    server.kill();

    let result = timeout(Duration::from_secs(10), run)
        .await
        .expect("run ends after the server dies")
        .expect("run task does not panic");

    let error = result.err().expect("run fails once the server is gone");

    assert!(
        matches!(error, LoadError::Aborted),
        "run fails with an abort, not `{}`",
        error
    );
}
