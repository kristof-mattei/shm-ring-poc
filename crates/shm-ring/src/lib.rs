//! Shared-memory ring buffer IPC between a load generator (process A) and a server (process B).
#![cfg_attr(
    test,
    expect(
        dead_code_pub_in_binary,
        reason = "the unit-test harness is an executable, where `pub` items used only by the binaries and integration tests have no user"
    )
)]

mod handshake;
pub mod loadgen;
mod message;
mod padded;
pub mod region;
mod ring;
pub mod server;
pub mod telemetry;
mod wait;

/// Spin iterations before a waiting thread parks on its futex.
pub const DEFAULT_SPIN: u32 = 4096;
