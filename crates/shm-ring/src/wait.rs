use std::hint;
use std::sync::atomic::{AtomicU32, Ordering, fence};

use rustix::io::Errno;
use rustix::thread::futex;
use tracing::{Level, event};

const AWAKE: u32 = 0;
const PARKED: u32 = 1;

// not `PRIVATE`: the futex word may live in a mapping shared by two processes
const FUTEX_FLAGS: futex::Flags = futex::Flags::empty();

/// Single-waiter park and wake cell backed by a futex word.
///
/// The all-zero bit pattern is an awake cell, so a cell is valid in zero-filled shared memory.
#[repr(transparent)]
pub(crate) struct WaitCell {
    state: AtomicU32,
}

impl WaitCell {
    pub(crate) const fn new() -> Self {
        Self {
            state: AtomicU32::new(AWAKE),
        }
    }

    /// Returns once `ready` returns `true`, spinning `spin` times before each park.
    ///
    /// At most one thread waits on a cell at a time.
    pub(crate) fn wait_until<F: FnMut() -> bool>(&self, spin: u32, mut ready: F) {
        loop {
            for _ in 0..spin {
                if ready() {
                    return;
                }

                hint::spin_loop();
            }

            self.state.store(PARKED, Ordering::Relaxed);

            // pairs with the fence in `notify`: either `ready` observes the notifier's store, or `notify` observes `PARKED`
            fence(Ordering::SeqCst);

            if ready() {
                self.state.store(AWAKE, Ordering::Relaxed);

                return;
            }

            park(&self.state);

            self.state.store(AWAKE, Ordering::Relaxed);
        }
    }

    /// Wakes the waiter if it is parked. Call after the store that makes the waiter's condition true.
    pub(crate) fn notify(&self) {
        fence(Ordering::SeqCst);

        if self.state.load(Ordering::Relaxed) == PARKED
            && self.state.swap(AWAKE, Ordering::Relaxed) == PARKED
        {
            event!(Level::TRACE, "waking parked peer");

            let _woken: usize = futex::wake(&self.state, FUTEX_FLAGS, 1)
                .expect("`FUTEX_WAKE` on a valid, aligned word does not fail");
        }
    }
}

fn park(state: &AtomicU32) {
    event!(Level::TRACE, "parking");

    match futex::wait(state, FUTEX_FLAGS, PARKED, None) {
        // `AGAIN`: the word no longer holds `PARKED`, `INTR`: a signal interrupted the wait
        Ok(()) | Err(Errno::AGAIN | Errno::INTR) => {},
        Err(errno) => panic!("`FUTEX_WAIT` failed: {}", errno),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, mpsc};
    use std::thread;
    use std::time::Duration;

    use pretty_assertions::assert_eq;

    use super::WaitCell;

    struct PingPong {
        ping: AtomicU64,
        ping_cell: WaitCell,
        pong: AtomicU64,
        pong_cell: WaitCell,
    }

    #[test]
    fn ping_pong_loses_no_wake_up() {
        const ROUNDS: u64 = 20_000;

        let shared = Arc::new(PingPong {
            ping: AtomicU64::new(0),
            ping_cell: WaitCell::new(),
            pong: AtomicU64::new(0),
            pong_cell: WaitCell::new(),
        });

        let (done_sender, done_receiver) = mpsc::channel();

        let responder = {
            let shared = Arc::clone(&shared);

            thread::spawn(move || {
                for round in 1..=ROUNDS {
                    // spin 0 parks on every round, the worst case for a lost wake-up
                    shared
                        .ping_cell
                        .wait_until(0, || shared.ping.load(Ordering::Acquire) == round);

                    shared.pong.store(round, Ordering::Release);
                    shared.pong_cell.notify();
                }
            })
        };

        let initiator = {
            let shared = Arc::clone(&shared);

            thread::spawn(move || {
                for round in 1..=ROUNDS {
                    shared.ping.store(round, Ordering::Release);
                    shared.ping_cell.notify();

                    shared
                        .pong_cell
                        .wait_until(0, || shared.pong.load(Ordering::Acquire) == round);
                }

                done_sender.send(()).expect("receiver outlives the threads");
            })
        };

        done_receiver
            .recv_timeout(Duration::from_secs(30))
            .expect("ping-pong finishes, a hang means a lost wake-up");

        initiator.join().expect("initiator does not panic");
        responder.join().expect("responder does not panic");

        assert_eq!(shared.pong.load(Ordering::Acquire), ROUNDS);
    }
}
