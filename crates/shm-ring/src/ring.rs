use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use thiserror::Error;

use crate::message::{SlotMut, SlotRef};
use crate::padded::CachePadded;
use crate::region::Region;
use crate::wait::WaitCell;

const OPEN: u32 = 0;
const CLOSED: u32 = 1;
const ABORTED: u32 = 2;

/// Control block at the start of a ring in shared memory. Zero-filled memory is an empty, open ring.
#[repr(C)]
pub(crate) struct RingHeader {
    /// Next index the producer writes. Only the producer stores it.
    tail: CachePadded<AtomicU64>,
    /// Next index the consumer reads. Only the consumer stores it.
    head: CachePadded<AtomicU64>,
    /// The consumer parks here when the ring is empty.
    data_ready: CachePadded<WaitCell>,
    state: CachePadded<AtomicU32>,
}

enum RingState {
    Open,
    Closed,
    Aborted,
}

impl RingHeader {
    fn state(&self) -> RingState {
        match self.state.load(Ordering::Acquire) {
            OPEN => RingState::Open,
            CLOSED => RingState::Closed,
            ABORTED => RingState::Aborted,
            other => unreachable!("ring state {} is not a valid state", other),
        }
    }
}

/// One ring inside a mapped region: control block plus `capacity` slots of `slot_size` bytes.
#[derive(Clone, Copy)]
pub(crate) struct RawRing {
    header: NonNull<RingHeader>,
    slots: NonNull<u8>,
    capacity: u64,
    slot_size: usize,
}

// SAFETY: `RawRing` points into a `MAP_SHARED` mapping that another process accesses concurrently anyway. The ring protocol, not thread affinity, guards every access.
unsafe impl Send for RawRing {}

impl RawRing {
    /// # Safety
    ///
    /// `start` points to a 128-byte aligned `RingHeader` followed by `capacity` slots of `slot_size` bytes, all inside a live mapping. `capacity` is a power of two and `slot_size` is a multiple of 64 above `MESSAGE_HEADER_SIZE`.
    pub(crate) unsafe fn new(start: NonNull<u8>, capacity: u32, slot_size: usize) -> Self {
        // SAFETY: the slots follow the header inside the same mapping
        let slots = unsafe { start.add(size_of::<RingHeader>()) };

        Self {
            header: start.cast::<RingHeader>(),
            slots,
            capacity: u64::from(capacity),
            slot_size,
        }
    }

    fn header(&self) -> &RingHeader {
        // SAFETY: the mapping outlives every `RawRing` of its region, and the header holds only atomics
        unsafe { self.header.as_ref() }
    }

    fn slot(&self, index: u64) -> NonNull<u8> {
        let position =
            usize::try_from(index & (self.capacity - 1)).expect("slot index fits in `usize`");

        // SAFETY: `position < capacity`, so the offset stays inside the slot array
        unsafe { self.slots.add(position * self.slot_size) }
    }

    pub(crate) fn abort(&self) {
        let header = self.header();

        header.state.store(ABORTED, Ordering::Release);
        header.data_ready.notify();
    }
}

#[derive(Debug, Error)]
pub(crate) enum SendError {
    /// A producer found `capacity` unreleased messages. The in-flight cap makes this unreachable unless a peer violates it.
    #[error("ring is full")]
    Full,
    #[error("ring was aborted")]
    Aborted,
}

#[derive(Debug, Error)]
pub(crate) enum RecvError {
    /// The producer closed the ring and every message has been read.
    #[error("ring is closed")]
    Closed,
    #[error("ring was aborted")]
    Aborted,
}

/// Write side of a ring. Dropping it closes the ring.
pub(crate) struct Producer {
    ring: RawRing,
    tail: u64,
    cached_head: u64,
    _region: Arc<Region>,
}

impl Producer {
    pub(crate) fn new(region: Arc<Region>, ring: RawRing) -> Self {
        let header = ring.header();
        let tail = header.tail.load(Ordering::Relaxed);
        let cached_head = header.head.load(Ordering::Acquire);

        Self {
            ring,
            tail,
            cached_head,
            _region: region,
        }
    }

    /// Claims the next slot for writing. The consumer sees it only after [`Reservation::commit`].
    pub(crate) fn reserve(&mut self) -> Result<Reservation<'_>, SendError> {
        if let RingState::Aborted = self.ring.header().state() {
            return Err(SendError::Aborted);
        }

        if self.tail - self.cached_head == self.ring.capacity {
            self.cached_head = self.ring.header().head.load(Ordering::Acquire);

            if self.tail - self.cached_head == self.ring.capacity {
                return Err(SendError::Full);
            }
        }

        let slot = self.ring.slot(self.tail);

        // SAFETY: `tail - head < capacity`, so the consumer released this slot and does not read it before `tail` moves past it
        let slot = unsafe { SlotMut::from_raw(slot, self.ring.slot_size) };

        Ok(Reservation {
            producer: self,
            slot,
        })
    }
}

/// A claimed, unpublished slot. Dropping it without [`Reservation::commit`] leaves the ring unchanged.
pub(crate) struct Reservation<'producer> {
    producer: &'producer mut Producer,
    slot: SlotMut<'producer>,
}

impl Reservation<'_> {
    /// Publishes the slot and wakes the consumer if it is parked.
    pub(crate) fn commit(self) {
        let producer = self.producer;

        producer.tail += 1;

        let header = producer.ring.header();

        header.tail.store(producer.tail, Ordering::Release);
        header.data_ready.notify();
    }
}

impl<'producer> Deref for Reservation<'producer> {
    type Target = SlotMut<'producer>;

    fn deref(&self) -> &Self::Target {
        &self.slot
    }
}

impl DerefMut for Reservation<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.slot
    }
}

impl Drop for Producer {
    fn drop(&mut self) {
        let header = self.ring.header();

        match header
            .state
            .compare_exchange(OPEN, CLOSED, Ordering::Release, Ordering::Relaxed)
        {
            Ok(_open) => header.data_ready.notify(),
            // already aborted, and the abort woke the consumer
            Err(_aborted) => {},
        }
    }
}

/// Read side of a ring.
pub(crate) struct Consumer {
    ring: RawRing,
    head: u64,
    cached_tail: u64,
    spin: u32,
    _region: Arc<Region>,
}

impl Consumer {
    pub(crate) fn new(region: Arc<Region>, ring: RawRing, spin: u32) -> Self {
        let header = ring.header();
        let head = header.head.load(Ordering::Relaxed);
        let cached_tail = header.tail.load(Ordering::Acquire);

        Self {
            ring,
            head,
            cached_tail,
            spin,
            _region: region,
        }
    }

    /// Waits for the next message. The producer can reuse its slot once the returned guard is released or dropped.
    pub(crate) fn recv(&mut self) -> Result<Received<'_>, RecvError> {
        let ring = self.ring;
        let header = ring.header();

        while !self.poll_readable()? {
            let head = self.head;

            header.data_ready.wait_until(self.spin, || {
                header.tail.load(Ordering::Acquire) != head
                    || !matches!(header.state(), RingState::Open)
            });
        }

        let slot = ring.slot(self.head);

        // SAFETY: `head < tail`, so the producer published this slot and does not write it before `head` moves past it
        let slot = unsafe { SlotRef::from_raw(slot, ring.slot_size) };

        Ok(Received {
            consumer: self,
            slot,
        })
    }

    /// Returns `Ok(true)` when a message is available, `Ok(false)` when the ring is open and empty.
    fn poll_readable(&mut self) -> Result<bool, RecvError> {
        let header = self.ring.header();

        // loaded before `tail`: a `Closed` seen here guarantees the `tail` load below sees the final `tail`
        let state = header.state();

        if let RingState::Aborted = state {
            return Err(RecvError::Aborted);
        }

        if self.head != self.cached_tail {
            return Ok(true);
        }

        self.cached_tail = header.tail.load(Ordering::Acquire);

        if self.head != self.cached_tail {
            return Ok(true);
        }

        match state {
            RingState::Open => Ok(false),
            RingState::Closed | RingState::Aborted => Err(RecvError::Closed),
        }
    }
}

/// A message read in place. Dropping it releases the slot to the producer.
pub(crate) struct Received<'consumer> {
    consumer: &'consumer mut Consumer,
    slot: SlotRef<'consumer>,
}

impl Received<'_> {
    /// Releases the slot to the producer. Same as dropping, spelled out where the ordering matters.
    pub(crate) fn release(self) {
        drop(self);
    }
}

impl<'consumer> Deref for Received<'consumer> {
    type Target = SlotRef<'consumer>;

    fn deref(&self) -> &Self::Target {
        &self.slot
    }
}

impl Drop for Received<'_> {
    fn drop(&mut self) {
        let consumer = &mut *self.consumer;

        consumer.head += 1;
        consumer
            .ring
            .header()
            .head
            .store(consumer.head, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, mpsc};
    use std::time::Duration;
    use std::{hint, thread};

    use pretty_assertions::assert_eq;

    use super::{Consumer, Producer, RecvError, SendError};
    use crate::region::Region;

    const SPIN: u32 = 64;

    fn send_id(producer: &mut Producer, id: u64) -> Result<(), SendError> {
        let mut slot = producer.reserve()?;

        slot.set_id(id);
        slot.commit();

        Ok(())
    }

    fn recv_id(consumer: &mut Consumer) -> Result<u64, RecvError> {
        consumer.recv().map(|received| received.id())
    }

    #[test]
    fn fifo_across_threads_with_wraparound() {
        const MESSAGES: u64 = 1_000_000;

        let region = Arc::new(Region::create(256, 16).expect("region is created"));
        let (mut producer, _responses) = region.client_endpoints(SPIN);
        let (mut consumer, _response_producer) = region.server_endpoints(SPIN);

        let writer = thread::spawn(move || {
            for id in 0..MESSAGES {
                let mut slot = loop {
                    match producer.reserve() {
                        Ok(slot) => break slot,
                        Err(SendError::Full) => hint::spin_loop(),
                        Err(SendError::Aborted) => panic!("ring aborted mid-test"),
                    }
                };

                slot.set_id(id);
                slot.payload_mut(8).copy_from_slice(&[0xA5; 8]);
                slot.commit();
            }
        });

        for expected in 0..MESSAGES {
            let received = consumer
                .recv()
                .expect("producer stays open until every message is read");

            assert_eq!(received.id(), expected);
            assert_eq!(received.payload(), [0xA5; 8]);
        }

        writer.join().expect("writer does not panic");

        assert!(
            matches!(recv_id(&mut consumer), Err(RecvError::Closed)),
            "dropping the producer closes the ring"
        );
    }

    #[test]
    fn reserve_on_full_ring_returns_full() {
        let region = Arc::new(Region::create(4, 8).expect("region is created"));
        let (mut producer, _responses) = region.client_endpoints(SPIN);
        let (mut consumer, _response_producer) = region.server_endpoints(SPIN);

        for id in 0..4 {
            send_id(&mut producer, id).expect("ring has room for 4 messages");
        }

        assert!(
            matches!(send_id(&mut producer, 4), Err(SendError::Full)),
            "fifth message does not fit"
        );

        let received = consumer.recv().expect("message 0 is readable");

        assert!(
            matches!(send_id(&mut producer, 4), Err(SendError::Full)),
            "a slot stays claimed until its receipt is released"
        );

        received.release();

        send_id(&mut producer, 4).expect("releasing one message frees one slot");
    }

    #[test]
    fn uncommitted_reservation_publishes_nothing() {
        let region = Arc::new(Region::create(4, 8).expect("region is created"));
        let (mut producer, _responses) = region.client_endpoints(SPIN);
        let (mut consumer, _response_producer) = region.server_endpoints(SPIN);

        {
            let mut abandoned = producer.reserve().expect("ring has room");

            abandoned.set_id(99);
        }

        send_id(&mut producer, 1).expect("ring has room");
        drop(producer);

        assert_eq!(
            recv_id(&mut consumer).expect("committed message is readable"),
            1
        );
        assert!(
            matches!(recv_id(&mut consumer), Err(RecvError::Closed)),
            "the abandoned slot never reaches the consumer"
        );
    }

    #[test]
    fn closed_ring_drains_before_reporting_closed() {
        let region = Arc::new(Region::create(8, 8).expect("region is created"));
        let (mut producer, _responses) = region.client_endpoints(SPIN);
        let (mut consumer, _response_producer) = region.server_endpoints(SPIN);

        for id in 0..3 {
            send_id(&mut producer, id).expect("ring has room");
        }

        drop(producer);

        for expected in 0..3 {
            assert_eq!(
                recv_id(&mut consumer).expect("queued messages survive close"),
                expected
            );
        }

        assert!(
            matches!(recv_id(&mut consumer), Err(RecvError::Closed)),
            "drained ring reports closed"
        );
    }

    #[test]
    fn abort_wakes_a_parked_consumer() {
        let region = Arc::new(Region::create(8, 8).expect("region is created"));
        let (_producer, _responses) = region.client_endpoints(SPIN);
        // spin 0 parks on the first empty check
        let (mut consumer, _response_producer) = region.server_endpoints(0);

        let (result_sender, result_receiver) = mpsc::channel();

        let reader = thread::spawn(move || {
            result_sender
                .send(recv_id(&mut consumer))
                .expect("receiver outlives the reader");
        });

        thread::sleep(Duration::from_millis(50));
        region.abort();

        let result = result_receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("abort wakes the parked consumer");

        reader.join().expect("reader does not panic");

        assert!(
            matches!(result, Err(RecvError::Aborted)),
            "parked consumer observes the abort"
        );
    }
}
