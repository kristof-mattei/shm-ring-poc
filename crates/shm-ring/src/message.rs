use std::ptr::NonNull;
use std::slice;

/// Fixed header at the start of every slot.
#[repr(C)]
struct MessageHeader {
    id: u64,
    sent_at_ns: u64,
    len: u32,
    reserved: u32,
}

pub(crate) const MESSAGE_HEADER_SIZE: usize = size_of::<MessageHeader>();

/// Write access to one slot before the producer publishes it.
pub(crate) struct SlotMut<'slot> {
    header: &'slot mut MessageHeader,
    payload: &'slot mut [u8],
}

impl SlotMut<'_> {
    /// # Safety
    ///
    /// `slot` points to `slot_size` initialized, 8-byte aligned bytes that nothing else accesses for the returned lifetime, and `slot_size` exceeds `MESSAGE_HEADER_SIZE`.
    pub(crate) unsafe fn from_raw(slot: NonNull<u8>, slot_size: usize) -> Self {
        // SAFETY: the caller guarantees alignment, initialization, and exclusive access
        let header = unsafe { slot.cast::<MessageHeader>().as_mut() };

        // SAFETY: the payload starts right after the header, inside the slot
        let payload_start = unsafe { slot.add(MESSAGE_HEADER_SIZE) };

        // SAFETY: `slot_size - MESSAGE_HEADER_SIZE` exclusive, initialized bytes follow the header
        let payload = unsafe {
            slice::from_raw_parts_mut(payload_start.as_ptr(), slot_size - MESSAGE_HEADER_SIZE)
        };

        Self { header, payload }
    }

    pub(crate) fn set_id(&mut self, id: u64) {
        self.header.id = id;
    }

    pub(crate) fn set_sent_at_ns(&mut self, sent_at_ns: u64) {
        self.header.sent_at_ns = sent_at_ns;
    }

    /// Sets the payload length to `len` and returns those bytes for writing.
    ///
    /// # Panics
    ///
    /// When `len` exceeds the slot's payload capacity.
    pub(crate) fn payload_mut(&mut self, len: usize) -> &mut [u8] {
        self.header.len = u32::try_from(len).expect("payload length fits in `u32`");

        &mut self.payload[..len]
    }
}

/// Read access to one published slot.
pub(crate) struct SlotRef<'slot> {
    header: &'slot MessageHeader,
    payload: &'slot [u8],
}

impl SlotRef<'_> {
    /// # Safety
    ///
    /// `slot` points to `slot_size` initialized, 8-byte aligned bytes that nothing writes for the returned lifetime, and `slot_size` exceeds `MESSAGE_HEADER_SIZE`.
    pub(crate) unsafe fn from_raw(slot: NonNull<u8>, slot_size: usize) -> Self {
        // SAFETY: the caller guarantees alignment, initialization, and the absence of writers
        let header = unsafe { slot.cast::<MessageHeader>().as_ref() };

        // SAFETY: the payload starts right after the header, inside the slot
        let payload_start = unsafe { slot.add(MESSAGE_HEADER_SIZE) };

        // SAFETY: `slot_size - MESSAGE_HEADER_SIZE` initialized bytes follow the header
        let payload = unsafe {
            slice::from_raw_parts(payload_start.as_ptr(), slot_size - MESSAGE_HEADER_SIZE)
        };

        Self { header, payload }
    }

    pub(crate) fn id(&self) -> u64 {
        self.header.id
    }

    pub(crate) fn sent_at_ns(&self) -> u64 {
        self.header.sent_at_ns
    }

    pub(crate) fn payload(&self) -> &[u8] {
        let len = usize::try_from(self.header.len).expect("`u32` fits in `usize`");

        &self.payload[..len]
    }
}
