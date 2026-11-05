use std::ffi::c_void;
use std::os::fd::{AsFd as _, BorrowedFd, OwnedFd};
use std::ptr::{self, NonNull};
use std::sync::Arc;

use rustix::fs::{self, MemfdFlags};
use rustix::io::Errno;
use rustix::mm::{self, MapFlags, ProtFlags};
use thiserror::Error;
use tracing::{Level, event};

use crate::message::MESSAGE_HEADER_SIZE;
use crate::ring::{Consumer, Producer, RawRing, RingHeader};

const MAGIC: u64 = 0x5348_4D52_494E_4731;
const VERSION: u32 = 1;

const REGION_HEADER_SIZE: usize = 128;
const RING_ALIGN: usize = 128;
const SLOT_ALIGN: usize = 64;

/// Written once by the creator before the fd leaves the process, read-only afterwards.
#[repr(C)]
struct RegionHeader {
    magic: u64,
    version: u32,
    capacity: u32,
    slot_size: u32,
    reserved: u32,
    requests: u64,
    responses: u64,
    len: u64,
}

const _: () = assert!(
    size_of::<RegionHeader>() <= REGION_HEADER_SIZE,
    "region header fits before the first ring"
);
const _: () = assert!(
    align_of::<RingHeader>() == RING_ALIGN,
    "ring header alignment matches `RING_ALIGN`"
);

/// Byte offsets of everything inside a region.
#[derive(Clone, Copy, PartialEq, Eq)]
struct Layout {
    capacity: u32,
    slot_size: u32,
    requests: usize,
    responses: usize,
    len: usize,
}

impl Layout {
    fn new(capacity: u32, slot_size: u32) -> Option<Self> {
        let slot = usize::try_from(slot_size).ok()?;

        if !capacity.is_power_of_two()
            || slot <= MESSAGE_HEADER_SIZE
            || !slot.is_multiple_of(SLOT_ALIGN)
        {
            return None;
        }

        let ring_len = usize::try_from(capacity)
            .ok()?
            .checked_mul(slot)?
            .checked_add(size_of::<RingHeader>())?
            .checked_next_multiple_of(RING_ALIGN)?;

        let requests = REGION_HEADER_SIZE;
        let responses = requests.checked_add(ring_len)?;
        let len = responses.checked_add(ring_len)?;

        Some(Self {
            capacity,
            slot_size,
            requests,
            responses,
            len,
        })
    }

    fn from_header(header: &RegionHeader) -> Option<Self> {
        let layout = Self::new(header.capacity, header.slot_size)?;

        let matches = u64::try_from(layout.requests).ok()? == header.requests
            && u64::try_from(layout.responses).ok()? == header.responses
            && u64::try_from(layout.len).ok()? == header.len;

        matches.then_some(layout)
    }
}

#[derive(Debug, Error)]
pub enum RegionError {
    #[error("region syscall failed")]
    Io(#[from] Errno),
    /// `max_in_flight` or `payload_size` produce a region that does not fit in memory.
    #[error("region size overflows")]
    TooLarge,
    #[error("region of {} bytes is smaller than its header", .len)]
    TooSmall { len: usize },
    #[error("bad region magic {:#018x}", .found)]
    BadMagic { found: u64 },
    #[error("region version {} is not version {}", .found, VERSION)]
    BadVersion { found: u32 },
    #[error("region header describes an inconsistent layout")]
    BadLayout,
    #[error("region header declares {} bytes but the memfd holds {} bytes", .header, .file)]
    BadLength { header: u64, file: usize },
}

/// A `MAP_SHARED` mapping, unmapped on drop.
struct Mapping {
    base: NonNull<u8>,
    len: usize,
}

// SAFETY: the mapping stays valid until `drop`, every shared field is atomic, and the ring protocol coordinates slot access
unsafe impl Send for Mapping {}
// SAFETY: see `Send`
unsafe impl Sync for Mapping {}

impl Mapping {
    fn new(fd: &OwnedFd, len: usize) -> Result<Self, RegionError> {
        // SAFETY: a fresh mapping at a kernel-chosen address aliases no existing Rust object
        let address = unsafe {
            mm::mmap(
                ptr::null_mut(),
                len,
                ProtFlags::READ | ProtFlags::WRITE,
                // prefault, so the first pass over the rings takes no page faults
                MapFlags::SHARED | MapFlags::POPULATE,
                fd,
                0,
            )
        }?;

        Ok(Self {
            base: NonNull::new(address.cast::<u8>())
                .expect("`mmap` does not return null on success"),
            len,
        })
    }

    fn header(&self) -> &RegionHeader {
        // SAFETY: the mapping is page-aligned and spans at least `REGION_HEADER_SIZE` bytes (checked before mapping), and the creator writes the header before the fd leaves its process
        unsafe { self.base.cast::<RegionHeader>().as_ref() }
    }
}

impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: `base` and `len` come from `mmap`, and every `Producer` and `Consumer` holds an `Arc` to the owning region
        if let Err(errno) = unsafe { mm::munmap(self.base.as_ptr().cast::<c_void>(), self.len) } {
            event!(Level::ERROR, %errno, "munmap failed");
        }
    }
}

/// A memfd mapped into this process. Each process maps the memfd at its own address, so the region holds offsets, never pointers.
pub(crate) struct Region {
    mapping: Mapping,
    layout: Layout,
    fd: OwnedFd,
}

impl Region {
    /// Creates a region whose rings hold `max_in_flight` messages of `payload_size` bytes without ever filling up.
    pub(crate) fn create(max_in_flight: u32, payload_size: usize) -> Result<Self, RegionError> {
        let capacity = max_in_flight
            .checked_next_power_of_two()
            .ok_or(RegionError::TooLarge)?;

        let slot_size = payload_size
            .checked_add(MESSAGE_HEADER_SIZE)
            .and_then(|size| size.checked_next_multiple_of(SLOT_ALIGN))
            .and_then(|size| u32::try_from(size).ok())
            .ok_or(RegionError::TooLarge)?;

        let layout = Layout::new(capacity, slot_size).ok_or(RegionError::TooLarge)?;

        let fd = fs::memfd_create("shm-ring", MemfdFlags::CLOEXEC)?;

        fs::ftruncate(
            &fd,
            u64::try_from(layout.len).map_err(|_overflow| RegionError::TooLarge)?,
        )?;

        let mapping = Mapping::new(&fd, layout.len)?;

        let header = RegionHeader {
            magic: MAGIC,
            version: VERSION,
            capacity: layout.capacity,
            slot_size: layout.slot_size,
            reserved: 0,
            requests: u64::try_from(layout.requests).map_err(|_overflow| RegionError::TooLarge)?,
            responses: u64::try_from(layout.responses)
                .map_err(|_overflow| RegionError::TooLarge)?,
            len: u64::try_from(layout.len).map_err(|_overflow| RegionError::TooLarge)?,
        };

        // SAFETY: the mapping starts page-aligned and spans more than `REGION_HEADER_SIZE` bytes, and the fd has not left this process
        unsafe {
            mapping.base.cast::<RegionHeader>().write(header);
        }

        event!(
            Level::DEBUG,
            capacity = layout.capacity,
            slot_size = layout.slot_size,
            len = layout.len,
            "region created"
        );

        Ok(Self {
            mapping,
            layout,
            fd,
        })
    }

    /// Maps a region received from the peer and validates its header against the memfd size.
    pub(crate) fn open(fd: OwnedFd) -> Result<Self, RegionError> {
        let stat = fs::fstat(&fd)?;
        let file_len = usize::try_from(stat.st_size).map_err(|_negative| RegionError::BadLayout)?;

        if file_len < REGION_HEADER_SIZE {
            return Err(RegionError::TooSmall { len: file_len });
        }

        let mapping = Mapping::new(&fd, file_len)?;
        let layout = validate(mapping.header(), file_len)?;

        Ok(Self {
            mapping,
            layout,
            fd,
        })
    }

    pub(crate) fn fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }

    pub(crate) fn capacity(&self) -> u32 {
        self.layout.capacity
    }

    pub(crate) fn slot_size(&self) -> u32 {
        self.layout.slot_size
    }

    /// Request producer and response consumer, for process A.
    pub(crate) fn client_endpoints(self: &Arc<Self>, spin: u32) -> (Producer, Consumer) {
        (
            Producer::new(Arc::clone(self), self.requests()),
            Consumer::new(Arc::clone(self), self.responses(), spin),
        )
    }

    /// Request consumer and response producer, for process B.
    pub(crate) fn server_endpoints(self: &Arc<Self>, spin: u32) -> (Consumer, Producer) {
        (
            Consumer::new(Arc::clone(self), self.requests(), spin),
            Producer::new(Arc::clone(self), self.responses()),
        )
    }

    /// Marks both rings aborted and wakes every thread parked on them, in both processes.
    pub(crate) fn abort(&self) {
        self.requests().abort();
        self.responses().abort();
    }

    fn requests(&self) -> RawRing {
        self.ring(self.layout.requests)
    }

    fn responses(&self) -> RawRing {
        self.ring(self.layout.responses)
    }

    fn ring(&self, offset: usize) -> RawRing {
        // SAFETY: `Layout` places a ring of `capacity` slots at `offset`, inside the mapping
        let start = unsafe { self.mapping.base.add(offset) };

        let slot_size = usize::try_from(self.layout.slot_size).expect("`u32` fits in `usize`");

        // SAFETY: `Layout::new` validated alignment, capacity, and slot size
        unsafe { RawRing::new(start, self.layout.capacity, slot_size) }
    }
}

fn validate(header: &RegionHeader, file_len: usize) -> Result<Layout, RegionError> {
    if header.magic != MAGIC {
        return Err(RegionError::BadMagic {
            found: header.magic,
        });
    }

    if header.version != VERSION {
        return Err(RegionError::BadVersion {
            found: header.version,
        });
    }

    if u64::try_from(file_len).ok() != Some(header.len) {
        return Err(RegionError::BadLength {
            header: header.len,
            file: file_len,
        });
    }

    Layout::from_header(header).ok_or(RegionError::BadLayout)
}

#[cfg(test)]
mod tests {
    use std::mem::offset_of;
    use std::os::fd::OwnedFd;
    use std::sync::Arc;

    use pretty_assertions::assert_eq;
    use rustix::fs;

    use super::{Region, RegionError, RegionHeader};

    fn clone_fd(region: &Region) -> OwnedFd {
        region.fd().try_clone_to_owned().expect("fd is duplicated")
    }

    #[test]
    fn open_accepts_a_created_region() {
        let created = Region::create(100, 40).expect("region is created");
        let opened = Region::open(clone_fd(&created)).expect("region opens");

        assert_eq!(opened.capacity(), 128);
        assert_eq!(opened.slot_size(), 64);
    }

    #[test]
    fn open_rejects_bad_magic() {
        let created = Region::create(8, 8).expect("region is created");

        // SAFETY: `magic` starts the header at `base`, and no ring is in use
        unsafe {
            created.mapping.base.cast::<u64>().write(0);
        }

        assert!(
            matches!(
                Region::open(clone_fd(&created)),
                Err(RegionError::BadMagic { found: 0 })
            ),
            "zeroed magic is rejected"
        );
    }

    #[test]
    fn open_rejects_bad_version() {
        let created = Region::create(8, 8).expect("region is created");

        // SAFETY: `version` lies inside the header
        let version = unsafe { created.mapping.base.add(offset_of!(RegionHeader, version)) };

        // SAFETY: `version` is a 4-byte aligned `u32` field, and no ring is in use
        unsafe {
            version.cast::<u32>().write(99);
        }

        assert!(
            matches!(
                Region::open(clone_fd(&created)),
                Err(RegionError::BadVersion { found: 99 })
            ),
            "unknown version is rejected"
        );
    }

    #[test]
    fn open_rejects_a_resized_memfd() {
        let created = Region::create(8, 8).expect("region is created");
        let fd = clone_fd(&created);

        let len = fs::fstat(&fd).expect("fstat succeeds").st_size;

        fs::ftruncate(&fd, u64::try_from(len + 4096).expect("length is positive"))
            .expect("memfd grows");

        assert!(
            matches!(Region::open(fd), Err(RegionError::BadLength { .. })),
            "a memfd longer than the header declares is rejected"
        );
    }

    #[test]
    fn two_mappings_share_the_rings() {
        let created = Arc::new(Region::create(8, 8).expect("region is created"));
        let opened = Arc::new(Region::open(clone_fd(&created)).expect("region opens"));

        let (mut requests, _responses) = created.client_endpoints(0);
        let (mut received, _response_producer) = opened.server_endpoints(0);

        let mut slot = requests.reserve().expect("ring has room");

        slot.set_id(7);
        slot.payload_mut(3).copy_from_slice(b"abc");
        slot.commit();

        let message = received.recv().expect("message crosses mappings");

        assert_eq!(message.id(), 7);
        assert_eq!(message.payload(), b"abc");
    }
}
