use std::ops::Deref;

/// Places `T` on its own 128-byte block, so adjacent-line prefetch never pairs it with a neighbour.
#[repr(C, align(128))]
pub(crate) struct CachePadded<T>(T);

impl<T> CachePadded<T> {
    pub(crate) const fn new(value: T) -> Self {
        Self(value)
    }
}

impl<T> Deref for CachePadded<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}
