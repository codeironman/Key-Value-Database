use std::mem::size_of;

use bytes::Bytes;
pub const TS_MAX: u64 = std::u64::MAX;
pub const TS_MIN: u64 = std::u64::MIN;
pub const TS_RANGE_BEGIN: u64 = std::u64::MAX;
pub const TS_RANGE_END: u64 = std::u64::MIN;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct Key<T: AsRef<[u8]>>(pub T, pub u64);

impl Key<Bytes> {
    pub fn data(&self) -> Bytes {
        self.0.clone()
    }
    pub fn ts(&self) -> u64 {
        self.1
    }
    pub fn new(data: &Bytes, ts: u64) -> Self {
        Self(data.clone(), ts)
    }
    pub fn init(key: &Bytes) -> Self {
        Self(key.clone(), 0)
    }
    pub fn len(&self) -> usize {
        self.0.len() + size_of::<u64>()
    }
}
