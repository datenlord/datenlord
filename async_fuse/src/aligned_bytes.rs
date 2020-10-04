//! Inline crate from <https://crates.io/crates/aligned-bytes/0.1.1>
//!
//! A continuous fixed-length byte array with a specified alignment.
//!
//! # Example
//! ```
//! use aligned_bytes::AlignedBytes;
//! let mut bytes = AlignedBytes::new_zeroed(1024, 8);
//! let buf: &mut [u8] = &mut *bytes;
//! ```

use core::ops::{Deref, DerefMut};
use core::ptr::NonNull;
use core::slice;
use std::alloc::{self as alloc_api, Layout};
use utilities::OverflowArithmetic;

/// A continuous fixed-length byte array with a specified alignment.
#[derive(Debug)]
pub struct AlignedBytes {
    /// Alignment size
    align: usize,
    /// Byte data
    bytes: NonNull<[u8]>,
}

unsafe impl Send for AlignedBytes {}
unsafe impl Sync for AlignedBytes {}

impl std::panic::UnwindSafe for AlignedBytes {}

impl std::panic::RefUnwindSafe for AlignedBytes {}

impl AlignedBytes {
    /// Allocate a zero-initialized byte array with an exact alignment.
    pub fn new_zeroed(len: usize, align: usize) -> Self {
        let layout = match Layout::from_size_align(len, align) {
            Ok(layout) => layout,
            Err(e) => panic!(
                "Invalid layout: size = {}, align = {}, the error is: {}",
                len, align, e,
            ),
        };
        let bytes = unsafe {
            let ptr = alloc_api::alloc_zeroed(layout);
            if ptr.is_null() {
                alloc_api::handle_alloc_error(layout);
            }
            let address = std::mem::transmute::<*mut u8, usize>(ptr);
            debug_assert!(
                address.overflow_rem(align) == 0,
                "pointer = {:p} is not a multiple of alignment = {}",
                ptr,
                align
            );
            NonNull::new_unchecked(slice::from_raw_parts_mut(ptr, len))
        };
        Self { align, bytes }
    }

    /// Returns the alignment of the byte array.
    #[allow(dead_code)]
    pub const fn alignment(&self) -> usize {
        self.align
    }
}

impl Drop for AlignedBytes {
    fn drop(&mut self) {
        unsafe {
            let size = self.deref().len();
            let layout = Layout::from_size_align_unchecked(size, self.align);
            let ptr: *mut u8 = self.bytes.as_ptr().cast();
            alloc_api::dealloc(ptr, layout)
        }
    }
}

impl Deref for AlignedBytes {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.bytes.as_ptr() }
    }
}

impl DerefMut for AlignedBytes {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.bytes.as_ptr() }
    }
}

impl AsRef<[u8]> for AlignedBytes {
    fn as_ref(&self) -> &[u8] {
        &*self
    }
}

impl AsMut<[u8]> for AlignedBytes {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut *self
    }
}

#[cfg(test)]
mod tests {
    use super::AlignedBytes;

    #[test]
    fn check_alignment() {
        let align = 4096;
        let bytes = AlignedBytes::new_zeroed(8, align);
        assert_eq!(bytes.alignment(), align);
        let address = unsafe { std::mem::transmute::<*const u8, usize>(bytes.as_ptr()) };
        assert!(address % align == 0);
    }

    #[should_panic(expected = "Invalid layout: size = 1, align = 0")]
    #[test]
    fn check_layout_zero_align() {
        AlignedBytes::new_zeroed(1, 0);
    }

    #[should_panic(expected = "Invalid layout: size = 1, align = 3")]
    #[test]
    fn check_layout_align_not_power_of_2() {
        AlignedBytes::new_zeroed(1, 3);
    }

    #[should_panic]
    #[test]
    fn check_layout_overflow() {
        let size = core::mem::size_of::<usize>() * 8;
        AlignedBytes::new_zeroed((1_usize << (size - 1)) + 1, 1_usize << (size - 1));
    }
}
