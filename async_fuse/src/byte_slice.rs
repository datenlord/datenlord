use anyhow::Context;
use std::ffi::{CStr, OsStr};
use std::mem;
use std::slice;

use super::protocol::FuseAbiData;

/// # Safety
/// + ensure that `bytes.as_ptr()` is well-aligned for T.
/// + ensure that `bytes.len()` is equal to `size_of::<T>()`.
unsafe fn transmute_ref_unchecked<T: FuseAbiData>(bytes: &[u8]) -> &T {
    &*(bytes.as_ptr().cast::<T>())
}

/// # Safety
/// + ensure that `bytes.as_ptr()` is well-aligned for T.
/// + ensure that `bytes.len()` is a multiple of `size_of::<T>()`.
unsafe fn transmute_slice_unchecked<T: FuseAbiData>(bytes: &[u8]) -> &[T] {
    let len = bytes.len() / mem::size_of::<T>();
    slice::from_raw_parts(bytes.as_ptr().cast::<T>(), len)
}

/// A slice of bytes which is used for parsing FUSE abi data.
#[derive(Debug)]
pub(crate) struct ByteSlice<'a> {
    data: &'a [u8],
}

impl<'a> ByteSlice<'a> {
    pub(crate) fn new(data: &'a [u8]) -> ByteSlice<'a> {
        ByteSlice { data }
    }

    #[allow(dead_code)]
    pub(crate) fn len(&self) -> usize {
        self.data.len()
    }

    pub(crate) fn fetch_all(&mut self) -> &'a [u8] {
        mem::replace(&mut self.data, &[])
    }

    pub(crate) fn fetch_bytes(&mut self, amt: usize) -> anyhow::Result<&'a [u8]> {
        if amt > self.data.len() {
            anyhow::bail!(
                "no enough bytes to fetch, remaining {} bytes but to fetch {} bytes",
                self.data.len(),
                amt
            );
        }
        let (bytes, remain) = self.data.split_at(amt);
        self.data = remain;
        Ok(bytes)
    }

    pub(crate) fn fetch<T: FuseAbiData>(&mut self) -> anyhow::Result<&'a T> {
        let elem_len: usize = mem::size_of::<T>();

        if (self.data.as_ptr() as usize) % mem::align_of::<T>() != 0 {
            anyhow::bail!(
                "failed to convert bytes to type {}, \
                    pointer={:p} is not a multiple of alignment={}",
                std::any::type_name::<T>(),
                self.data.as_ptr(),
                mem::align_of::<T>()
            );
        }

        let bytes: &[u8] = self.fetch_bytes(elem_len).with_context(|| {
            format!(
                "failed to build FUSE request payload type {}",
                std::any::type_name::<T>(),
            )
        })?;

        let ret: &T = unsafe { transmute_ref_unchecked(bytes) };
        Ok(ret)
    }

    #[allow(dead_code)]
    pub(crate) fn fetch_all_as_slice<T: FuseAbiData>(&mut self) -> anyhow::Result<&'a [T]> {
        let elem_len: usize = mem::size_of::<T>();

        if self.data.len() % elem_len != 0 {
            anyhow::bail!(
                "failed to convert bytes to a slice of type={}, \
                    the total bytes length={} % the type size={} is nonzero",
                std::any::type_name::<T>(),
                self.data.len(),
                elem_len,
            );
        }

        if (self.data.as_ptr() as usize) % mem::align_of::<T>() != 0 {
            anyhow::bail!(
                "failed to convert bytes to a slice of type={}, \
                    pointer={:p} is not a multiple of alignment={}",
                std::any::type_name::<T>(),
                self.data.as_ptr(),
                mem::align_of::<T>(),
            );
        }

        let bytes: &[u8] = self.fetch_all();
        let ret = unsafe { transmute_slice_unchecked(bytes) };
        Ok(ret)
    }

    pub(crate) fn fetch_c_str(&mut self) -> anyhow::Result<&'a CStr> {
        let strlen: usize = match memchr::memchr(b'\0', self.data) {
            Some(nul_pos) => nul_pos + 1,
            None => anyhow::bail!("no trailing zero in bytes, cannot fetch c-string"),
        };
        let bytes: &[u8] = self.fetch_bytes(strlen)?;
        let ret: &CStr = unsafe { CStr::from_bytes_with_nul_unchecked(bytes) };
        Ok(ret)
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    pub(crate) fn fetch_os_str(&mut self) -> anyhow::Result<&'a OsStr> {
        let c_str: &CStr = self.fetch_c_str()?;
        let bytes_without_nul: &[u8] = c_str.to_bytes();
        let ret: &OsStr = std::os::unix::ffi::OsStrExt::from_bytes(bytes_without_nul);
        Ok(ret)
    }
}

#[cfg(test)]
mod tests {
    use super::ByteSlice;
    use std::ffi::CStr;

    #[repr(align(8))]
    struct Align8<T: Sized>(T);

    impl<T: Sized> std::ops::Deref for Align8<T> {
        type Target = T;
        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    #[test]
    fn byte_slice_fetch_all() {
        let buf: [u8; 8] = [0; 8];
        let mut byte_slice = ByteSlice::new(&buf);
        assert_eq!(byte_slice.fetch_all(), &[0; 8]);
        assert_eq!(byte_slice.len(), 0);
    }

    #[test]
    fn byte_slice_fetch_bytes() {
        let buf: [u8; 8] = [0; 8];
        let mut byte_slice = ByteSlice::new(&buf);
        assert_eq!(byte_slice.fetch_bytes(5).unwrap(), &[0; 5]);
        assert_eq!(byte_slice.len(), 3);

        assert!(byte_slice.fetch_bytes(5).is_err());
        assert_eq!(byte_slice.len(), 3);
    }

    #[test]
    fn byte_slice_fetch() {
        // this buffer contains two `u32` or one `u64`
        // so it is aligned to 8 bytes
        let buf: Align8<[u8; 8]> = Align8([0, 1, 2, 3, 4, 5, 6, 7]);

        let mut byte_slice = ByteSlice::new(&*buf);
        assert_eq!(
            byte_slice.fetch::<u32>().unwrap(),
            &u32::from_ne_bytes([0, 1, 2, 3])
        );
        assert_eq!(byte_slice.len(), 4);

        let mut byte_slice = ByteSlice::new(&*buf);
        assert_eq!(
            byte_slice.fetch::<u64>().unwrap(),
            &u64::from_ne_bytes([0, 1, 2, 3, 4, 5, 6, 7])
        );
        assert_eq!(byte_slice.len(), 0);
    }

    #[test]
    fn byte_slice_fetch_all_as_slice() {
        // this buffer contains two `u32`
        // so it can be aligned to 4 bytes
        // it is aligned to 8 bytes here
        let buf: Align8<[u8; 8]> = Align8([0, 1, 2, 3, 4, 5, 6, 7]);

        let mut byte_slice = ByteSlice::new(&*buf);
        assert_eq!(
            byte_slice.fetch_all_as_slice::<u32>().unwrap(),
            &[
                u32::from_ne_bytes([0, 1, 2, 3]),
                u32::from_ne_bytes([4, 5, 6, 7]),
            ]
        );
        assert_eq!(byte_slice.len(), 0);

        let mut byte_slice = ByteSlice::new(&buf[..5]);
        assert_eq!(
            byte_slice
                .fetch_all_as_slice::<u32>()
                .unwrap_err()
                .to_string(),
            "failed to convert bytes to a slice of type=u32, the total bytes length=5 % the type size=4 is nonzero"
        );
        assert_eq!(byte_slice.len(), 5)
    }

    #[test]
    fn byte_slice_fetch_c_str() {
        let buf: [u8; 12] = *b"hello\0world\0";

        let mut byte_slice = ByteSlice::new(&buf);
        assert_eq!(
            byte_slice.fetch_c_str().unwrap(),
            CStr::from_bytes_with_nul(b"hello\0".as_ref()).unwrap(),
        );
        assert_eq!(
            byte_slice.fetch_c_str().unwrap(),
            CStr::from_bytes_with_nul(b"world\0".as_ref()).unwrap(),
        );
        assert_eq!(byte_slice.len(), 0);
    }
}
