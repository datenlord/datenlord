//! The implementation of FUSE response

// We allow unused async here for supporting other asynchronous fuse IO in the future
#![allow(clippy::unused_async)]

use std::convert::AsRef;
#[cfg(feature = "abi-7-18")]
use std::ffi::CString;
use std::ffi::OsStr;
use std::fmt::Debug;
use std::fs::File;
use std::io::{IoSlice, Write};
use std::os::unix::ffi::OsStrExt;
use std::time::Duration;
use std::{mem, slice};

use clippy_utilities::{Cast, OverflowArithmetic};
use nix::errno::Errno;
use nix::sys::stat::SFlag;
use tracing::debug;

use super::abi_marker;
use super::protocol::{
    FuseAttr, FuseAttrOut, FuseBMapOut, FuseDirEnt, FuseEntryOut, FuseFileLock, FuseGetXAttrOut,
    FuseInitOut, FuseKStatFs, FuseLockOut, FuseOpenOut, FuseOutHeader, FuseStatFsOut, FuseWriteOut,
};
#[cfg(feature = "abi-7-18")]
use super::protocol::{FuseNotifyCode::FUSE_NOTIFY_DELETE, FuseNotifyDeleteOut};

/// This trait describes a type that can be converted to `Vec<IoSlice>`
pub trait AsIoSliceList {
    /// Convert the type to a Vec of `IoSlice`
    fn as_io_slice_list(&self) -> Vec<IoSlice>;
    /// The sum of the length of all the `IoSlice`s in the Vec
    fn len(&self) -> usize;
    /// Vec of `IoSlice` is empty
    fn is_empty(&self) -> bool;
}

/// Any type implement the `AsIoSlice` trait, its Vec can be automatically for
/// `Vec<IoSlice>`.
impl<T> AsIoSliceList for Vec<T>
where
    T: AsIoSlice,
{
    fn as_io_slice_list(&self) -> Vec<IoSlice> {
        self.iter().map(T::as_io_slice).collect()
    }

    fn len(&self) -> usize {
        self.iter().map(T::len).sum()
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }
}

/// All the Type implement `CouldBeAsIoSliceList` and `AsIoSlice` can
/// automatically implement `AsIoSliceList`
pub trait CouldBeAsIoSliceList {}

impl<T> AsIoSliceList for T
where
    T: AsIoSlice + CouldBeAsIoSliceList,
{
    fn as_io_slice_list(&self) -> Vec<IoSlice> {
        vec![self.as_io_slice()]
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }
}

/// Implement `AsIoSliceList` for Vec<u8> Separately
impl AsIoSliceList for Vec<u8> {
    fn as_io_slice_list(&self) -> Vec<IoSlice> {
        vec![self.as_io_slice()]
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }
}

/// Implement `AsIoSliceList` for empty tuple
impl AsIoSliceList for () {
    fn as_io_slice_list(&self) -> Vec<IoSlice> {
        vec![]
    }

    fn len(&self) -> usize {
        0
    }

    fn is_empty(&self) -> bool {
        true
    }
}

impl<U, V> AsIoSliceList for (U, V)
where
    U: AsIoSlice,
    V: AsIoSlice,
{
    fn as_io_slice_list(&self) -> Vec<IoSlice> {
        vec![self.0.as_io_slice(), self.1.as_io_slice()]
    }

    fn len(&self) -> usize {
        AsIoSlice::len(&self.0).overflow_add(AsIoSlice::len(&self.1))
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty() && self.1.is_empty()
    }
}

/// The trait indicates the ability to be converted to `IoSlice`
pub trait AsIoSlice {
    /// Convert the type to `IoSlice`
    fn as_io_slice(&self) -> IoSlice;
    /// Tell if the type is ready to be converted, please call it before calling
    /// `as_io_slice`
    fn can_convert(&self) -> bool;
    /// The length of the `IoSlice`
    fn len(&self) -> usize;
    /// `IoSlice` is empty
    fn is_empty(&self) -> bool;
}

impl AsIoSlice for Vec<u8> {
    fn as_io_slice(&self) -> IoSlice {
        IoSlice::new(self.as_slice())
    }

    fn can_convert(&self) -> bool {
        true
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }
}

/// FUSE raw response
#[derive(Debug)]
struct ReplyRaw<'a> {
    /// The FUSE request unique ID
    unique: u64,
    /// The FUSE device fd
    file: &'a mut File,
}

impl<'a> ReplyRaw<'a> {
    /// Create `ReplyRaw`
    fn new(unique: u64, file: &'a mut File) -> Self {
        Self { unique, file }
    }

    /// Send raw message to FUSE kernel
    #[allow(clippy::needless_pass_by_value)]
    fn send_raw_message(
        self,
        error: i32,
        data: impl AsIoSliceList + Send + Sync + 'static,
    ) -> nix::Result<usize> {
        let header_len = mem::size_of::<FuseOutHeader>();

        let header = FuseOutHeader {
            len: (header_len.overflow_add(data.len())).cast(),
            error,
            unique: self.unique,
        };

        let header_bytes = abi_marker::as_abi_bytes(&header);

        let (single, mut vecs);
        let io_slices: &[IoSlice] = if data.len() > 0 {
            vecs = data.as_io_slice_list();
            vecs.insert(0, IoSlice::new(header_bytes));
            vecs.as_slice()
        } else {
            single = [IoSlice::new(header_bytes)];
            &single
        };

        let wsize = self
            .file
            .write_vectored(io_slices)
            .map_err(|e| Errno::try_from(e).unwrap_or(Errno::EIO))?;

        debug!("sent {} bytes to fuse device successfully", wsize);
        Ok(wsize)
    }

    /// Send response to FUSE kernel
    async fn send(self, data: impl AsIoSliceList + Send + Sync + 'static) -> nix::Result<usize> {
        self.send_raw_message(0_i32, data)
    }

    /// Send error code response to FUSE kernel
    async fn send_error_code(self, error_code: Errno) -> nix::Result<usize> {
        // FUSE requires the error number to be negative
        self.send_raw_message(
            crate::async_fuse::util::convert_nix_errno_to_cint(error_code).overflow_neg(),
            (),
        )
    }

    #[allow(clippy::wildcard_enum_match_arm)]
    /// Send error response to FUSE kernel
    async fn send_error(self, err: DatenLordError) -> nix::Result<usize> {
        match err {
            DatenLordError::InternalErr { source, context } => {
                let error_code = if let Some(nix_err) =
                    source.root_cause().downcast_ref::<nix::Error>()
                {
                    if *nix_err == nix::errno::Errno::UnknownErrno {
                        panic!(
                            "should not send nix::errno::Errno::UnknownErrno to FUSE kernel, \
                                    the error is: {} ,context is : {:?}",
                            crate::common::util::format_anyhow_error(&source),
                            context,
                        );
                    } else {
                        nix_err
                    }
                } else {
                    panic!(
                            "should not send non-nix error to FUSE kernel, the error is: {},context is : {:?}",
                            crate::common::util::format_anyhow_error(&source),context,
                        );
                };
                self.send_error_code(*error_code).await
            }
            err => {
                panic!("should not send non-internal error to FUSE kernel ,the error is : {err}",);
            }
        }
    }
}

/// Impl fuse reply new
macro_rules! impl_fuse_reply_new_for{
    {$($t:tt,)+} => {
        $(impl<'a> $t<'a> {
            /// New fuse reply
            #[must_use]
            pub fn new(unique: u64, file: &'a mut File) -> Self {
                Self {
                    reply: ReplyRaw::new(unique, file),
                }
            }
        })+
    }
}

impl_fuse_reply_new_for! {
    ReplyAttr,
    ReplyBMap,
    ReplyCreate,
    ReplyData,
    ReplyEmpty,
    ReplyEntry,
    ReplyInit,
    ReplyLock,
    ReplyOpen,
    ReplyStatFs,
    ReplyWrite,
    ReplyXAttr,
}

use crate::common::error::DatenLordError;

/// Impl fuse reply error
macro_rules! impl_fuse_reply_error_for{
    {$($t:tt,)+} => {
        $(impl $t<'_> {
            #[allow(dead_code)]
            /// fuse reply error
            pub async fn error(self, err: DatenLordError) -> nix::Result<usize> {
                self.reply.send_error(err).await
            }

            #[allow(dead_code)]
            /// fuse reply error code
            pub async fn error_code(self, error_code: Errno) -> nix::Result<usize> {
                self.reply.send_error_code(error_code).await
            }
        })+
    }
}

impl_fuse_reply_error_for! {
    ReplyAttr,
    ReplyBMap,
    ReplyCreate,
    ReplyData,
    ReplyDirectory,
    ReplyEmpty,
    ReplyEntry,
    ReplyInit,
    ReplyLock,
    ReplyOpen,
    ReplyStatFs,
    ReplyWrite,
    ReplyXAttr,
}

/// Impl `AsIoSlice` trait
macro_rules! impl_as_ioslice_for {
    {$($t:ty,)+} => {
        $(impl AsIoSlice for $t {
            #[allow(dead_code)]
            fn as_io_slice(&self) -> IoSlice {
                IoSlice::new(abi_marker::as_abi_bytes(self))
            }

            #[allow(dead_code)]
            fn can_convert(&self) -> bool {
                true
            }

            #[allow(dead_code)]
            fn len(&self) -> usize {
                mem::size_of::<Self>()
            }

            #[allow(dead_code)]
            fn is_empty(&self) -> bool {
                AsIoSlice::len(self) == 0
            }
        }

        impl CouldBeAsIoSliceList for $t {}
        )+
    }
}

impl_as_ioslice_for! {
    FuseAttrOut,
    FuseBMapOut,
    FuseEntryOut,
    FuseInitOut,
    FuseLockOut,
    FuseOpenOut,
    FuseStatFsOut,
    FuseWriteOut,
    FuseGetXAttrOut,
}
#[cfg(feature = "abi-7-18")]
impl_as_ioslice_for! {
    FuseNotifyDeleteOut,
}

/// FUSE init response
#[derive(Debug)]
pub struct ReplyInit<'a> {
    /// The inner raw reply
    reply: ReplyRaw<'a>,
}

impl ReplyInit<'_> {
    /// Reply init response
    pub async fn init(self, resp: FuseInitOut) -> nix::Result<usize> {
        self.reply.send(resp).await
    }
}

/// FUSE empty response
#[derive(Debug)]
pub struct ReplyEmpty<'a> {
    /// The inner raw reply
    reply: ReplyRaw<'a>,
}

impl ReplyEmpty<'_> {
    /// Reply with empty OK response
    pub async fn ok(self) -> nix::Result<usize> {
        self.reply.send(()).await
    }
}

/// FUSE data response
#[derive(Debug)]
pub struct ReplyData<'a> {
    /// The inner raw reply
    reply: ReplyRaw<'a>,
}

impl ReplyData<'_> {
    /// Reply with byte data response
    pub async fn data(
        self,
        bytes: impl AsIoSliceList + Send + Sync + 'static,
    ) -> nix::Result<usize> {
        self.reply.send(bytes).await
    }
}

/// FUSE entry response
#[derive(Debug)]
pub struct ReplyEntry<'a> {
    /// The inner raw reply
    reply: ReplyRaw<'a>,
}

impl ReplyEntry<'_> {
    /// Reply to a request with the given entry
    pub async fn entry(self, ttl: Duration, attr: FuseAttr, generation: u64) -> nix::Result<usize> {
        self.reply
            .send(FuseEntryOut {
                nodeid: attr.ino,
                generation,
                entry_valid: ttl.as_secs(),
                attr_valid: ttl.as_secs(),
                entry_valid_nsec: ttl.subsec_nanos(),
                attr_valid_nsec: ttl.subsec_nanos(),
                attr,
            })
            .await
    }
}

/// FUSE attribute response
#[derive(Debug)]
pub struct ReplyAttr<'a> {
    /// The inner raw reply
    reply: ReplyRaw<'a>,
}

impl ReplyAttr<'_> {
    /// Reply to a request with the given attribute
    pub async fn attr(self, ttl: Duration, attr: FuseAttr) -> nix::Result<usize> {
        self.reply
            .send(FuseAttrOut {
                attr_valid: ttl.as_secs(),
                attr_valid_nsec: ttl.subsec_nanos(),
                dummy: 0,
                attr,
            })
            .await
    }
}

/// FUSE open response
#[derive(Debug)]
pub struct ReplyOpen<'a> {
    /// The inner raw reply
    reply: ReplyRaw<'a>,
}

impl ReplyOpen<'_> {
    /// Reply to a request with the given open result
    pub async fn opened(self, fh: u64, flags: u32) -> nix::Result<usize> {
        self.reply
            .send(FuseOpenOut {
                fh,
                open_flags: flags,
                padding: 0,
            })
            .await
    }
}

/// FUSE write response
#[derive(Debug)]
pub struct ReplyWrite<'a> {
    /// The inner raw reply
    reply: ReplyRaw<'a>,
}

impl ReplyWrite<'_> {
    /// Reply to a request with the given open result
    pub async fn written(self, size: u32) -> nix::Result<usize> {
        self.reply.send(FuseWriteOut { size, padding: 0 }).await
    }
}

/// FUSE statfs response
#[derive(Debug)]
pub struct ReplyStatFs<'a> {
    /// The inner raw reply
    reply: ReplyRaw<'a>,
}

/// POSIX statvfs parameters
#[derive(Debug)]
pub struct StatFsParam {
    /// The number of blocks in the filesystem
    pub blocks: u64,
    /// The number of free blocks
    pub bfree: u64,
    /// The number of free blocks for non-privilege users
    pub bavail: u64,
    /// The number of inodes
    pub files: u64,
    /// The number of free inodes
    pub f_free: u64,
    /// Block size
    pub bsize: u32,
    /// Maximum file name length
    pub namelen: u32,
    /// Fragment size
    pub frsize: u32,
}

impl ReplyStatFs<'_> {
    /// Reply statfs response
    #[allow(dead_code)]
    pub async fn statfs(self, param: StatFsParam) -> nix::Result<usize> {
        self.reply
            .send(FuseStatFsOut {
                st: FuseKStatFs {
                    blocks: param.blocks,
                    bfree: param.bfree,
                    bavail: param.bavail,
                    files: param.files,
                    ffree: param.f_free,
                    bsize: param.bsize,
                    namelen: param.namelen,
                    frsize: param.frsize,
                    padding: 0,
                    spare: [0; 6],
                },
            })
            .await
    }
}

/// FUSE create response
#[derive(Debug)]
pub struct ReplyCreate<'a> {
    /// The inner raw reply
    reply: ReplyRaw<'a>,
}

impl ReplyCreate<'_> {
    /// Reply to a request with the given entry
    #[allow(dead_code)]
    pub async fn created(
        self,
        ttl: &Duration,
        attr: FuseAttr,
        generation: u64,
        fh: u64,
        flags: u32,
    ) -> nix::Result<usize> {
        self.reply
            .send((
                FuseEntryOut {
                    nodeid: attr.ino,
                    generation,
                    entry_valid: ttl.as_secs(),
                    attr_valid: ttl.as_secs(),
                    entry_valid_nsec: ttl.subsec_nanos(),
                    attr_valid_nsec: ttl.subsec_nanos(),
                    attr,
                },
                FuseOpenOut {
                    fh,
                    open_flags: flags,
                    padding: 0,
                },
            ))
            .await
    }
}

/// FUSE lock response
#[derive(Debug)]
pub struct ReplyLock<'a> {
    /// The inner raw reply
    reply: ReplyRaw<'a>,
}

impl ReplyLock<'_> {
    /// Reply to a request with the given open result
    #[allow(dead_code)]
    pub async fn locked(self, start: u64, end: u64, typ: u32, pid: u32) -> nix::Result<usize> {
        self.reply
            .send(FuseLockOut {
                lk: FuseFileLock {
                    start,
                    end,
                    typ,
                    pid,
                },
            })
            .await
    }
}

/// FUSE bmap response
#[derive(Debug)]
pub struct ReplyBMap<'a> {
    /// The inner raw reply
    reply: ReplyRaw<'a>,
}

impl ReplyBMap<'_> {
    /// Reply to a request with the given open result
    #[allow(dead_code)]
    pub async fn bmap(self, block: u64) -> nix::Result<usize> {
        self.reply.send(FuseBMapOut { block }).await
    }
}

/// FUSE directory response
#[derive(Debug)]
pub struct ReplyDirectory<'a> {
    /// The inner raw reply
    reply: ReplyRaw<'a>,
    /// The directory data in bytes
    data: Vec<u8>,
}

impl<'a> ReplyDirectory<'a> {
    /// Creates a new `ReplyDirectory` with a specified buffer size.
    #[must_use]
    pub fn new(unique: u64, file: &'a mut File, size: usize) -> Self {
        Self {
            reply: ReplyRaw::new(unique, file),
            data: Vec::with_capacity(size),
        }
    }

    /// Add an entry to the directory reply buffer. Returns true if the buffer
    /// is full. A transparent offset value can be provided for each entry.
    /// The kernel uses these value to request the next entries in further
    /// readdir calls
    pub fn add<T: AsRef<OsStr>>(&mut self, ino: u64, offset: i64, kind: SFlag, name: T) -> bool {
        let name_bytes = name.as_ref().as_bytes();
        let dirent = FuseDirEnt {
            ino,
            off: offset.cast(),
            namelen: name_bytes.len().cast(),
            typ: crate::async_fuse::util::mode_from_kind_and_perm(kind, 0).overflow_shr(12),
        };
        let entlen = dirent.size_with_name();

        // This is similar to call `FUSE_REC_ALIGN(entlen)` in `fuse.h`.
        //
        // <https://github.com/torvalds/linux/blob/b85ea95d086471afb4ad062012a4d73cd328fa86/include/uapi/linux/fuse.h#L988-L989>
        let entsize = super::super::util::round_up(entlen, mem::size_of::<u64>()); // 64bit align

        let padlen = entsize.overflow_sub(entlen);
        if self.data.len().overflow_add(entsize) > self.data.capacity() {
            return true;
        }

        // # Safety
        // The `fuse_dir_ent_in_raw` call is safe here, because:
        // 1. The `dirent` is just built above, as a in-stack allocated object.
        //    Therefore, `&dirent` is a valid reference.
        // 2. `dirent.namelen` is evaluated from `name_bytes`, and `name_bytes` is to be
        //    written into `self.data`, which is a `Vec<u8>`, after `dirent_bytes` is
        //    written.
        let dirent_bytes = unsafe { fuse_dir_ent_in_raw(&dirent) };
        // Write dirent
        self.data.extend_from_slice(dirent_bytes);

        // write name
        self.data.extend_from_slice(name_bytes);

        // write zero padding
        self.data.extend(std::iter::repeat(0).take(padlen));

        false
    }

    /// Reply to a request with the filled directory buffer
    pub async fn ok(self) -> nix::Result<usize> {
        self.reply.send(self.data).await
    }
}

/// Get the underlying raw part of a `FuseDirEnt`, represented in `&[u8]`.
///
/// # Safety
/// Behavior is undefined if any of the following conditions are violated:
/// - `from` must be a valid reference to `FuseDirEnt`.
/// - The `namelen` field in `from` must represent a valid length of the nearly
///   following data of the `FuseDirEnt` struct, i.e., the caller of this
///   function takes the responsibility to build a raw string (`[u8]`) with
///   length of `from.namelen` and place it nearly following the `FuseDirEnt` in
///   a continuous, single allocated space (for example, a `Vec<u8>`).
unsafe fn fuse_dir_ent_in_raw(from: &FuseDirEnt) -> &[u8] {
    let base: *const u8 = <*const FuseDirEnt>::cast(from);
    let bytes = slice::from_raw_parts(base, mem::size_of::<FuseDirEnt>());
    bytes
}

/// FUSE extended attribute response
#[derive(Debug)]
pub struct ReplyXAttr<'a> {
    /// The inner raw reply
    reply: ReplyRaw<'a>,
}

impl ReplyXAttr<'_> {
    /// Reply to a request with the size of the xattr.
    #[allow(dead_code)]
    pub async fn size(self, size: u32) -> nix::Result<usize> {
        self.reply.send(FuseGetXAttrOut { size, padding: 0 }).await
    }

    /// Reply to a request with the data in the xattr.
    #[allow(dead_code)]
    pub async fn data(self, bytes: FuseGetXAttrOut) -> nix::Result<usize> {
        self.reply.send(bytes).await
    }
}

#[cfg(feature = "abi-7-18")]
impl AsIoSlice for CString {
    fn as_io_slice(&self) -> IoSlice {
        IoSlice::new(self.as_bytes_with_nul())
    }

    fn can_convert(&self) -> bool {
        true
    }

    fn len(&self) -> usize {
        self.as_bytes_with_nul().len()
    }

    // CString cannot be empty
    fn is_empty(&self) -> bool {
        false
    }
}
/// Fuse delete notification
#[cfg(feature = "abi-7-18")]
#[derive(Debug)]
pub struct FuseDeleteNotification<'a> {
    /// The inner raw reply
    reply: ReplyRaw<'a>,
}

#[cfg(feature = "abi-7-18")]
impl<'a> FuseDeleteNotification<'a> {
    /// Create `FuseDeleteNotification`
    #[must_use]
    pub fn new(file: &'a mut File) -> Self {
        Self {
            reply: ReplyRaw::new(0, file),
        }
    }

    /// Notify kernel
    pub async fn notify(self, parent: u64, child: u64, name: String) -> nix::Result<usize> {
        let notify_delete = FuseNotifyDeleteOut {
            parent,
            child,
            namelen: name.len().cast(),
            padding: 0,
        };
        let file_name = CString::new(name.clone())
            .unwrap_or_else(|e| panic!("failed to create CString for {name}, error is {e:?}"));
        #[allow(clippy::as_conversions)] // allow this for enum
        self.reply
            .send_raw_message(FUSE_NOTIFY_DELETE as i32, (notify_delete, file_name))
    }
}

#[cfg(test)]
mod test {
    use std::fs::File;
    use std::os::unix::io::FromRawFd;
    use std::time::Duration;

    use aligned_utils::bytes::AlignedBytes;
    use anyhow::Context;
    use nix::fcntl::{self, OFlag};
    use nix::sys::stat::Mode;
    use nix::unistd;
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    use super::super::de::Deserializer;
    use super::super::protocol::{FuseAttr, FuseAttrOut, FuseOutHeader};
    use super::ReplyAttr;

    #[test]
    fn test_slice() {
        let s = [1_i32, 2_i32, 3_i32, 4_i32, 5_i32, 6_i32];
        let v = s.to_owned();
        println!("{v:?}");
        let v1 = s.to_vec();
        println!("{v1:?}");

        let s1 = [1_i32, 2_i32, 3_i32];
        let s2 = [4_i32, 5_i32, 6_i32];
        let s3 = [7_i32, 8_i32, 9_i32];
        let l1 = [&s1];
        let l2 = [&s2, &s3];
        let mut v1 = l1.to_vec();
        v1.extend(&l2);

        println!("{l1:?}");
        println!("{v1:?}");
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_reply_output() -> anyhow::Result<()> {
        let file_name = "fuse_reply.log";
        let fd = tokio::task::spawn_blocking(move || {
            fcntl::open(
                file_name,
                OFlag::O_CREAT | OFlag::O_TRUNC | OFlag::O_RDWR,
                Mode::all(),
            )
        })
        .await??;
        tokio::task::spawn_blocking(move || unistd::unlink(file_name)).await??;

        let ino = 64;
        let size = 64;
        let blocks = 64;
        let a_time = 64;
        let m_time = 64;
        let c_time = 64;
        let a_timensec = 32;
        let m_timensec = 32;
        let c_timensec = 32;
        let mode = 32;
        let nlink = 32;
        let uid = 32;
        let g_id = 32;
        let rdev = 32;
        #[cfg(feature = "abi-7-9")]
        let blksize = 32;
        #[cfg(feature = "abi-7-9")]
        let padding = 32;
        let attr = FuseAttr {
            ino,
            size,
            blocks,
            atime: a_time,
            mtime: m_time,
            ctime: c_time,
            atimensec: a_timensec,
            mtimensec: m_timensec,
            ctimensec: c_timensec,
            mode,
            nlink,
            uid,
            gid: g_id,
            rdev,
            #[cfg(feature = "abi-7-9")]
            blksize,
            #[cfg(feature = "abi-7-9")]
            padding,
        };

        let unique = 12345;
        // SAFETY: `fd` is just opened
        let mut file = unsafe { File::from_raw_fd(fd) };
        let reply_attr = ReplyAttr::new(unique, &mut file);
        reply_attr.attr(Duration::from_secs(1), attr).await?;

        let mut file =
            tokio::task::spawn_blocking(move || unsafe { tokio::fs::File::from_raw_fd(fd) })
                .await?;
        file.seek(std::io::SeekFrom::Start(0)).await?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).await?;

        let mut aligned_bytes = AlignedBytes::new_zeroed(bytes.len(), 4096);
        aligned_bytes.copy_from_slice(&bytes);

        let mut de = Deserializer::new(&aligned_bytes);
        let _foh: &FuseOutHeader = de.fetch_ref().context("failed to fetch FuseOutHeader")?;
        let fao: &FuseAttrOut = de.fetch_ref().context("failed to fetch FuseAttrOut")?;

        debug_assert_eq!(fao.attr.ino, ino);
        debug_assert_eq!(fao.attr.size, size);
        debug_assert_eq!(fao.attr.blocks, blocks);
        debug_assert_eq!(fao.attr.mtime, m_time);
        debug_assert_eq!(fao.attr.atime, a_time);
        debug_assert_eq!(fao.attr.ctime, c_time);
        Ok(())
    }
}
