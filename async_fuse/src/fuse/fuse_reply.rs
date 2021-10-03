//! The implementation of FUSE response

use log::debug;
use nix::errno::Errno;
use nix::sys::stat::SFlag;
use nix::sys::uio::{self, IoVec};
use std::convert::AsRef;
use std::ffi::OsStr;
use std::fmt::Debug;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::RawFd;
use std::time::Duration;
use std::{mem, ptr, slice};
use utilities::{Cast, OverflowArithmetic};

use super::abi_marker;
#[cfg(target_os = "macos")]
use super::protocol::FuseGetXTimesOut;
use super::protocol::{
    FuseAttr, FuseAttrOut, FuseBMapOut, FuseDirEnt, FuseEntryOut, FuseFileLock, FuseGetXAttrOut,
    FuseInitOut, FuseKStatFs, FuseLockOut, FuseOpenOut, FuseOutHeader, FuseStatFsOut, FuseWriteOut,
};

/// This trait describes a type that can be converted to Vec<IoVec<&[u8]>>
pub trait AsIoVecList {
    /// Convert the type to a Vec of `IoVec`
    fn as_io_vec_list(&self) -> Vec<IoVec<&[u8]>>;
    /// The sum of the length of all the `IoVec`s in the Vec
    fn len(&self) -> usize;
}

/// Any type implement the `AsIoVec` trait, its Vec can be automatically for Vec<IoVec<&[u8]>>.
impl<T> AsIoVecList for Vec<T>
where
    T: AsIoVec,
{
    fn as_io_vec_list(&self) -> Vec<IoVec<&[u8]>> {
        self.iter().map(|a| a.as_io_vec()).collect()
    }

    fn len(&self) -> usize {
        self.iter().map(|a| a.len()).sum()
    }
}

/// All the Type implement `CouldBeAsIoVecList` and `AsIoVec` can automatically implement
/// `AsIoVecList`
pub trait CouldBeAsIoVecList {}

impl<T> AsIoVecList for T
where
    T: AsIoVec + CouldBeAsIoVecList,
{
    fn as_io_vec_list(&self) -> Vec<IoVec<&[u8]>> {
        vec![self.as_io_vec()]
    }

    fn len(&self) -> usize {
        self.len()
    }
}

/// Implement `AsIoVecList` for Vec<u8> Separately
impl AsIoVecList for Vec<u8> {
    fn as_io_vec_list(&self) -> Vec<IoVec<&[u8]>> {
        vec![self.as_io_vec()]
    }

    fn len(&self) -> usize {
        self.len()
    }
}

/// Implement `AsIoVecList` for empty tuple
impl AsIoVecList for () {
    fn as_io_vec_list(&self) -> Vec<IoVec<&[u8]>> {
        vec![]
    }

    fn len(&self) -> usize {
        0
    }
}

impl<U, V> AsIoVecList for (U, V)
where
    U: AsIoVec,
    V: AsIoVec,
{
    fn as_io_vec_list(&self) -> Vec<IoVec<&[u8]>> {
        vec![self.0.as_io_vec(), self.1.as_io_vec()]
    }

    fn len(&self) -> usize {
        AsIoVec::len(&self.0).overflow_add(AsIoVec::len(&self.1))
    }
}

/// The trait indicates the ability to be converted to `IoVec`
pub trait AsIoVec {
    /// Convert the type to IoVec<&[u8]>
    fn as_io_vec(&self) -> IoVec<&[u8]>;
    /// Tell if the type is ready to be converted, please call it before calling `as_io_vec`
    fn can_convert(&self) -> bool;
    /// The length of the `IoVec`
    fn len(&self) -> usize;
}

impl AsIoVec for Vec<u8> {
    fn as_io_vec(&self) -> IoVec<&[u8]> {
        IoVec::from_slice(self.as_slice())
    }
    fn can_convert(&self) -> bool {
        true
    }
    fn len(&self) -> usize {
        self.len()
    }
}

/// FUSE raw response
#[derive(Debug)]
struct ReplyRaw {
    /// The FUSE request unique ID
    unique: u64,
    /// The FUSE device fd
    fd: RawFd,
}

impl ReplyRaw {
    /// Create `ReplyRaw`
    const fn new(unique: u64, fd: RawFd) -> Self {
        Self { unique, fd }
    }

    /// Send response to FUSE kernel
    async fn send(self, data: impl AsIoVecList + Send + Sync + 'static) -> nix::Result<usize> {
        let fd = self.fd;
        let wsize = smol::unblock(move || {
            let header_len = mem::size_of::<FuseOutHeader>();

            let header = FuseOutHeader {
                len: (header_len.overflow_add(data.len())).cast(),
                error: 0, // FUSE requires the error number to be negative
                unique: self.unique,
            };

            let header_bytes = abi_marker::as_abi_bytes(&header);

            let (single, mut vecs);
            let iovecs: &[IoVec<&[u8]>] = if data.len() > 0 {
                vecs = data.as_io_vec_list();
                vecs.insert(0, IoVec::from_slice(header_bytes));
                vecs.as_slice()
            } else {
                single = [IoVec::from_slice(header_bytes)];
                &single
            };

            uio::writev(fd, iovecs)
        })
        .await?;

        debug!("sent {} bytes to fuse device successfully", wsize);
        Ok(wsize)
    }

    /// Send error code response to FUSE kernel
    async fn send_error_code(self, error_code: Errno) -> nix::Result<usize> {
        let fd = self.fd;
        let wsize = smol::unblock(move || {
            let header_len = mem::size_of::<FuseOutHeader>();

            let header = FuseOutHeader {
                len: (header_len).cast(),
                error: crate::util::convert_nix_errno_to_cint(error_code).overflow_neg(), // FUSE requires the error number to be negative
                unique: self.unique,
            };

            let header_bytes = abi_marker::as_abi_bytes(&header);
            let iovecs = [IoVec::from_slice(header_bytes)];

            uio::writev(fd, &iovecs)
        })
        .await?;
        debug!("sent {} bytes to fuse device successfully", wsize);
        Ok(wsize)
    }

    /// Send error response to FUSE kernel
    async fn send_error(self, err: anyhow::Error) -> nix::Result<usize> {
        let error_code = if let Some(nix_err) = err.root_cause().downcast_ref::<nix::Error>() {
            if let Some(error_code) = nix_err.as_errno() {
                if let nix::errno::Errno::UnknownErrno = error_code {
                    panic!(
                        "should not send nix::errno::Errno::UnknownErrno to FUSE kernel, \
                            the error is: {}",
                        common::util::format_anyhow_error(&err),
                    );
                } else {
                    error_code
                }
            } else {
                panic!(
                    "should not send non-nix::Error::Sys to FUSE kernel, the error is: {}",
                    common::util::format_anyhow_error(&err),
                )
            }
        } else {
            panic!(
                "should not send non-nix error to FUSE kernel, the error is: {}",
                common::util::format_anyhow_error(&err),
            );
        };
        self.send_error_code(error_code).await
    }
}

macro_rules! impl_fuse_reply_new_for{
    {$($t:ty,)+} => {
        $(impl $t {
            pub const fn new(unique: u64, fd: RawFd) -> Self {
                Self {
                    reply: ReplyRaw::new(unique, fd),
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

#[cfg(target_os = "macos")]
impl_fuse_reply_new_for! {
    ReplyXTimes,
}

macro_rules! impl_fuse_reply_error_for{
    {$($t:ty,)+} => {
        $(impl $t {
            #[allow(dead_code)]
            pub async fn error(self, err: anyhow::Error) -> nix::Result<usize> {
                self.reply.send_error(err).await
            }

            #[allow(dead_code)]
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

macro_rules! impl_as_iovec_for {
    {$($t:ty,)+} => {
        $(impl AsIoVec for $t {
            #[allow(dead_code)]
            fn as_io_vec(&self) -> IoVec<&[u8]> {
                IoVec::from_slice(abi_marker::as_abi_bytes(self))
            }

            #[allow(dead_code)]
            fn can_convert(&self) -> bool {
                true
            }

            #[allow(dead_code)]
            fn len(&self) -> usize {
                mem::size_of::<Self>()
            }
        }

        impl CouldBeAsIoVecList for $t {}
        )+
    }
}

impl_as_iovec_for! {
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

/// FUSE init response
#[derive(Debug)]
pub struct ReplyInit {
    /// The inner raw reply
    reply: ReplyRaw,
}

impl ReplyInit {
    /// Reply init response
    pub async fn init(self, resp: FuseInitOut) -> nix::Result<usize> {
        self.reply.send(resp).await
    }
}

/// FUSE empty response
#[derive(Debug)]
pub struct ReplyEmpty {
    /// The inner raw reply
    reply: ReplyRaw,
}

impl ReplyEmpty {
    /// Reply with empty OK response
    pub async fn ok(self) -> nix::Result<usize> {
        self.reply.send(()).await
    }
}

/// FUSE data response
#[derive(Debug)]
pub struct ReplyData {
    /// The inner raw reply
    reply: ReplyRaw,
}

impl ReplyData {
    /// Reply with byte data repsonse
    pub async fn data(self, bytes: impl AsIoVecList + Send + Sync + 'static) -> nix::Result<usize> {
        self.reply.send(bytes).await
    }
}

/// FUSE entry response
#[derive(Debug)]
pub struct ReplyEntry {
    /// The inner raw reply
    reply: ReplyRaw,
}

impl ReplyEntry {
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
pub struct ReplyAttr {
    /// The inner raw reply
    reply: ReplyRaw,
}

impl ReplyAttr {
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

/// FUSE extended timestamp response
#[cfg(target_os = "macos")]
#[derive(Debug)]
pub struct ReplyXTimes {
    /// The inner raw reply
    reply: ReplyRaw<FuseGetXTimesOut>,
}

#[cfg(target_os = "macos")]
impl ReplyXTimes {
    /// Reply to a request with the given xtimes
    #[allow(dead_code)]
    pub async fn xtimes(
        self,
        bkuptime_secs: u64,
        bkuptime_nanos: u32,
        crtime_secs: u64,
        crtime_nanos: u32,
    ) -> nix::Result<usize> {
        self.reply
            .send(FuseGetXTimesOut {
                bkuptime: bkuptime_secs,
                crtime: crtime_secs,
                bkuptimensec: bkuptime_nanos,
                crtimensec: crtime_nanos,
            })
            .await
    }
}

/// FUSE open response
#[derive(Debug)]
pub struct ReplyOpen {
    /// The inner raw reply
    reply: ReplyRaw,
}

impl ReplyOpen {
    /// Reply to a request with the given open result
    pub async fn opened(self, fh: RawFd, flags: u32) -> nix::Result<usize> {
        self.reply
            .send(FuseOpenOut {
                fh: fh.cast(),
                open_flags: flags,
                padding: 0,
            })
            .await
    }
}

/// FUSE write response
#[derive(Debug)]
pub struct ReplyWrite {
    /// The inner raw reply
    reply: ReplyRaw,
}

impl ReplyWrite {
    /// Reply to a request with the given open result
    pub async fn written(self, size: u32) -> nix::Result<usize> {
        self.reply.send(FuseWriteOut { size, padding: 0 }).await
    }
}

/// FUSE statfs response
#[derive(Debug)]
pub struct ReplyStatFs {
    /// The inner raw reply
    reply: ReplyRaw,
}

/// POSIX statvfs parameters
#[derive(Debug)]
pub struct StatFsParam {
    /// The number of blocks in the filesystem
    pub blocks: u64,
    /// The number of free blocks
    pub bfree: u64,
    /// The number of free blocks for non-priviledge users
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

impl ReplyStatFs {
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
pub struct ReplyCreate {
    /// The inner raw reply
    reply: ReplyRaw,
}

impl ReplyCreate {
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
pub struct ReplyLock {
    /// The inner raw reply
    reply: ReplyRaw,
}

impl ReplyLock {
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
pub struct ReplyBMap {
    /// The inner raw reply
    reply: ReplyRaw,
}

impl ReplyBMap {
    /// Reply to a request with the given open result
    #[allow(dead_code)]
    pub async fn bmap(self, block: u64) -> nix::Result<usize> {
        self.reply.send(FuseBMapOut { block }).await
    }
}

/// FUSE directory response
#[derive(Debug)]
pub struct ReplyDirectory {
    /// The inner raw reply
    reply: ReplyRaw,
    /// The directory data in bytes
    data: Vec<u8>,
}

impl ReplyDirectory {
    /// Creates a new `ReplyDirectory` with a specified buffer size.
    pub fn new(unique: u64, fd: RawFd, size: usize) -> Self {
        Self {
            reply: ReplyRaw::new(unique, fd),
            data: Vec::with_capacity(size),
        }
    }

    /// Add an entry to the directory reply buffer. Returns true if the buffer is full.
    /// A transparent offset value can be provided for each entry. The kernel uses these
    /// value to request the next entries in further readdir calls
    pub fn add<T: AsRef<OsStr>>(&mut self, ino: u64, offset: i64, kind: SFlag, name: T) -> bool {
        /// <https://doc.rust-lang.org/std/alloc/struct.Layout.html#method.padding_needed_for>
        ///
        /// <https://doc.rust-lang.org/src/core/alloc/layout.rs.html#226-250>
        const fn round_up(len: usize, align: usize) -> usize {
            len.wrapping_add(align).wrapping_sub(1) & !align.wrapping_sub(1)
        }

        let name_bytes = name.as_ref().as_bytes();
        let entlen = mem::size_of::<FuseDirEnt>().overflow_add(name_bytes.len());
        let entsize = round_up(entlen, mem::size_of::<u64>()); // 64bit align

        let padlen = entsize.overflow_sub(entlen);
        if self.data.len().overflow_add(entsize) > self.data.capacity() {
            return true;
        }

        let mut dirent = FuseDirEnt {
            ino,
            off: offset.cast(),
            namelen: name_bytes.len().cast(),
            typ: crate::util::mode_from_kind_and_perm(kind, 0).overflow_shr(12),
        };

        // TODO: refactory this, do not use unsafe code
        unsafe {
            // write dirent
            let base: *mut u8 = <*mut FuseDirEnt>::cast(&mut dirent);
            let bytes = slice::from_raw_parts(base, mem::size_of::<FuseDirEnt>());
            self.data.extend_from_slice(bytes);

            // write name
            self.data.extend_from_slice(name_bytes);

            // write zero padding
            let end_ptr = self.data.as_mut_ptr().add(self.data.len());
            ptr::write_bytes(end_ptr, 0, padlen);

            // set len
            let new_len = self.data.len().wrapping_add(padlen);
            self.data.set_len(new_len);
        }

        false
    }

    /// Reply to a request with the filled directory buffer
    pub async fn ok(self) -> nix::Result<usize> {
        self.reply.send(self.data).await
    }
}

/// FUSE extended attribute response
#[derive(Debug)]
pub struct ReplyXAttr {
    /// The inner raw reply
    reply: ReplyRaw,
}

impl ReplyXAttr {
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

#[cfg(test)]
mod test {
    use super::super::de::Deserializer;
    use super::super::protocol::{FuseAttr, FuseAttrOut, FuseOutHeader};
    use super::ReplyAttr;

    use aligned_utils::bytes::AlignedBytes;
    use anyhow::Context;
    use nix::fcntl::{self, OFlag};
    use nix::sys::stat::Mode;
    use nix::unistd;
    use std::os::unix::io::FromRawFd;
    use std::time::Duration;

    use futures::AsyncReadExt;
    use futures::AsyncSeekExt;

    #[test]
    fn test_slice() {
        let s = [1, 2, 3, 4, 5, 6];
        let v = s.to_owned();
        println!("{:?}", v);
        let v1 = s.to_vec();
        println!("{:?}", v1);

        let s1 = [1, 2, 3];
        let s2 = [4, 5, 6];
        let s3 = [7, 8, 9];
        let l1 = [&s1];
        let l2 = [&s2, &s3];
        let mut v1 = l1.to_vec();
        v1.extend(&l2);

        println!("{:?}", l1);
        println!("{:?}", v1);
    }
    #[test]
    fn test_reply_output() -> anyhow::Result<()> {
        smol::block_on(async move {
            let file_name = "fuse_reply.log";
            let fd = smol::unblock(move || {
                fcntl::open(
                    file_name,
                    OFlag::O_CREAT | OFlag::O_TRUNC | OFlag::O_RDWR,
                    Mode::all(),
                )
            })
            .await?;
            smol::unblock(move || unistd::unlink(file_name)).await?;

            let ino = 64;
            let size = 64;
            let blocks = 64;
            let a_time = 64;
            let m_time = 64;
            let c_time = 64;
            #[cfg(target_os = "macos")]
            let creat_time = 64;
            let a_timensec = 32;
            let m_timensec = 32;
            let c_timensec = 32;
            #[cfg(target_os = "macos")]
            let creat_timensec = 32;
            let mode = 32;
            let nlink = 32;
            let uid = 32;
            let g_id = 32;
            let rdev = 32;
            #[cfg(target_os = "macos")]
            let flags = 32;
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
                #[cfg(target_os = "macos")]
                crtime: creat_time,
                atimensec: a_timensec,
                mtimensec: m_timensec,
                ctimensec: c_timensec,
                #[cfg(target_os = "macos")]
                crtimensec: creat_timensec,
                mode,
                nlink,
                uid,
                gid: g_id,
                rdev,
                #[cfg(target_os = "macos")]
                flags,
                #[cfg(feature = "abi-7-9")]
                blksize,
                #[cfg(feature = "abi-7-9")]
                padding,
            };

            let unique = 12345;
            let reply_attr = ReplyAttr::new(unique, fd);
            reply_attr.attr(Duration::from_secs(1), attr).await?;

            let mut file = smol::unblock(move || unsafe { smol::fs::File::from_raw_fd(fd) }).await;
            file.seek(smol::io::SeekFrom::Start(0)).await?;
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes).await?;

            let mut aligned_bytes = AlignedBytes::new_zeroed(bytes.len(), 4096);
            aligned_bytes.copy_from_slice(&bytes);

            let mut de = Deserializer::new(&aligned_bytes);
            let foh: &FuseOutHeader = de.fetch_ref().context("failed to fetch FuseOutHeader")?;
            let fao: &FuseAttrOut = de.fetch_ref().context("failed to fetch FuseAttrOut")?;

            dbg!(foh, fao);
            debug_assert_eq!(fao.attr.ino, ino);
            debug_assert_eq!(fao.attr.size, size);
            debug_assert_eq!(fao.attr.blocks, blocks);
            debug_assert_eq!(fao.attr.mtime, m_time);
            debug_assert_eq!(fao.attr.atime, a_time);
            debug_assert_eq!(fao.attr.ctime, c_time);
            Ok(())
        })
    }
}
