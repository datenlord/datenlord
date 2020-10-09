//! The implementation of FUSE response

use log::debug;
use nix::sys::stat::SFlag;
use nix::sys::uio::{self, IoVec};
use smol::blocking;
use std::convert::AsRef;
use std::ffi::OsStr;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::os::raw::c_int;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::RawFd;
use std::time::Duration;
use std::{mem, ptr, slice};
use utilities::{Cast, OverflowArithmetic};

use super::fs;
#[cfg(target_os = "macos")]
use super::protocol::FuseGetXTimesOut;
use super::protocol::{
    FuseAttr, FuseAttrOut, FuseBMapOut, FuseDirEnt, FuseEntryOut, FuseFileLock, FuseGetXAttrOut,
    FuseInitOut, FuseKStatFs, FuseLockOut, FuseOpenOut, FuseOutHeader, FuseStatFsOut, FuseWriteOut,
};

/// The FUSE response data
#[derive(Debug)]
enum ToBytes<T> {
    /// The response data is a structure, will be converted to bytes
    Struct(T),
    /// The response data is bytes
    Bytes(Vec<u8>),
    /// The response data is error code
    Error,
}

/// FUSE raw response
#[derive(Debug)]
struct ReplyRaw<T: Send + Sync + 'static> {
    /// The FUSE request unique ID
    unique: u64,
    /// The FUSE device fd
    fd: RawFd,
    /// The phantom data
    marker: PhantomData<T>,
}

impl<T: Send + Sync + 'static> ReplyRaw<T> {
    /// Create `ReplyRaw`
    fn new(unique: u64, fd: RawFd) -> Self {
        Self {
            unique,
            fd,
            marker: PhantomData,
        }
    }

    /// Send response to FUSE kernel
    async fn send(self, to_bytes: ToBytes<T>, err: c_int) -> nix::Result<usize> {
        let fd = self.fd;
        let wsize = blocking!(
            let mut send_error = false;
            let instance: T; // to hold the instance of ToBytes::Struct
            let byte_vec: Vec<u8>; // to hold the Vec<u8> of ToBytes::Bytes
            let empty_vec: Vec<u8>; // to hold the emtpy Vec<u8> of ToBytes::Null
            let (data_len, bytes) = match to_bytes {
                ToBytes::Struct(inst) => {
                    instance = inst;
                    let len = mem::size_of::<T>();
                    let bytes = match len {
                        0 => &[],
                        len => {
                            let p = utilities::cast_to_ptr(&instance);
                            unsafe { slice::from_raw_parts(p, len) }
                        }
                    };
                    (len, bytes)
                }
                ToBytes::Bytes(bv) => {
                    byte_vec = bv;
                    (byte_vec.len(), &byte_vec[..])
                }
                ToBytes::Error => {
                    send_error = true;
                    empty_vec = Vec::new();
                    (0, &empty_vec[..])
                }
            };
            let header_len = mem::size_of::<FuseOutHeader>();
            let header = FuseOutHeader {
                len: (header_len.overflow_add(data_len)).cast(),
                error: err.overflow_neg(), // FUSE requires the error number to be negative
                unique: self.unique,
            };
            let h = utilities::cast_to_ptr(&header);
            let header_bytes = unsafe { slice::from_raw_parts(h, header_len) };
            let iovecs: Vec<_> = if data_len > 0 {
                vec![IoVec::from_slice(header_bytes), IoVec::from_slice(bytes)]
            } else {
                vec![IoVec::from_slice(header_bytes)]
            };
            if send_error {
                debug_assert_ne!(err, 0);
            } else {
                debug_assert_eq!(err, 0);
            }
            uio::writev(fd, &iovecs)
            // .context(format!(
            //     "failed to send to FUSE, the reply header is: {:?}", header,
            // ))
        )?;

        debug!("sent {} bytes to fuse device successfully", wsize);
        Ok(wsize)
    }

    /// Send byte array response to FUSE kernel
    async fn send_bytes(self, byte_vec: Vec<u8>) -> nix::Result<usize> {
        self.send(ToBytes::Bytes(byte_vec), 0).await
        // .context("send_bytes() failed to send bytes")
    }

    /// Send structure response to FUSE kernel
    async fn send_data(self, instance: T) -> nix::Result<usize> {
        self.send(ToBytes::Struct(instance), 0).await
        // .context("send_data() failed to send data")
    }

    /// Send error code response to FUSE kernel
    async fn send_error_code(self, error_code: c_int) -> nix::Result<usize> {
        self.send(ToBytes::Error, error_code).await
        // .context("send_error_code() failed to send error")
    }

    /// Send error response to FUSE kernel
    async fn send_error(self, err: anyhow::Error) -> nix::Result<usize> {
        let error_code = if let Some(nix_err) = err.root_cause().downcast_ref::<nix::Error>() {
            if let Some(error_code) = nix_err.as_errno() {
                if let nix::errno::Errno::UnknownErrno = error_code {
                    panic!(
                        "should not send nix::errno::Errno::UnknownErrno to FUSE kernel, \
                            the error is: {}",
                        fs::util::format_anyhow_error(&err),
                    );
                } else {
                    error_code
                }
            } else {
                panic!(
                    "should not send non-nix::Error::Sys to FUSE kernel, the error is: {}",
                    fs::util::format_anyhow_error(&err),
                )
            }
        } else {
            panic!(
                "should not send non-nix error to FUSE kernel, the error is: {}",
                fs::util::format_anyhow_error(&err),
            );
        };
        #[allow(clippy::as_conversions)]
        self.send_error_code(error_code as c_int).await
    }
}

macro_rules! impl_fuse_reply_new_for{
    {$($t:ty,)+} => {
        $(impl $t {
            pub fn new(unique: u64, fd: RawFd) -> Self {
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
            pub async fn error_code(self, error_code: c_int) -> nix::Result<usize> {
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

#[cfg(target_os = "macos")]
impl_fuse_reply_error_for! {
    ReplyXTimes,
}
/// FUSE init response
#[derive(Debug)]
pub struct ReplyInit {
    /// The inner raw reply
    reply: ReplyRaw<FuseInitOut>,
}

impl ReplyInit {
    /// Reply init response
    pub async fn init(
        self,
        major: u32,
        minor: u32,
        max_readahead: u32,
        flags: u32,
        #[cfg(not(feature = "abi-7-13"))] unused: u32,
        #[cfg(feature = "abi-7-13")] max_background: u16,
        #[cfg(feature = "abi-7-13")] congestion_threshold: u16,
        max_write: u32,
        #[cfg(feature = "abi-7-23")] time_gran: u32,
        #[cfg(all(feature = "abi-7-23", not(feature = "abi-7-28")))] unused: [u32; 9],
        #[cfg(feature = "abi-7-28")] max_pages: u16,
        #[cfg(feature = "abi-7-28")] padding: u16,
        #[cfg(feature = "abi-7-28")] unused: [u32; 8],
    ) -> nix::Result<usize> {
        self.reply
            .send_data(FuseInitOut {
                major,
                minor,
                max_readahead,
                flags,
                #[cfg(not(feature = "abi-7-13"))]
                unused,
                #[cfg(feature = "abi-7-13")]
                max_background,
                #[cfg(feature = "abi-7-13")]
                congestion_threshold,
                max_write,
                #[cfg(feature = "abi-7-23")]
                time_gran,
                #[cfg(all(feature = "abi-7-23", not(feature = "abi-7-28")))]
                unused,
                #[cfg(feature = "abi-7-28")]
                max_pages,
                #[cfg(feature = "abi-7-28")]
                padding,
                #[cfg(feature = "abi-7-28")]
                unused,
            })
            .await
    }
}

/// FUSE empty response
#[derive(Debug)]
pub struct ReplyEmpty {
    /// The inner raw reply
    reply: ReplyRaw<()>,
}

impl ReplyEmpty {
    /// Reply with empty OK response
    pub async fn ok(self) -> nix::Result<usize> {
        self.reply.send_data(()).await
    }
}

/// FUSE data response
#[derive(Debug)]
pub struct ReplyData {
    /// The inner raw reply
    reply: ReplyRaw<Vec<u8>>,
}

impl ReplyData {
    /// Reply with byte data repsonse
    pub async fn data(self, bytes: Vec<u8>) -> nix::Result<usize> {
        self.reply.send_bytes(bytes).await
    }
}

/// FUSE entry response
#[derive(Debug)]
pub struct ReplyEntry {
    /// The inner raw reply
    reply: ReplyRaw<FuseEntryOut>,
}

impl ReplyEntry {
    /// Reply to a request with the given entry
    pub async fn entry(self, ttl: Duration, attr: FuseAttr, generation: u64) -> nix::Result<usize> {
        self.reply
            .send_data(FuseEntryOut {
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
    reply: ReplyRaw<FuseAttrOut>,
}

impl ReplyAttr {
    /// Reply to a request with the given attribute
    pub async fn attr(self, ttl: Duration, attr: FuseAttr) -> nix::Result<usize> {
        self.reply
            .send_data(FuseAttrOut {
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
            .send_data(FuseGetXTimesOut {
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
    reply: ReplyRaw<FuseOpenOut>,
}

impl ReplyOpen {
    /// Reply to a request with the given open result
    pub async fn opened(self, fh: RawFd, flags: u32) -> nix::Result<usize> {
        self.reply
            .send_data(FuseOpenOut {
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
    reply: ReplyRaw<FuseWriteOut>,
}

impl ReplyWrite {
    /// Reply to a request with the given open result
    pub async fn written(self, size: u32) -> nix::Result<usize> {
        self.reply
            .send_data(FuseWriteOut { size, padding: 0 })
            .await
    }
}

/// FUSE statfs response
#[derive(Debug)]
pub struct ReplyStatFs {
    /// The inner raw reply
    reply: ReplyRaw<FuseStatFsOut>,
}

/// POSIX statvfs parameters
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
            .send_data(FuseStatFsOut {
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
    reply: ReplyRaw<(FuseEntryOut, FuseOpenOut)>,
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
            .send_data((
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
    reply: ReplyRaw<FuseLockOut>,
}

impl ReplyLock {
    /// Reply to a request with the given open result
    #[allow(dead_code)]
    pub async fn locked(self, start: u64, end: u64, typ: u32, pid: u32) -> nix::Result<usize> {
        self.reply
            .send_data(FuseLockOut {
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
    reply: ReplyRaw<FuseBMapOut>,
}

impl ReplyBMap {
    /// Reply to a request with the given open result
    #[allow(dead_code)]
    pub async fn bmap(self, block: u64) -> nix::Result<usize> {
        self.reply.send_data(FuseBMapOut { block }).await
    }
}

/// FUSE directory response
#[derive(Debug)]
pub struct ReplyDirectory {
    /// The inner raw reply
    reply: ReplyRaw<()>,
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
        let name_bytes = name.as_ref().as_bytes();
        let entlen = mem::size_of::<FuseDirEnt>().overflow_add(name_bytes.len());
        let entsize = entlen.overflow_add(mem::size_of::<u64>().overflow_sub(1))
            & !(mem::size_of::<u64>().overflow_sub(1)); // 64bit align
        let padlen = entsize.overflow_sub(entlen);
        if self.data.len().overflow_add(entsize) > self.data.capacity() {
            return true;
        }
        // TODO: refactory this, do not use unsafe code
        unsafe {
            let data_end_ptr = self.data.as_mut_ptr().add(self.data.len());
            let pdirent: *mut FuseDirEnt = utilities::cast_to_mut_ptr(&mut *data_end_ptr);
            (*pdirent).ino = ino;
            (*pdirent).off = offset.cast();
            (*pdirent).namelen = name_bytes.len().cast();
            (*pdirent).typ = fs::util::mode_from_kind_and_perm(kind, 0).overflow_shr(12);
            let dirent_end_ptr = data_end_ptr.add(mem::size_of_val(&*pdirent));
            ptr::copy_nonoverlapping(name_bytes.as_ptr(), dirent_end_ptr, name_bytes.len());
            let name_end_ptr = dirent_end_ptr.add(name_bytes.len());
            ptr::write_bytes(name_end_ptr, 0_u8, padlen);
            let newlen = self.data.len().overflow_add(entsize);
            self.data.set_len(newlen);
        }
        false
    }

    /// Reply to a request with the filled directory buffer
    pub async fn ok(self) -> nix::Result<usize> {
        self.reply.send_bytes(self.data).await
    }
}

/// FUSE extended attribute response
#[derive(Debug)]
pub struct ReplyXAttr {
    /// The inner raw reply
    reply: ReplyRaw<FuseGetXAttrOut>,
}

impl ReplyXAttr {
    /// Reply to a request with the size of the xattr.
    #[allow(dead_code)]
    pub async fn size(self, size: u32) -> nix::Result<usize> {
        self.reply
            .send_data(FuseGetXAttrOut { size, padding: 0 })
            .await
    }

    /// Reply to a request with the data in the xattr.
    #[allow(dead_code)]
    pub async fn data(self, bytes: Vec<u8>) -> nix::Result<usize> {
        self.reply.send_bytes(bytes).await
    }
}

#[cfg(test)]
mod test {
    use super::super::byte_slice::ByteSlice;
    use super::super::protocol::{FuseAttr, FuseAttrOut, FuseOutHeader};
    use super::ReplyAttr;

    use aligned_bytes::AlignedBytes;
    use anyhow::Context;
    use futures::prelude::*;
    use nix::fcntl::{self, OFlag};
    use nix::sys::stat::Mode;
    use nix::unistd::{self, Whence};
    use smol::{self, blocking};
    use std::fs::File;
    use std::os::unix::io::FromRawFd;
    use std::time::Duration;

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
        smol::run(async move {
            let file_name = "fuse_reply.log";
            let fd = blocking!(fcntl::open(
                file_name,
                OFlag::O_CREAT | OFlag::O_TRUNC | OFlag::O_RDWR,
                Mode::all(),
            ))?;
            blocking!(unistd::unlink(file_name))?;

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

            // let file = blocking!(File::open(file_name))?;
            blocking!(unistd::lseek(fd, 0, Whence::SeekSet))?;
            let file = blocking!(unsafe { File::from_raw_fd(fd) });
            let mut file = smol::reader(file);
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes).await?;

            let mut aligned_bytes = AlignedBytes::new_zeroed(bytes.len(), 4096);
            aligned_bytes.copy_from_slice(&bytes);

            let mut bs = ByteSlice::new(&aligned_bytes);
            let foh: &FuseOutHeader = bs.fetch().context("failed to fetch FuseOutHeader")?;
            let fao: &FuseAttrOut = bs.fetch().context("failed to fetch FuseAttrOut")?;

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
