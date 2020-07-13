use anyhow::{self, Context};
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

use super::protocol::*;

// TODO: remove it
fn mode_from_kind_and_perm(kind: SFlag, perm: u16) -> u32 {
    (match kind {
        SFlag::S_IFIFO => libc::S_IFIFO,
        SFlag::S_IFCHR => libc::S_IFCHR,
        SFlag::S_IFBLK => libc::S_IFBLK,
        SFlag::S_IFDIR => libc::S_IFDIR,
        SFlag::S_IFREG => libc::S_IFREG,
        SFlag::S_IFLNK => libc::S_IFLNK,
        SFlag::S_IFSOCK => libc::S_IFSOCK,
        _ => panic!("unknown SFlag type={:?}", kind),
    }) as u32
        | perm as u32
}

#[derive(Debug)]
enum ToBytes<T> {
    Struct(T),
    Bytes(Vec<u8>),
    Error,
}

#[derive(Debug)]
struct ReplyRaw<T: Send + Sync + 'static> {
    unique: u64,
    fd: RawFd,
    marker: PhantomData<T>,
}

impl<T: Send + Sync + 'static> ReplyRaw<T> {
    fn new(unique: u64, fd: RawFd) -> Self {
        Self {
            unique,
            fd,
            marker: PhantomData,
        }
    }

    async fn send(self, to_bytes: ToBytes<T>, err: c_int) -> anyhow::Result<usize> {
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
                            let p = &instance as *const T as *const u8;
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
                len: (header_len + data_len) as u32,
                error: -err, // FUSE requires the error number to be negative
                unique: self.unique,
            };
            let h = &header as *const FuseOutHeader as *const u8;
            let header_bytes = unsafe { slice::from_raw_parts(h, header_len) };
            let iovecs: Vec<_> = if data_len > 0 {
                vec![IoVec::from_slice(header_bytes), IoVec::from_slice(bytes)]
            } else {
                vec![IoVec::from_slice(header_bytes)]
            };
            if !send_error {
                debug_assert_eq!(err, 0);
            } else {
                debug_assert_ne!(err, 0);
            }
            uio::writev(fd, &iovecs).context(format!(
                "failed to send to FUSE, the reply header is: {:?}", header,
            ))
        )?;

        debug!("sent {} bytes to fuse device successfully", wsize);
        Ok(wsize)
    }

    async fn send_bytes(self, byte_vec: Vec<u8>) -> anyhow::Result<()> {
        let _reply_size = self
            .send(ToBytes::Bytes(byte_vec), 0)
            .await
            .context("send_bytes() failed to send bytes")?;
        Ok(())
    }

    async fn send_data(self, instance: T) -> anyhow::Result<()> {
        let _reply_size = self
            .send(ToBytes::Struct(instance), 0)
            .await
            .context("send_data() failed to send data")?;
        Ok(())
    }

    async fn send_error(self, err: c_int) -> anyhow::Result<()> {
        let _reply_size = self
            .send(ToBytes::Error, err)
            .await
            .context("send_error() failed to send error")?;
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct ReplyInit {
    reply: ReplyRaw<FuseInitOut>,
}

impl ReplyInit {
    pub fn new(unique: u64, fd: RawFd) -> ReplyInit {
        ReplyInit {
            reply: ReplyRaw::new(unique, fd),
        }
    }
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
    ) -> anyhow::Result<()> {
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
    pub async fn error(self, err: c_int) -> anyhow::Result<()> {
        self.reply.send_error(err).await
    }
}

#[derive(Debug)]
pub(crate) struct ReplyEmpty {
    reply: ReplyRaw<()>,
}

impl ReplyEmpty {
    pub fn new(unique: u64, fd: RawFd) -> ReplyEmpty {
        ReplyEmpty {
            reply: ReplyRaw::new(unique, fd),
        }
    }
    pub async fn ok(self) -> anyhow::Result<()> {
        self.reply.send_data(()).await
    }
    pub async fn error(self, err: c_int) -> anyhow::Result<()> {
        self.reply.send_error(err).await
    }
}

#[derive(Debug)]
pub(crate) struct ReplyData {
    reply: ReplyRaw<Vec<u8>>,
}

impl ReplyData {
    pub fn new(unique: u64, fd: RawFd) -> ReplyData {
        ReplyData {
            reply: ReplyRaw::new(unique, fd),
        }
    }
    pub async fn data(self, bytes: Vec<u8>) -> anyhow::Result<()> {
        self.reply.send_bytes(bytes).await
    }
    pub async fn error(self, err: c_int) -> anyhow::Result<()> {
        self.reply.send_error(err).await
    }
}

#[derive(Debug)]
pub(crate) struct ReplyEntry {
    reply: ReplyRaw<FuseEntryOut>,
}

impl ReplyEntry {
    pub fn new(unique: u64, fd: RawFd) -> ReplyEntry {
        ReplyEntry {
            reply: ReplyRaw::new(unique, fd),
        }
    }
    /// Reply to a request with the given entry
    pub async fn entry(self, ttl: Duration, attr: FuseAttr, generation: u64) -> anyhow::Result<()> {
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

    /// Reply to a request with the given error code
    pub async fn error(self, err: c_int) -> anyhow::Result<()> {
        self.reply.send_error(err).await
    }
}

#[derive(Debug)]
pub(crate) struct ReplyAttr {
    reply: ReplyRaw<FuseAttrOut>,
}

impl ReplyAttr {
    pub fn new(unique: u64, fd: RawFd) -> ReplyAttr {
        ReplyAttr {
            reply: ReplyRaw::new(unique, fd),
        }
    }
    /// Reply to a request with the given attribute
    pub async fn attr(self, ttl: Duration, attr: FuseAttr) -> anyhow::Result<()> {
        self.reply
            .send_data(FuseAttrOut {
                attr_valid: ttl.as_secs(),
                attr_valid_nsec: ttl.subsec_nanos(),
                dummy: 0,
                attr,
            })
            .await
    }

    /// Reply to a request with the given error code
    pub async fn error(self, err: c_int) -> anyhow::Result<()> {
        self.reply.send_error(err).await
    }
}

#[cfg(target_os = "macos")]
#[derive(Debug)]
pub(crate) struct ReplyXTimes {
    reply: ReplyRaw<FuseGetXTimesOut>,
}

#[cfg(target_os = "macos")]
impl ReplyXTimes {
    pub fn new(unique: u64, fd: RawFd) -> ReplyXTimes {
        ReplyXTimes {
            reply: ReplyRaw::new(unique, fd),
        }
    }
    /// Reply to a request with the given xtimes
    // pub async fn xtimes(self, bkuptime: SystemTime, crtime: SystemTime) {
    pub async fn xtimes(
        self,
        bkuptime_secs: u64,
        bkuptime_nanos: u32,
        crtime_secs: u64,
        crtime_nanos: u32,
    ) -> anyhow::Result<()> {
        // let (bkuptime_secs, bkuptime_nanos) = time_from_system_time(&bkuptime);
        // let (crtime_secs, crtime_nanos) = time_from_system_time(&crtime);
        self.reply
            .send_data(FuseGetXTimesOut {
                bkuptime: bkuptime_secs,
                crtime: crtime_secs,
                bkuptimensec: bkuptime_nanos,
                crtimensec: crtime_nanos,
            })
            .await
    }

    /// Reply to a request with the given error code
    pub async fn error(self, err: c_int) -> anyhow::Result<()> {
        self.reply.send_error(err).await
    }
}

#[derive(Debug)]
pub(crate) struct ReplyOpen {
    reply: ReplyRaw<FuseOpenOut>,
}

impl ReplyOpen {
    pub fn new(unique: u64, fd: RawFd) -> ReplyOpen {
        ReplyOpen {
            reply: ReplyRaw::new(unique, fd),
        }
    }
    /// Reply to a request with the given open result
    pub async fn opened(self, fh: u64, flags: u32) -> anyhow::Result<()> {
        self.reply
            .send_data(FuseOpenOut {
                fh,
                open_flags: flags,
                padding: 0,
            })
            .await
    }

    /// Reply to a request with the given error code
    #[allow(dead_code)]
    pub async fn error(self, err: c_int) -> anyhow::Result<()> {
        self.reply.send_error(err).await
    }
}

#[derive(Debug)]
pub(crate) struct ReplyWrite {
    reply: ReplyRaw<FuseWriteOut>,
}

impl ReplyWrite {
    pub fn new(unique: u64, fd: RawFd) -> ReplyWrite {
        ReplyWrite {
            reply: ReplyRaw::new(unique, fd),
        }
    }
    /// Reply to a request with the given open result
    pub async fn written(self, size: u32) -> anyhow::Result<()> {
        self.reply
            .send_data(FuseWriteOut { size, padding: 0 })
            .await
    }

    /// Reply to a request with the given error code
    pub async fn error(self, err: c_int) -> anyhow::Result<()> {
        self.reply.send_error(err).await
    }
}

#[derive(Debug)]
pub(crate) struct ReplyStatFs {
    reply: ReplyRaw<FuseStatFsOut>,
}

impl ReplyStatFs {
    pub fn new(unique: u64, fd: RawFd) -> ReplyStatFs {
        ReplyStatFs {
            reply: ReplyRaw::new(unique, fd),
        }
    }

    pub async fn statfs(
        self,
        blocks: u64,
        bfree: u64,
        bavail: u64,
        files: u64,
        ffree: u64,
        bsize: u32,
        namelen: u32,
        frsize: u32,
    ) -> anyhow::Result<()> {
        self.reply
            .send_data(FuseStatFsOut {
                st: FuseKStatFs {
                    blocks,
                    bfree,
                    bavail,
                    files,
                    ffree,
                    bsize,
                    namelen,
                    frsize,
                    padding: 0,
                    spare: [0; 6],
                },
            })
            .await
    }

    /// Reply to a request with the given error code
    pub async fn error(self, err: c_int) -> anyhow::Result<()> {
        self.reply.send_error(err).await
    }
}

#[derive(Debug)]
pub(crate) struct ReplyCreate {
    reply: ReplyRaw<(FuseEntryOut, FuseOpenOut)>,
}

impl ReplyCreate {
    pub fn new(unique: u64, fd: RawFd) -> ReplyCreate {
        ReplyCreate {
            reply: ReplyRaw::new(unique, fd),
        }
    }
    /// Reply to a request with the given entry
    pub async fn created(
        self,
        ttl: &Duration,
        attr: FuseAttr,
        generation: u64,
        fh: u64,
        flags: u32,
    ) -> anyhow::Result<()> {
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

    /// Reply to a request with the given error code
    pub async fn error(self, err: c_int) -> anyhow::Result<()> {
        self.reply.send_error(err).await
    }
}

#[derive(Debug)]
pub(crate) struct ReplyLock {
    reply: ReplyRaw<FuseLockOut>,
}

impl ReplyLock {
    pub fn new(unique: u64, fd: RawFd) -> ReplyLock {
        ReplyLock {
            reply: ReplyRaw::new(unique, fd),
        }
    }
    /// Reply to a request with the given open result
    pub async fn locked(self, start: u64, end: u64, typ: u32, pid: u32) -> anyhow::Result<()> {
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

    /// Reply to a request with the given error code
    pub async fn error(self, err: c_int) -> anyhow::Result<()> {
        self.reply.send_error(err).await
    }
}

#[derive(Debug)]
pub(crate) struct ReplyBMap {
    reply: ReplyRaw<FuseBMapOut>,
}

impl ReplyBMap {
    pub fn new(unique: u64, fd: RawFd) -> ReplyBMap {
        ReplyBMap {
            reply: ReplyRaw::new(unique, fd),
        }
    }
    /// Reply to a request with the given open result
    pub async fn bmap(self, block: u64) -> anyhow::Result<()> {
        self.reply.send_data(FuseBMapOut { block }).await
    }

    /// Reply to a request with the given error code
    pub async fn error(self, err: c_int) -> anyhow::Result<()> {
        self.reply.send_error(err).await
    }
}

#[derive(Debug)]
pub(crate) struct ReplyDirectory {
    reply: ReplyRaw<()>,
    data: Vec<u8>,
}

impl ReplyDirectory {
    /// Creates a new ReplyDirectory with a specified buffer size.
    pub fn new(unique: u64, fd: RawFd, size: usize) -> ReplyDirectory {
        ReplyDirectory {
            reply: ReplyRaw::new(unique, fd),
            data: Vec::with_capacity(size),
        }
    }

    /// Add an entry to the directory reply buffer. Returns true if the buffer is full.
    /// A transparent offset value can be provided for each entry. The kernel uses these
    /// value to request the next entries in further readdir calls
    pub fn add<T: AsRef<OsStr>>(&mut self, ino: u64, offset: i64, kind: SFlag, name: T) -> bool {
        let name = name.as_ref().as_bytes();
        let entlen = mem::size_of::<FuseDirEnt>() + name.len();
        let entsize = (entlen + mem::size_of::<u64>() - 1) & !(mem::size_of::<u64>() - 1); // 64bit align
        let padlen = entsize - entlen;
        if self.data.len() + entsize > self.data.capacity() {
            return true;
        }
        unsafe {
            let p = self.data.as_mut_ptr().add(self.data.len());
            let pdirent: *mut FuseDirEnt = mem::transmute(p);
            (*pdirent).ino = ino;
            (*pdirent).off = offset as u64;
            (*pdirent).namelen = name.len() as u32;
            (*pdirent).typ = mode_from_kind_and_perm(kind, 0) >> 12;
            let p = p.add(mem::size_of_val(&*pdirent));
            ptr::copy_nonoverlapping(name.as_ptr(), p, name.len());
            let p = p.add(name.len());
            ptr::write_bytes(p, 0u8, padlen);
            let newlen = self.data.len() + entsize;
            self.data.set_len(newlen);
        }
        false
    }

    /// Reply to a request with the filled directory buffer
    pub async fn ok(self) -> anyhow::Result<()> {
        self.reply.send_bytes(self.data).await
    }

    /// Reply to a request with the given error code
    pub async fn error(self, err: c_int) -> anyhow::Result<()> {
        self.reply.send_error(err).await
    }
}

#[derive(Debug)]
pub(crate) struct ReplyXAttr {
    reply: ReplyRaw<FuseGetXAttrOut>,
}

impl ReplyXAttr {
    pub fn new(unique: u64, fd: RawFd) -> ReplyXAttr {
        ReplyXAttr {
            reply: ReplyRaw::new(unique, fd),
        }
    }
    /// Reply to a request with the size of the xattr.
    pub async fn size(self, size: u32) -> anyhow::Result<()> {
        self.reply
            .send_data(FuseGetXAttrOut { size, padding: 0 })
            .await
    }

    /// Reply to a request with the data in the xattr.
    pub async fn data(self, bytes: Vec<u8>) -> anyhow::Result<()> {
        self.reply.send_bytes(bytes).await
    }

    /// Reply to a request with the given error code.
    pub async fn error(self, err: c_int) -> anyhow::Result<()> {
        self.reply.send_error(err).await
    }
}

#[cfg(test)]
mod test {
    use super::super::fuse_request::ByteSlice;
    use super::super::protocol::{FuseAttr, FuseAttrOut, FuseOutHeader};
    use super::ReplyAttr;
    use futures::prelude::*;
    use nix::fcntl::{self, OFlag};
    use nix::sys::stat::Mode;
    use nix::unistd::{self, Whence};
    use smol::{self, blocking};
    use std::fs::File;
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
            let atime = 64;
            let mtime = 64;
            let ctime = 64;
            #[cfg(target_os = "macos")]
            let crtime = 64;
            let atimensec = 32;
            let mtimensec = 32;
            let ctimensec = 32;
            #[cfg(target_os = "macos")]
            let crtimensec = 32;
            let mode = 32;
            let nlink = 32;
            let uid = 32;
            let gid = 32;
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
                atime,
                mtime,
                ctime,
                #[cfg(target_os = "macos")]
                crtime,
                atimensec,
                mtimensec,
                ctimensec,
                #[cfg(target_os = "macos")]
                crtimensec,
                mode,
                nlink,
                uid,
                gid,
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
            use std::os::unix::io::FromRawFd;
            let file = blocking!(unsafe { File::from_raw_fd(fd) });
            let mut file = smol::reader(file);
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes).await?;

            let mut bs = ByteSlice::new(&bytes);
            let foh: &FuseOutHeader = bs.fetch().unwrap();
            let fao: &FuseAttrOut = bs.fetch().unwrap();

            dbg!(foh, fao);
            debug_assert_eq!(fao.attr.ino, ino);
            debug_assert_eq!(fao.attr.size, size);
            debug_assert_eq!(fao.attr.blocks, blocks);
            debug_assert_eq!(fao.attr.atime, atime);
            debug_assert_eq!(fao.attr.mtime, mtime);
            debug_assert_eq!(fao.attr.ctime, ctime);
            Ok(())
        })
    }
}
