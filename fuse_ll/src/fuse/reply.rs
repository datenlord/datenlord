//! Filesystem operation reply
//!
//! A reply is passed to filesystem operation implementations and must be used to send back the
//! result of an operation. The reply can optionally be sent to another thread to asynchronously
//! work on an operation and provide the result later. Also it allows replying with a block of
//! data without cloning the data. A reply *must always* be used (by calling either ok() or
//! error() exactly once).

use libc::{EIO, S_IFBLK, S_IFCHR, S_IFDIR, S_IFIFO, S_IFLNK, S_IFREG, S_IFSOCK};
use log::warn;
use std::convert::AsRef;
use std::ffi::OsStr;
use std::fmt;
use std::marker::PhantomData;
use std::os::raw::c_int;
use std::os::unix::ffi::OsStrExt;
use std::time::{Duration, SystemTime, SystemTimeError, UNIX_EPOCH};
use std::{mem, ptr, slice};

#[cfg(target_os = "macos")]
use super::abi::fuse_getxtimes_out;
use super::abi::{
    fuse_attr, fuse_attr_out, fuse_bmap_out, fuse_dirent, fuse_entry_out, fuse_file_lock,
    fuse_getxattr_out, fuse_kstatfs, fuse_lk_out, fuse_open_out, fuse_out_header, fuse_statfs_out,
    fuse_write_out,
};
use super::{FileAttr, FileType};

/// Generic reply callback to send data
pub trait ReplySender: Send + 'static {
    /// Send data.
    fn send(&self, data: &[&[u8]]);
}

impl fmt::Debug for Box<dyn ReplySender> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "Box<ReplySender>")
    }
}

/// Generic reply trait
pub trait Reply {
    /// Create a new reply for the given request
    fn new<S: ReplySender>(unique: u64, sender: S) -> Self;
}

/// Serialize an arbitrary type to bytes (memory copy, useful for fuse_*_out types)
fn as_bytes<T, U, F: FnOnce(&[&[u8]]) -> U>(data: &T, f: F) -> U {
    let len = mem::size_of::<T>();
    match len {
        0 => f(&[]),
        len => {
            let p = data as *const T as *const u8;
            let bytes = unsafe { slice::from_raw_parts(p, len) };
            f(&[bytes])
        }
    }
}

fn time_from_system_time(system_time: &SystemTime) -> Result<(u64, u32), SystemTimeError> {
    let duration = system_time.duration_since(UNIX_EPOCH)?;
    Ok((duration.as_secs(), duration.subsec_nanos()))
}

// Some platforms like Linux x86_64 have mode_t = u32, and lint warns of a trivial_numeric_casts.
// But others like macOS x86_64 have mode_t = u16, requiring a typecast.  So, just silence lint.
#[allow(trivial_numeric_casts)]
/// Returns the mode for a given file kind and permission
fn mode_from_kind_and_perm(kind: FileType, perm: u16) -> u32 {
    (match kind {
        FileType::NamedPipe => S_IFIFO,
        FileType::CharDevice => S_IFCHR,
        FileType::BlockDevice => S_IFBLK,
        FileType::Directory => S_IFDIR,
        FileType::RegularFile => S_IFREG,
        FileType::Symlink => S_IFLNK,
        FileType::Socket => S_IFSOCK,
    }) as u32
        | perm as u32
}

/// Returns a fuse_attr from FileAttr
#[cfg(target_os = "macos")]
fn fuse_attr_from_attr(attr: &FileAttr) -> fuse_attr {
    // FIXME: unwrap may panic, use unwrap_or((0, 0)) or return a result instead?
    let (atime_secs, atime_nanos) = time_from_system_time(&attr.atime).unwrap();
    let (mtime_secs, mtime_nanos) = time_from_system_time(&attr.mtime).unwrap();
    let (ctime_secs, ctime_nanos) = time_from_system_time(&attr.ctime).unwrap();
    let (crtime_secs, crtime_nanos) = time_from_system_time(&attr.crtime).unwrap();

    fuse_attr {
        ino: attr.ino,
        size: attr.size,
        blocks: attr.blocks,
        atime: atime_secs,
        mtime: mtime_secs,
        ctime: ctime_secs,
        crtime: crtime_secs,
        atimensec: atime_nanos,
        mtimensec: mtime_nanos,
        ctimensec: ctime_nanos,
        crtimensec: crtime_nanos,
        mode: mode_from_kind_and_perm(attr.kind, attr.perm),
        nlink: attr.nlink,
        uid: attr.uid,
        gid: attr.gid,
        rdev: attr.rdev,
        flags: attr.flags,
    }
}

/// Returns a fuse_attr from FileAttr
#[cfg(not(target_os = "macos"))]
fn fuse_attr_from_attr(attr: &FileAttr) -> fuse_attr {
    // FIXME: unwrap may panic, use unwrap_or((0, 0)) or return a result instead?
    let (atime_secs, atime_nanos) = time_from_system_time(&attr.atime).unwrap();
    let (mtime_secs, mtime_nanos) = time_from_system_time(&attr.mtime).unwrap();
    let (ctime_secs, ctime_nanos) = time_from_system_time(&attr.ctime).unwrap();

    fuse_attr {
        ino: attr.ino,
        size: attr.size,
        blocks: attr.blocks,
        atime: atime_secs,
        mtime: mtime_secs,
        ctime: ctime_secs,
        atimensec: atime_nanos,
        mtimensec: mtime_nanos,
        ctimensec: ctime_nanos,
        mode: mode_from_kind_and_perm(attr.kind, attr.perm),
        nlink: attr.nlink,
        uid: attr.uid,
        gid: attr.gid,
        rdev: attr.rdev,
    }
}

///
/// Raw reply
///
#[derive(Debug)]
pub struct ReplyRaw<T> {
    /// Unique id of the request to reply to
    unique: u64,
    /// Closure to call for sending the reply
    sender: Option<Box<dyn ReplySender>>,
    /// Marker for being able to have T on this struct (which enforces
    /// reply types to send the correct type of data)
    marker: PhantomData<T>,
}

impl<T> Reply for ReplyRaw<T> {
    fn new<S: ReplySender>(unique: u64, sender: S) -> ReplyRaw<T> {
        let sender = Box::new(sender);
        ReplyRaw {
            unique,
            sender: Some(sender),
            marker: PhantomData,
        }
    }
}

impl<T> ReplyRaw<T> {
    /// Reply to a request with the given error code and data. Must be called
    /// only once (the `ok` and `error` methods ensure this by consuming `self`)
    fn send(&mut self, err: c_int, bytes: &[&[u8]]) {
        assert!(self.sender.is_some());
        let len = bytes.iter().fold(0, |l, b| l + b.len());
        let header = fuse_out_header {
            len: (mem::size_of::<fuse_out_header>() + len) as u32,
            error: -err,
            unique: self.unique,
        };
        as_bytes(&header, |headerbytes| {
            let sender = self.sender.take().unwrap();
            let mut sendbytes = headerbytes.to_vec();
            sendbytes.extend(bytes);
            sender.send(&sendbytes);
        });
    }

    /// Reply to a request with the given type
    pub fn ok(mut self, data: &T) {
        as_bytes(data, |bytes| {
            self.send(0, bytes);
        })
    }

    /// Reply to a request with the given error code
    pub fn error(mut self, err: c_int) {
        self.send(err, &[]);
    }
}

impl<T> Drop for ReplyRaw<T> {
    fn drop(&mut self) {
        if self.sender.is_some() {
            warn!(
                "Reply not sent for operation {}, replying with I/O error",
                self.unique
            );
            self.send(EIO, &[]);
        }
    }
}

///
/// Empty reply
///
#[derive(Debug)]
pub struct ReplyEmpty {
    reply: ReplyRaw<()>,
}

impl Reply for ReplyEmpty {
    fn new<S: ReplySender>(unique: u64, sender: S) -> ReplyEmpty {
        ReplyEmpty {
            reply: Reply::new(unique, sender),
        }
    }
}

impl ReplyEmpty {
    /// Reply to a request with nothing
    pub fn ok(mut self) {
        self.reply.send(0, &[]);
    }

    /// Reply to a request with the given error code
    pub fn error(self, err: c_int) {
        self.reply.error(err);
    }
}

///
/// Data reply
///
#[derive(Debug)]
pub struct ReplyData {
    reply: ReplyRaw<()>,
}

impl Reply for ReplyData {
    fn new<S: ReplySender>(unique: u64, sender: S) -> ReplyData {
        ReplyData {
            reply: Reply::new(unique, sender),
        }
    }
}

impl ReplyData {
    /// Reply to a request with the given data
    pub fn data(mut self, data: &[u8]) {
        self.reply.send(0, &[data]);
    }

    /// Reply to a request with the given error code
    pub fn error(self, err: c_int) {
        self.reply.error(err);
    }
}

///
/// Entry reply
///
#[derive(Debug)]
pub struct ReplyEntry {
    reply: ReplyRaw<fuse_entry_out>,
}

impl Reply for ReplyEntry {
    fn new<S: ReplySender>(unique: u64, sender: S) -> ReplyEntry {
        ReplyEntry {
            reply: Reply::new(unique, sender),
        }
    }
}

impl ReplyEntry {
    /// Reply to a request with the given entry
    pub fn entry(self, ttl: &Duration, attr: &FileAttr, generation: u64) {
        self.reply.ok(&fuse_entry_out {
            nodeid: attr.ino,
            generation,
            entry_valid: ttl.as_secs(),
            attr_valid: ttl.as_secs(),
            entry_valid_nsec: ttl.subsec_nanos(),
            attr_valid_nsec: ttl.subsec_nanos(),
            attr: fuse_attr_from_attr(attr),
        });
    }

    /// Reply to a request with the given error code
    pub fn error(self, err: c_int) {
        self.reply.error(err);
    }
}

///
/// Attribute Reply
///
#[derive(Debug)]
pub struct ReplyAttr {
    reply: ReplyRaw<fuse_attr_out>,
}

impl Reply for ReplyAttr {
    fn new<S: ReplySender>(unique: u64, sender: S) -> ReplyAttr {
        ReplyAttr {
            reply: Reply::new(unique, sender),
        }
    }
}

impl ReplyAttr {
    /// Reply to a request with the given attribute
    pub fn attr(self, ttl: &Duration, attr: &FileAttr) {
        self.reply.ok(&fuse_attr_out {
            attr_valid: ttl.as_secs(),
            attr_valid_nsec: ttl.subsec_nanos(),
            dummy: 0,
            attr: fuse_attr_from_attr(attr),
        });
    }

    /// Reply to a request with the given error code
    pub fn error(self, err: c_int) {
        self.reply.error(err);
    }
}

///
/// XTimes Reply
///
#[cfg(target_os = "macos")]
#[derive(Debug)]
pub struct ReplyXTimes {
    reply: ReplyRaw<fuse_getxtimes_out>,
}

#[cfg(target_os = "macos")]
impl Reply for ReplyXTimes {
    fn new<S: ReplySender>(unique: u64, sender: S) -> ReplyXTimes {
        ReplyXTimes {
            reply: Reply::new(unique, sender),
        }
    }
}

#[cfg(target_os = "macos")]
impl ReplyXTimes {
    /// Reply to a request with the given xtimes
    #[allow(dead_code)]
    pub fn xtimes(self, bkuptime: SystemTime, crtime: SystemTime) {
        // FIXME: unwrap may panic, use unwrap_or((0, 0)) or return a result instead?
        let (bkuptime_secs, bkuptime_nanos) = time_from_system_time(&bkuptime).unwrap();
        let (crtime_secs, crtime_nanos) = time_from_system_time(&crtime).unwrap();
        self.reply.ok(&fuse_getxtimes_out {
            bkuptime: bkuptime_secs,
            crtime: crtime_secs,
            bkuptimensec: bkuptime_nanos,
            crtimensec: crtime_nanos,
        });
    }

    /// Reply to a request with the given error code
    pub fn error(self, err: c_int) {
        self.reply.error(err);
    }
}

///
/// Open Reply
///
#[derive(Debug)]
pub struct ReplyOpen {
    reply: ReplyRaw<fuse_open_out>,
}

impl Reply for ReplyOpen {
    fn new<S: ReplySender>(unique: u64, sender: S) -> ReplyOpen {
        ReplyOpen {
            reply: Reply::new(unique, sender),
        }
    }
}

impl ReplyOpen {
    /// Reply to a request with the given open result
    pub fn opened(self, fh: u64, flags: u32) {
        self.reply.ok(&fuse_open_out {
            fh,
            open_flags: flags,
            padding: 0,
        });
    }

    /// Reply to a request with the given error code
    #[allow(dead_code)]
    pub fn error(self, err: c_int) {
        self.reply.error(err);
    }
}

///
/// Write Reply
///
#[derive(Debug)]
pub struct ReplyWrite {
    reply: ReplyRaw<fuse_write_out>,
}

impl Reply for ReplyWrite {
    fn new<S: ReplySender>(unique: u64, sender: S) -> ReplyWrite {
        ReplyWrite {
            reply: Reply::new(unique, sender),
        }
    }
}

impl ReplyWrite {
    /// Reply to a request with the given open result
    pub fn written(self, size: u32) {
        self.reply.ok(&fuse_write_out { size, padding: 0 });
    }

    /// Reply to a request with the given error code
    pub fn error(self, err: c_int) {
        self.reply.error(err);
    }
}

///
/// Statfs Reply
///
#[derive(Debug)]
pub struct ReplyStatfs {
    reply: ReplyRaw<fuse_statfs_out>,
}

impl Reply for ReplyStatfs {
    fn new<S: ReplySender>(unique: u64, sender: S) -> ReplyStatfs {
        ReplyStatfs {
            reply: Reply::new(unique, sender),
        }
    }
}

/// Param passed to Statfs Reply
#[derive(Debug)]
pub struct ReplyStatfsParam {
    /// Total data blocks in filesystem
    pub blocks: u64,
    /// Free blocks in filesystem
    pub bfree: u64,
    /// Free blocks available to unprivileged user
    pub bavail: u64,
    /// Total inodes in filesystem
    pub files: u64,
    /// Free inodes in filesystem
    pub ffree: u64,
    /// Optimal transfer block size
    pub bsize: u32,
    /// Maximum length of filenames
    pub namelen: u32,
    /// Fragment size (since Linux 2.6)
    pub frsize: u32,
}

impl ReplyStatfs {
    /// Reply to a request with the given open result
    pub fn statfs(self, param: &ReplyStatfsParam) {
        self.reply.ok(&fuse_statfs_out {
            st: fuse_kstatfs {
                blocks: param.blocks,
                bfree: param.bfree,
                bavail: param.bavail,
                files: param.files,
                ffree: param.ffree,
                bsize: param.bsize,
                namelen: param.namelen,
                frsize: param.frsize,
                padding: 0,
                spare: [0; 6],
            },
        });
    }

    /// Reply to a request with the given error code
    #[allow(dead_code)]
    pub fn error(self, err: c_int) {
        self.reply.error(err);
    }
}

///
/// Create reply
///
#[derive(Debug)]
pub struct ReplyCreate {
    reply: ReplyRaw<(fuse_entry_out, fuse_open_out)>,
}

impl Reply for ReplyCreate {
    fn new<S: ReplySender>(unique: u64, sender: S) -> ReplyCreate {
        ReplyCreate {
            reply: Reply::new(unique, sender),
        }
    }
}

impl ReplyCreate {
    /// Reply to a request with the given entry
    #[allow(dead_code)]
    pub fn created(self, ttl: &Duration, attr: &FileAttr, generation: u64, fh: u64, flags: u32) {
        self.reply.ok(&(
            fuse_entry_out {
                nodeid: attr.ino,
                generation,
                entry_valid: ttl.as_secs(),
                attr_valid: ttl.as_secs(),
                entry_valid_nsec: ttl.subsec_nanos(),
                attr_valid_nsec: ttl.subsec_nanos(),
                attr: fuse_attr_from_attr(attr),
            },
            fuse_open_out {
                fh,
                open_flags: flags,
                padding: 0,
            },
        ));
    }

    /// Reply to a request with the given error code
    pub fn error(self, err: c_int) {
        self.reply.error(err);
    }
}

///
/// Lock Reply
///
#[derive(Debug)]
pub struct ReplyLock {
    reply: ReplyRaw<fuse_lk_out>,
}

impl Reply for ReplyLock {
    fn new<S: ReplySender>(unique: u64, sender: S) -> ReplyLock {
        ReplyLock {
            reply: Reply::new(unique, sender),
        }
    }
}

impl ReplyLock {
    /// Reply to a request with the given open result
    #[allow(dead_code)]
    pub fn locked(self, start: u64, end: u64, typ: u32, pid: u32) {
        self.reply.ok(&fuse_lk_out {
            lk: fuse_file_lock {
                start,
                end,
                typ,
                pid,
            },
        });
    }

    /// Reply to a request with the given error code
    pub fn error(self, err: c_int) {
        self.reply.error(err);
    }
}

///
/// Bmap Reply
///
#[derive(Debug)]
pub struct ReplyBmap {
    reply: ReplyRaw<fuse_bmap_out>,
}

impl Reply for ReplyBmap {
    fn new<S: ReplySender>(unique: u64, sender: S) -> ReplyBmap {
        ReplyBmap {
            reply: Reply::new(unique, sender),
        }
    }
}

impl ReplyBmap {
    /// Reply to a request with the given open result
    #[allow(dead_code)]
    pub fn bmap(self, block: u64) {
        self.reply.ok(&fuse_bmap_out { block });
    }

    /// Reply to a request with the given error code
    pub fn error(self, err: c_int) {
        self.reply.error(err);
    }
}

///
/// Directory reply
///
#[derive(Debug)]
pub struct ReplyDirectory {
    reply: ReplyRaw<()>,
    data: Vec<u8>,
}

impl ReplyDirectory {
    /// Creates a new ReplyDirectory with a specified buffer size.
    pub fn new<S: ReplySender>(unique: u64, sender: S, size: usize) -> ReplyDirectory {
        ReplyDirectory {
            reply: Reply::new(unique, sender),
            data: Vec::with_capacity(size),
        }
    }

    /// Add an entry to the directory reply buffer. Returns true if the buffer is full.
    /// A transparent offset value can be provided for each entry. The kernel uses these
    /// value to request the next entries in further readdir calls
    pub fn add<T: AsRef<OsStr>>(&mut self, ino: u64, offset: i64, kind: FileType, name: T) -> bool {
        let name = name.as_ref().as_bytes();
        let entlen = mem::size_of::<fuse_dirent>() + name.len();
        let entsize = (entlen + mem::size_of::<u64>() - 1) & !(mem::size_of::<u64>() - 1); // 64bit align
        let padlen = entsize - entlen;
        if self.data.len() + entsize > self.data.capacity() {
            return true;
        }
        unsafe {
            let p = self.data.as_mut_ptr().add(self.data.len());
            // The following serialization won't produce error
            // because the size is checked before
            // TODO: Change the Param to use fuse_dirent directly
            bincode::serialize_into(&mut self.data, &ino).unwrap();
            bincode::serialize_into(&mut self.data, &(offset as u64)).unwrap();
            bincode::serialize_into(&mut self.data, &(name.len() as u32)).unwrap();
            bincode::serialize_into(&mut self.data, &(mode_from_kind_and_perm(kind, 0) >> 12))
                .unwrap();
            let p = p.add(mem::size_of::<fuse_dirent>());
            ptr::copy_nonoverlapping(name.as_ptr(), p, name.len());
            let p = p.add(name.len());
            ptr::write_bytes(p, 0u8, padlen);
            let newlen = self.data.len() + padlen + name.len();
            self.data.set_len(newlen);
        }
        false
    }

    /// Reply to a request with the filled directory buffer
    pub fn ok(mut self) {
        self.reply.send(0, &[&self.data]);
    }

    /// Reply to a request with the given error code
    pub fn error(self, err: c_int) {
        self.reply.error(err);
    }
}

///
/// Xattr reply
///
#[derive(Debug)]
pub struct ReplyXattr {
    reply: ReplyRaw<fuse_getxattr_out>,
}

impl Reply for ReplyXattr {
    fn new<S: ReplySender>(unique: u64, sender: S) -> ReplyXattr {
        ReplyXattr {
            reply: Reply::new(unique, sender),
        }
    }
}

impl ReplyXattr {
    /// Reply to a request with the size of the xattr.
    #[allow(dead_code)]
    pub fn size(self, size: u32) {
        self.reply.ok(&fuse_getxattr_out { size, padding: 0 });
    }

    /// Reply to a request with the data in the xattr.
    #[allow(dead_code)]
    pub fn data(mut self, data: &[u8]) {
        self.reply.send(0, &[data]);
    }

    /// Reply to a request with the given error code.
    pub fn error(self, err: c_int) {
        self.reply.error(err);
    }
}

#[cfg(test)]
mod test {
    use super::as_bytes;
    #[cfg(target_os = "macos")]
    use super::ReplyXTimes;
    use super::ReplyXattr;
    use super::{FileAttr, FileType};
    use super::{
        Reply, ReplyAttr, ReplyData, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyRaw, ReplyStatfsParam,
    };
    use super::{ReplyBmap, ReplyCreate, ReplyDirectory, ReplyLock, ReplyStatfs, ReplyWrite};
    use std::sync::mpsc::{channel, Sender};
    use std::thread;
    use std::time::{Duration, UNIX_EPOCH};

    #[allow(dead_code)]
    #[repr(C)]
    struct Data {
        a: u8,
        b: u8,
        c: u16,
    }

    #[test]
    fn serialize_empty() {
        let data = ();
        as_bytes(&data, |bytes| {
            assert!(bytes.is_empty());
        });
    }

    #[test]
    fn serialize_slice() {
        let data: [u8; 4] = [0x12, 0x34, 0x56, 0x78];
        as_bytes(&data, |bytes| {
            assert_eq!(bytes, [[0x12, 0x34, 0x56, 0x78]]);
        });
    }

    #[test]
    fn serialize_struct() {
        let data = Data {
            a: 0x12,
            b: 0x34,
            c: 0x5678,
        };
        as_bytes(&data, |bytes| {
            assert_eq!(bytes, [[0x12, 0x34, 0x78, 0x56]]);
        });
    }

    #[test]
    fn serialize_tuple() {
        let data = (
            Data {
                a: 0x12,
                b: 0x34,
                c: 0x5678,
            },
            Data {
                a: 0x9a,
                b: 0xbc,
                c: 0xdef0,
            },
        );
        as_bytes(&data, |bytes| {
            assert_eq!(bytes, [[0x12, 0x34, 0x78, 0x56, 0x9a, 0xbc, 0xf0, 0xde]]);
        });
    }

    struct AssertSender {
        expected: Vec<Vec<u8>>,
    }

    impl super::ReplySender for AssertSender {
        fn send(&self, data: &[&[u8]]) {
            assert_eq!(self.expected, data);
        }
    }

    #[test]
    fn reply_raw() {
        let data = Data {
            a: 0x12,
            b: 0x34,
            c: 0x5678,
        };
        let sender = AssertSender {
            expected: vec![
                vec![
                    0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x00,
                    0x00, 0x00, 0x00,
                ],
                vec![0x12, 0x34, 0x78, 0x56],
            ],
        };
        let reply: ReplyRaw<Data> = Reply::new(0xdeadbeef, sender);
        reply.ok(&data);
    }

    #[test]
    fn reply_error() {
        let sender = AssertSender {
            expected: vec![vec![
                0x10, 0x00, 0x00, 0x00, 0xbe, 0xff, 0xff, 0xff, 0xef, 0xbe, 0xad, 0xde, 0x00, 0x00,
                0x00, 0x00,
            ]],
        };
        let reply: ReplyRaw<Data> = Reply::new(0xdeadbeef, sender);
        reply.error(66);
    }

    #[test]
    fn reply_empty() {
        let sender = AssertSender {
            expected: vec![vec![
                0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x00, 0x00,
                0x00, 0x00,
            ]],
        };
        let reply: ReplyEmpty = Reply::new(0xdeadbeef, sender);
        reply.ok();
    }

    #[test]
    fn reply_data() {
        let sender = AssertSender {
            expected: vec![
                vec![
                    0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x00,
                    0x00, 0x00, 0x00,
                ],
                vec![0xde, 0xad, 0xbe, 0xef],
            ],
        };
        let reply: ReplyData = Reply::new(0xdeadbeef, sender);
        reply.data(&[0xde, 0xad, 0xbe, 0xef]);
    }

    #[test]
    fn reply_entry() {
        let sender = AssertSender {
            expected: if cfg!(target_os = "macos") {
                vec![
                    vec![
                        0x98, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde,
                        0x00, 0x00, 0x00, 0x00,
                    ],
                    vec![
                        0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xaa, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x65, 0x87, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x65, 0x87, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x21, 0x43, 0x00, 0x00,
                        0x21, 0x43, 0x00, 0x00, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x22, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x33, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x12, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x78, 0x56, 0x00, 0x00, 0x78, 0x56, 0x00, 0x00, 0x78, 0x56, 0x00, 0x00,
                        0x78, 0x56, 0x00, 0x00, 0xa4, 0x81, 0x00, 0x00, 0x55, 0x00, 0x00, 0x00,
                        0x66, 0x00, 0x00, 0x00, 0x77, 0x00, 0x00, 0x00, 0x88, 0x00, 0x00, 0x00,
                        0x99, 0x00, 0x00, 0x00,
                    ],
                ]
            } else {
                vec![
                    vec![
                        0x88, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde,
                        0x00, 0x00, 0x00, 0x00,
                    ],
                    vec![
                        0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xaa, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x65, 0x87, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x65, 0x87, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x21, 0x43, 0x00, 0x00,
                        0x21, 0x43, 0x00, 0x00, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x22, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x33, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x12, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x78, 0x56, 0x00, 0x00, 0x78, 0x56, 0x00, 0x00,
                        0x78, 0x56, 0x00, 0x00, 0xa4, 0x81, 0x00, 0x00, 0x55, 0x00, 0x00, 0x00,
                        0x66, 0x00, 0x00, 0x00, 0x77, 0x00, 0x00, 0x00, 0x88, 0x00, 0x00, 0x00,
                    ],
                ]
            },
        };
        let reply: ReplyEntry = Reply::new(0xdeadbeef, sender);
        let time = UNIX_EPOCH + Duration::new(0x1234, 0x5678);
        let ttl = Duration::new(0x8765, 0x4321);
        let attr = FileAttr {
            ino: 0x11,
            size: 0x22,
            blocks: 0x33,
            atime: time,
            mtime: time,
            ctime: time,
            crtime: time,
            kind: FileType::RegularFile,
            perm: 0o644,
            nlink: 0x55,
            uid: 0x66,
            gid: 0x77,
            rdev: 0x88,
            flags: 0x99,
        };
        reply.entry(&ttl, &attr, 0xaa);
    }

    #[test]
    fn reply_attr() {
        let sender = AssertSender {
            expected: if cfg!(target_os = "macos") {
                vec![
                    vec![
                        0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde,
                        0x00, 0x00, 0x00, 0x00,
                    ],
                    vec![
                        0x65, 0x87, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x21, 0x43, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x22, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x33, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x12, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x78, 0x56, 0x00, 0x00, 0x78, 0x56, 0x00, 0x00, 0x78, 0x56, 0x00, 0x00,
                        0x78, 0x56, 0x00, 0x00, 0xa4, 0x81, 0x00, 0x00, 0x55, 0x00, 0x00, 0x00,
                        0x66, 0x00, 0x00, 0x00, 0x77, 0x00, 0x00, 0x00, 0x88, 0x00, 0x00, 0x00,
                        0x99, 0x00, 0x00, 0x00,
                    ],
                ]
            } else {
                vec![
                    vec![
                        0x70, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde,
                        0x00, 0x00, 0x00, 0x00,
                    ],
                    vec![
                        0x65, 0x87, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x21, 0x43, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x22, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x33, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x12, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x78, 0x56, 0x00, 0x00, 0x78, 0x56, 0x00, 0x00,
                        0x78, 0x56, 0x00, 0x00, 0xa4, 0x81, 0x00, 0x00, 0x55, 0x00, 0x00, 0x00,
                        0x66, 0x00, 0x00, 0x00, 0x77, 0x00, 0x00, 0x00, 0x88, 0x00, 0x00, 0x00,
                    ],
                ]
            },
        };
        let reply: ReplyAttr = Reply::new(0xdeadbeef, sender);
        let time = UNIX_EPOCH + Duration::new(0x1234, 0x5678);
        let ttl = Duration::new(0x8765, 0x4321);
        let attr = FileAttr {
            ino: 0x11,
            size: 0x22,
            blocks: 0x33,
            atime: time,
            mtime: time,
            ctime: time,
            crtime: time,
            kind: FileType::RegularFile,
            perm: 0o644,
            nlink: 0x55,
            uid: 0x66,
            gid: 0x77,
            rdev: 0x88,
            flags: 0x99,
        };
        reply.attr(&ttl, &attr);
    }

    #[test]
    #[cfg(target_os = "macos")]
    fn reply_xtimes() {
        let sender = AssertSender {
            expected: vec![
                vec![
                    0x28, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x00,
                    0x00, 0x00, 0x00,
                ],
                vec![
                    0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x12, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x78, 0x56, 0x00, 0x00, 0x78, 0x56, 0x00, 0x00,
                ],
            ],
        };
        let reply: ReplyXTimes = Reply::new(0xdeadbeef, sender);
        let time = UNIX_EPOCH + Duration::new(0x1234, 0x5678);
        reply.xtimes(time, time);
    }

    #[test]
    fn reply_open() {
        let sender = AssertSender {
            expected: vec![
                vec![
                    0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x00,
                    0x00, 0x00, 0x00,
                ],
                vec![
                    0x22, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x33, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00,
                ],
            ],
        };
        let reply: ReplyOpen = Reply::new(0xdeadbeef, sender);
        reply.opened(0x1122, 0x33);
    }

    #[test]
    fn reply_write() {
        let sender = AssertSender {
            expected: vec![
                vec![
                    0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x00,
                    0x00, 0x00, 0x00,
                ],
                vec![0x22, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
            ],
        };
        let reply: ReplyWrite = Reply::new(0xdeadbeef, sender);
        reply.written(0x1122);
    }

    #[test]
    fn reply_statfs() {
        let sender = AssertSender {
            expected: vec![
                vec![
                    0x60, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x00,
                    0x00, 0x00, 0x00,
                ],
                vec![
                    0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x22, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x33, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x44, 0x00,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x55, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x66, 0x00, 0x00, 0x00, 0x77, 0x00, 0x00, 0x00, 0x88, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00,
                ],
            ],
        };
        let reply: ReplyStatfs = Reply::new(0xdeadbeef, sender);
        reply.statfs(&ReplyStatfsParam {
            blocks: 0x11,
            bfree: 0x22,
            bavail: 0x33,
            files: 0x44,
            ffree: 0x55,
            bsize: 0x66,
            namelen: 0x77,
            frsize: 0x88,
        });
    }

    #[test]
    fn reply_create() {
        let sender = AssertSender {
            expected: if cfg!(target_os = "macos") {
                vec![
                    vec![
                        0xa8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde,
                        0x00, 0x00, 0x00, 0x00,
                    ],
                    vec![
                        0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xaa, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x65, 0x87, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x65, 0x87, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x21, 0x43, 0x00, 0x00,
                        0x21, 0x43, 0x00, 0x00, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x22, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x33, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x12, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x78, 0x56, 0x00, 0x00, 0x78, 0x56, 0x00, 0x00, 0x78, 0x56, 0x00, 0x00,
                        0x78, 0x56, 0x00, 0x00, 0xa4, 0x81, 0x00, 0x00, 0x55, 0x00, 0x00, 0x00,
                        0x66, 0x00, 0x00, 0x00, 0x77, 0x00, 0x00, 0x00, 0x88, 0x00, 0x00, 0x00,
                        0x99, 0x00, 0x00, 0x00, 0xbb, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0xcc, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    ],
                ]
            } else {
                vec![
                    vec![
                        0x98, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde,
                        0x00, 0x00, 0x00, 0x00,
                    ],
                    vec![
                        0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xaa, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x65, 0x87, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x65, 0x87, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x21, 0x43, 0x00, 0x00,
                        0x21, 0x43, 0x00, 0x00, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x22, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x33, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x12, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x78, 0x56, 0x00, 0x00, 0x78, 0x56, 0x00, 0x00,
                        0x78, 0x56, 0x00, 0x00, 0xa4, 0x81, 0x00, 0x00, 0x55, 0x00, 0x00, 0x00,
                        0x66, 0x00, 0x00, 0x00, 0x77, 0x00, 0x00, 0x00, 0x88, 0x00, 0x00, 0x00,
                        0xbb, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xcc, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00,
                    ],
                ]
            },
        };
        let reply: ReplyCreate = Reply::new(0xdeadbeef, sender);
        let time = UNIX_EPOCH + Duration::new(0x1234, 0x5678);
        let ttl = Duration::new(0x8765, 0x4321);
        let attr = FileAttr {
            ino: 0x11,
            size: 0x22,
            blocks: 0x33,
            atime: time,
            mtime: time,
            ctime: time,
            crtime: time,
            kind: FileType::RegularFile,
            perm: 0o644,
            nlink: 0x55,
            uid: 0x66,
            gid: 0x77,
            rdev: 0x88,
            flags: 0x99,
        };
        reply.created(&ttl, &attr, 0xaa, 0xbb, 0xcc);
    }

    #[test]
    fn reply_lock() {
        let sender = AssertSender {
            expected: vec![
                vec![
                    0x28, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x00,
                    0x00, 0x00, 0x00,
                ],
                vec![
                    0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x22, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x33, 0x00, 0x00, 0x00, 0x44, 0x00, 0x00, 0x00,
                ],
            ],
        };
        let reply: ReplyLock = Reply::new(0xdeadbeef, sender);
        reply.locked(0x11, 0x22, 0x33, 0x44);
    }

    #[test]
    fn reply_bmap() {
        let sender = AssertSender {
            expected: vec![
                vec![
                    0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x00,
                    0x00, 0x00, 0x00,
                ],
                vec![0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
            ],
        };
        let reply: ReplyBmap = Reply::new(0xdeadbeef, sender);
        reply.bmap(0x1234);
    }

    #[test]
    fn reply_directory() {
        let sender = AssertSender {
            expected: vec![
                vec![
                    0x50, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x00,
                    0x00, 0x00, 0x00,
                ],
                vec![
                    0xbb, 0xaa, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x68, 0x65,
                    0x6c, 0x6c, 0x6f, 0x00, 0x00, 0x00, 0xdd, 0xcc, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00,
                    0x08, 0x00, 0x00, 0x00, 0x77, 0x6f, 0x72, 0x6c, 0x64, 0x2e, 0x72, 0x73,
                ],
            ],
        };
        let mut reply = ReplyDirectory::new(0xdeadbeef, sender, 4096);
        reply.add(0xaabb, 1, FileType::Directory, "hello");
        reply.add(0xccdd, 2, FileType::RegularFile, "world.rs");
        reply.ok();
    }

    impl super::ReplySender for Sender<()> {
        fn send(&self, _: &[&[u8]]) {
            Sender::send(self, ()).unwrap()
        }
    }

    #[test]
    fn reply_xattr_size() {
        let sender = AssertSender {
            expected: vec![
                vec![
                    0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xEF, 0xBE, 0xAD, 0xDE, 0x00,
                    0x00, 0x00, 0x00,
                ],
                vec![0x78, 0x56, 0x34, 0x12, 0x00, 0x00, 0x00, 0x00],
            ],
        };
        let reply = ReplyXattr::new(0xdeadbeef, sender);
        reply.size(0x12345678);
    }

    #[test]
    fn reply_xattr_data() {
        let sender = AssertSender {
            expected: vec![
                vec![
                    0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xEF, 0xBE, 0xAD, 0xDE, 0x00,
                    0x00, 0x00, 0x00,
                ],
                vec![0x11, 0x22, 0x33, 0x44],
            ],
        };
        let reply = ReplyXattr::new(0xdeadbeef, sender);
        reply.data(&[0x11, 0x22, 0x33, 0x44]);
    }

    #[test]
    fn async_reply() {
        let (tx, rx) = channel::<()>();
        let reply: ReplyEmpty = Reply::new(0xdeadbeef, tx);
        thread::spawn(move || {
            reply.ok();
        });
        rx.recv().unwrap();
    }
}
