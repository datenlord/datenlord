//! FUSE kernel interface.
//!
//! Support FUSE ABI version from 7.8 up to 7.31.
//! Compatible with OSXFUSE ABI version from 7.8 up to 7.19.
//! https://github.com/libfuse/libfuse/blob/master/include/fuse_kernel.h

// Version number of this interface
pub const FUSE_KERNEL_VERSION: u32 = 7;
// Minor version number of this interface
#[cfg(not(feature = "abi-7-9"))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 8;
#[cfg(all(feature = "abi-7-9", not(feature = "abi-7-10")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 9;
#[cfg(all(feature = "abi-7-10", not(feature = "abi-7-11")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 10;
#[cfg(all(feature = "abi-7-11", not(feature = "abi-7-12")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 11;
#[cfg(all(feature = "abi-7-12", not(feature = "abi-7-13")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 12;
#[cfg(all(feature = "abi-7-13", not(feature = "abi-7-14")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 13;
#[cfg(all(feature = "abi-7-14", not(feature = "abi-7-15")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 14;
#[cfg(all(feature = "abi-7-15", not(feature = "abi-7-16")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 15;
#[cfg(all(feature = "abi-7-16", not(feature = "abi-7-17")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 16;
#[cfg(all(feature = "abi-7-17", not(feature = "abi-7-18")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 17;
#[cfg(all(feature = "abi-7-18", not(feature = "abi-7-19")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 18;
#[cfg(all(feature = "abi-7-19", not(feature = "abi-7-20")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 19;
#[cfg(all(feature = "abi-7-20", not(feature = "abi-7-21")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 20;
#[cfg(all(feature = "abi-7-21", not(feature = "abi-7-22")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 21;
#[cfg(all(feature = "abi-7-22", not(feature = "abi-7-23")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 22;
#[cfg(all(feature = "abi-7-23", not(feature = "abi-7-24")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 23;
#[cfg(all(feature = "abi-7-24", not(feature = "abi-7-25")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 24;
#[cfg(all(feature = "abi-7-25", not(feature = "abi-7-26")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 25;
#[cfg(all(feature = "abi-7-26", not(feature = "abi-7-27")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 26;
#[cfg(all(feature = "abi-7-27", not(feature = "abi-7-28")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 27;
#[cfg(all(feature = "abi-7-28", not(feature = "abi-7-29")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 28;
#[cfg(all(feature = "abi-7-29", not(feature = "abi-7-30")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 29;
#[cfg(all(feature = "abi-7-30", not(feature = "abi-7-31")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 30;
#[cfg(all(feature = "abi-7-31", not(feature = "abi-7-32")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 31;
// The node ID of the root inode
pub const FUSE_ROOT_ID: u64 = 1;

pub type INum = u64;

#[repr(C)]
#[derive(Debug)]
pub struct FuseAttr {
    // fuse_attr
    pub ino: INum,
    pub size: u64,
    pub blocks: u64,
    pub atime: u64,
    pub mtime: u64,
    pub ctime: u64,
    #[cfg(target_os = "macos")]
    pub crtime: u64,
    pub atimensec: u32,
    pub mtimensec: u32,
    pub ctimensec: u32,
    #[cfg(target_os = "macos")]
    pub crtimensec: u32,
    pub mode: u32,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
    #[cfg(target_os = "macos")]
    pub flags: u32, // see chflags(2)
    #[cfg(feature = "abi-7-9")]
    pub blksize: u32,
    #[cfg(feature = "abi-7-9")]
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug)]
pub struct FuseKStatFs {
    // fuse_kstatfs
    pub blocks: u64,  // Total blocks (in units of frsize)
    pub bfree: u64,   // Free blocks
    pub bavail: u64,  // Free blocks for unprivileged users
    pub files: u64,   // Total inodes
    pub ffree: u64,   // Free inodes
    pub bsize: u32,   // Filesystem block size
    pub namelen: u32, // Maximum filename length
    pub frsize: u32,  // Fundamental file system block size
    pub padding: u32,
    pub spare: [u32; 6],
}

#[repr(C)]
#[derive(Debug)]
pub struct FuseFileLock {
    // fuse_file_lock
    pub start: u64,
    pub end: u64,
    pub typ: u32,
    pub pid: u32, // tgid
}

// Bitmasks for fuse_setattr_in.valid
pub const FATTR_MODE: u32 = 1 << 0;
pub const FATTR_UID: u32 = 1 << 1;
pub const FATTR_GID: u32 = 1 << 2;
pub const FATTR_SIZE: u32 = 1 << 3;
pub const FATTR_ATIME: u32 = 1 << 4;
pub const FATTR_MTIME: u32 = 1 << 5;
pub const FATTR_FH: u32 = 1 << 6;
#[cfg(feature = "abi-7-9")]
pub const FATTR_ATIME_NOW: u32 = 1 << 7;
#[cfg(feature = "abi-7-9")]
pub const FATTR_MTIME_NOW: u32 = 1 << 8;
#[cfg(feature = "abi-7-9")]
pub const FATTR_LOCKOWNER: u32 = 1 << 9;
#[cfg(feature = "abi-7-23")]
pub const FATTR_CTIME: u32 = 1 << 10;
#[cfg(target_os = "macos")]
pub const FATTR_CRTIME: u32 = 1 << 28;
#[cfg(target_os = "macos")]
pub const FATTR_CHGTIME: u32 = 1 << 29;
#[cfg(target_os = "macos")]
pub const FATTR_BKUPTIME: u32 = 1 << 30;
#[cfg(target_os = "macos")]
pub const FATTR_FLAGS: u32 = 1 << 31;

// Flags returned by the OPEN request
//
// FOPEN_DIRECT_IO: bypass page cache for this open file
// FOPEN_KEEP_CACHE: don't invalidate the data cache on open
// FOPEN_NONSEEKABLE: the file is not seekable
// FOPEN_CACHE_DIR: allow caching this directory
// FOPEN_STREAM: the file is stream-like (no file position at all)
//
pub const FOPEN_DIRECT_IO: u32 = 1 << 0;
pub const FOPEN_KEEP_CACHE: u32 = 1 << 1;
#[cfg(feature = "abi-7-10")]
pub const FOPEN_NONSEEKABLE: u32 = 1 << 2;
#[cfg(feature = "abi-7-28")]
pub const FOPEN_CACHE_DIR: u32 = 1 << 3;
#[cfg(feature = "abi-7-31")]
pub const FOPEN_STREAM: u32 = 1 << 4;
#[cfg(target_os = "macos")]
pub const FOPEN_PURGE_ATTR: u32 = 1 << 30;
#[cfg(target_os = "macos")]
pub const FOPEN_PURGE_UBC: u32 = 1 << 31;

// INIT request/reply flags
//
// FUSE_ASYNC_READ: asynchronous read requests
// FUSE_POSIX_LOCKS: remote locking for POSIX file locks
// FUSE_FILE_OPS: kernel sends file handle for fstat, etc... (not yet supported)
// FUSE_ATOMIC_O_TRUNC: handles the O_TRUNC open flag in the filesystem
// FUSE_EXPORT_SUPPORT: filesystem handles lookups of "." and ".."
// FUSE_BIG_WRITES: filesystem can handle write size larger than 4kB
// FUSE_DONT_MASK: don't apply umask to file mode on create operations
// FUSE_SPLICE_WRITE: kernel supports splice write on the device
// FUSE_SPLICE_MOVE: kernel supports splice move on the device
// FUSE_SPLICE_READ: kernel supports splice read on the device
// FUSE_FLOCK_LOCKS: remote locking for BSD style file locks
// FUSE_HAS_IOCTL_DIR: kernel supports ioctl on directories
// FUSE_AUTO_INVAL_DATA: automatically invalidate cached pages
// FUSE_DO_READDIRPLUS: do READDIRPLUS (READDIR+LOOKUP in one)
// FUSE_READDIRPLUS_AUTO: adaptive readdirplus
// FUSE_ASYNC_DIO: asynchronous direct I/O submission
// FUSE_WRITEBACK_CACHE: use writeback cache for buffered writes
// FUSE_NO_OPEN_SUPPORT: kernel supports zero-message opens
// FUSE_PARALLEL_DIROPS: allow parallel lookups and readdir
// FUSE_HANDLE_KILLPRIV: fs handles killing suid/sgid/cap on write/chown/trunc
// FUSE_POSIX_ACL: filesystem supports posix acls
// FUSE_ABORT_ERROR: reading the device after abort returns ECONNABORTED
// FUSE_MAX_PAGES: init_out.max_pages contains the max number of req pages
// FUSE_CACHE_SYMLINKS: cache READLINK responses
// FUSE_NO_OPENDIR_SUPPORT: kernel supports zero-message opendir
// FUSE_EXPLICIT_INVAL_DATA: only invalidate cached pages on explicit request
//
pub const FUSE_ASYNC_READ: u32 = 1 << 0;
pub const FUSE_POSIX_LOCKS: u32 = 1 << 1;
#[cfg(feature = "abi-7-9")]
pub const FUSE_FILE_OPS: u32 = 1 << 2;
#[cfg(feature = "abi-7-9")]
pub const FUSE_ATOMIC_O_TRUNC: u32 = 1 << 3;
#[cfg(feature = "abi-7-10")]
pub const FUSE_EXPORT_SUPPORT: u32 = 1 << 4;
#[cfg(feature = "abi-7-9")]
pub const FUSE_BIG_WRITES: u32 = 1 << 5;
#[cfg(feature = "abi-7-12")]
pub const FUSE_DONT_MASK: u32 = 1 << 6;
#[cfg(feature = "abi-7-14")]
pub const FUSE_SPLICE_WRITE: u32 = 1 << 7;
#[cfg(feature = "abi-7-14")]
pub const FUSE_SPLICE_MOVE: u32 = 1 << 8;
#[cfg(feature = "abi-7-14")]
pub const FUSE_SPLICE_READ: u32 = 1 << 9;
#[cfg(feature = "abi-7-17")]
pub const FUSE_FLOCK_LOCKS: u32 = 1 << 10;

#[cfg(feature = "abi-7-18")]
pub const FUSE_HAS_IOCTL_DIR: u32 = 1 << 11;
#[cfg(feature = "abi-7-20")]
pub const FUSE_AUTO_INVAL_DATA: u32 = 1 << 12;
#[cfg(feature = "abi-7-21")]
pub const FUSE_DO_READDIRPLUS: u32 = 1 << 13;

// TODO: verify it's added in 7.21
#[cfg(feature = "abi-7-21")]
pub const FUSE_READDIRPLUS_AUTO: u32 = 1 << 14;
#[cfg(feature = "abi-7-22")]
pub const FUSE_ASYNC_DIO: u32 = 1 << 15;
#[cfg(feature = "abi-7-23")]
pub const FUSE_WRITEBACK_CACHE: u32 = 1 << 16;
#[cfg(feature = "abi-7-23")]
pub const FUSE_NO_OPEN_SUPPORT: u32 = 1 << 17;
#[cfg(feature = "abi-7-25")]
pub const FUSE_PARALLEL_DIROPS: u32 = 1 << 18;
#[cfg(feature = "abi-7-26")]
pub const FUSE_HANDLE_KILLPRIV: u32 = 1 << 19;
#[cfg(feature = "abi-7-26")]
pub const FUSE_POSIX_ACL: u32 = 1 << 20;
#[cfg(feature = "abi-7-27")]
pub const FUSE_ABORT_ERROR: u32 = 1 << 21;
#[cfg(feature = "abi-7-28")]
pub const FUSE_MAX_PAGES: u32 = 1 << 22;
#[cfg(feature = "abi-7-28")]
pub const FUSE_CACHE_SYMLINKS: u32 = 1 << 23;
#[cfg(feature = "abi-7-29")]
pub const FUSE_NO_OPENDIR_SUPPORT: u32 = 1 << 24;
#[cfg(feature = "abi-7-30")]
pub const FUSE_EXPLICIT_INVAL_DATA: u32 = 1 << 25;

#[cfg(target_os = "macos")]
pub const FUSE_ALLOCATE: u32 = 1 << 27;
#[cfg(target_os = "macos")]
pub const FUSE_EXCHANGE_DATA: u32 = 1 << 28;
#[cfg(target_os = "macos")]
pub const FUSE_CASE_INSENSITIVE: u32 = 1 << 29;
#[cfg(target_os = "macos")]
pub const FUSE_VOL_RENAME: u32 = 1 << 30;
#[cfg(target_os = "macos")]
pub const FUSE_XTIMES: u32 = 1 << 31;

// CUSE INIT request/reply flags
//
// CUSE_UNRESTRICTED_IOCTL:  use unrestricted ioctl
#[cfg(feature = "abi-7-11")]
pub const CUSE_UNRESTRICTED_IOCTL: u32 = 1 << 0; // use unrestricted ioctl

// Release flags
pub const FUSE_RELEASE_FLUSH: u32 = 1 << 0;
#[cfg(feature = "abi-7-17")]
pub const FUSE_RELEASE_FLOCK_UNLOCK: u32 = 1 << 1;

// Getattr flags
#[cfg(feature = "abi-7-9")]
pub const FUSE_GETATTR_FH: u32 = 1 << 0;

// Lock flags
#[cfg(feature = "abi-7-9")]
pub const FUSE_LK_FLOCK: u32 = 1 << 0;

// WRITE flags
//
// FUSE_WRITE_CACHE: delayed write from page cache, file handle is guessed
// FUSE_WRITE_LOCKOWNER: lock_owner field is valid
// FUSE_WRITE_KILL_PRIV: kill suid and sgid bits
//
#[cfg(feature = "abi-7-9")]
pub const FUSE_WRITE_CACHE: u32 = 1 << 0;
#[cfg(feature = "abi-7-9")]
pub const FUSE_WRITE_LOCKOWNER: u32 = 1 << 1;
#[cfg(feature = "abi-7-31")]
pub const FUSE_WRITE_KILL_PRIV: u32 = 1 << 2;

// Read flags
#[cfg(feature = "abi-7-9")]
pub const FUSE_READ_LOCKOWNER: u32 = 1 << 1;

// Ioctl flags
//
// FUSE_IOCTL_COMPAT: 32bit compat ioctl on 64bit machine
// FUSE_IOCTL_UNRESTRICTED: not restricted to well-formed ioctls, retry allowed
// FUSE_IOCTL_RETRY: retry with new iovecs
// FUSE_IOCTL_32BIT: 32bit ioctl
// FUSE_IOCTL_DIR: is a directory
// FUSE_IOCTL_COMPAT_X32: x32 compat ioctl on 64bit machine (64bit time_t)
//
// FUSE_IOCTL_MAX_IOV: maximum of in_iovecs + out_iovecs
//
#[cfg(feature = "abi-7-11")]
pub const FUSE_IOCTL_COMPAT: u32 = 1 << 0;
#[cfg(feature = "abi-7-11")]
pub const FUSE_IOCTL_UNRESTRICTED: u32 = 1 << 1;
#[cfg(feature = "abi-7-11")]
pub const FUSE_IOCTL_RETRY: u32 = 1 << 2;
#[cfg(feature = "abi-7-16")]
pub const FUSE_IOCTL_32BIT: u32 = 1 << 3;
#[cfg(feature = "abi-7-18")]
pub const FUSE_IOCTL_DIR: u32 = 1 << 4;
#[cfg(feature = "abi-7-30")]
pub const FUSE_IOCTL_COMPAT_X32: u32 = 1 << 5;

#[cfg(feature = "abi-7-11")]
pub const FUSE_IOCTL_MAX_IOV: u32 = 256;

// Poll flags
//
// FUSE_POLL_SCHEDULE_NOTIFY: request poll notify
#[cfg(feature = "abi-7-11")]
pub const FUSE_POLL_SCHEDULE_NOTIFY: u32 = 1 << 0;

// Fsync flags
//
// FUSE_FSYNC_FDATASYNC: Sync data only, not metadata
#[cfg(feature = "abi-7-31")]
pub const FUSE_FSYNC_FDATASYNC: u32 = 1 << 0;

#[repr(C)]
#[derive(Debug)]
#[allow(non_camel_case_types)]
#[non_exhaustive]
pub enum FuseOpCode {
    // fuse_opcode
    FUSE_LOOKUP = 1,
    FUSE_FORGET = 2, // no reply
    FUSE_GETATTR = 3,
    FUSE_SETATTR = 4,
    FUSE_READLINK = 5,
    FUSE_SYMLINK = 6,
    FUSE_MKNOD = 8,
    FUSE_MKDIR = 9,
    FUSE_UNLINK = 10,
    FUSE_RMDIR = 11,
    FUSE_RENAME = 12,
    FUSE_LINK = 13,
    FUSE_OPEN = 14,
    FUSE_READ = 15,
    FUSE_WRITE = 16,
    FUSE_STATFS = 17,
    FUSE_RELEASE = 18,
    FUSE_FSYNC = 20,
    FUSE_SETXATTR = 21,
    FUSE_GETXATTR = 22,
    FUSE_LISTXATTR = 23,
    FUSE_REMOVEXATTR = 24,
    FUSE_FLUSH = 25,
    FUSE_INIT = 26,
    FUSE_OPENDIR = 27,
    FUSE_READDIR = 28,
    FUSE_RELEASEDIR = 29,
    FUSE_FSYNCDIR = 30,
    FUSE_GETLK = 31,
    FUSE_SETLK = 32,
    FUSE_SETLKW = 33,
    FUSE_ACCESS = 34,
    FUSE_CREATE = 35,
    FUSE_INTERRUPT = 36,
    FUSE_BMAP = 37,
    FUSE_DESTROY = 38,
    #[cfg(feature = "abi-7-11")]
    FUSE_IOCTL = 39,
    #[cfg(feature = "abi-7-11")]
    FUSE_POLL = 40,
    #[cfg(feature = "abi-7-15")]
    FUSE_NOTIFY_REPLY = 41,
    #[cfg(feature = "abi-7-16")]
    FUSE_BATCH_FORGET = 42,
    #[cfg(feature = "abi-7-19")]
    FUSE_FALLOCATE = 43,
    #[cfg(feature = "abi-7-21")]
    FUSE_READDIRPLUS = 44,
    #[cfg(feature = "abi-7-23")]
    FUSE_RENAME2 = 45,
    #[cfg(feature = "abi-7-24")]
    FUSE_LSEEK = 46,
    #[cfg(feature = "abi-7-28")]
    FUSE_COPY_FILE_RANGE = 47,

    #[cfg(target_os = "macos")]
    FUSE_SETVOLNAME = 61,
    #[cfg(target_os = "macos")]
    FUSE_GETXTIMES = 62,
    #[cfg(target_os = "macos")]
    FUSE_EXCHANGE = 63,

    // CUSE specific operations
    #[cfg(feature = "abi-7-11")]
    CUSE_INIT = 4096,
}

#[cfg(feature = "abi-7-11")]
#[repr(C)]
#[derive(Debug)]
pub enum FuseNotifyCode {
    // fuse_notify_code
    FUSE_POLL = 1,
    #[cfg(feature = "abi-7-12")]
    FUSE_NOTIFY_INVAL_INODE = 2,
    #[cfg(feature = "abi-7-12")]
    FUSE_NOTIFY_INVAL_ENTRY = 3,
    #[cfg(feature = "abi-7-15")]
    FUSE_NOTIFY_STORE = 4,
    #[cfg(feature = "abi-7-15")]
    FUSE_NOTIFY_RETRIEVE = 5,
    #[cfg(feature = "abi-7-18")]
    FUSE_NOTIFY_DELETE = 6,
    FUSE_NOTIFY_CODE_MAX,
}

// The read buffer is required to be at least 8k, but may be much larger
pub const FUSE_MIN_READ_BUFFER: usize = 8192;

#[cfg(all(target_os = "macos", feature = "abi-7-9"))]
pub const FUSE_COMPAT_ENTRY_OUT_SIZE: usize = 136;
#[cfg(feature = "abi-7-9")]
pub const FUSE_COMPAT_ENTRY_OUT_SIZE: usize = 120;

#[repr(C)]
#[derive(Debug)]
pub struct FuseEntryOut {
    // fuse_entry_out
    pub nodeid: u64,     // Inode ID
    pub generation: u64, // Inode generation: nodeid:gen must
    // be unique for the fs's lifetime
    pub entry_valid: u64, // Cache timeout for the name
    pub attr_valid: u64,  // Cache timeout for the attributes
    pub entry_valid_nsec: u32,
    pub attr_valid_nsec: u32,
    pub attr: FuseAttr,
}

#[repr(C)]
#[derive(Debug)]
pub struct FuseForgetIn {
    // fuse_forget_in
    pub nlookup: u64,
}

#[cfg(feature = "abi-7-16")]
#[repr(C)]
#[derive(Debug)]
pub struct FuseForgetOne {
    // fuse_forget_one
    pub nodeid: u64,
    pub nlookup: u64,
}

#[cfg(feature = "abi-7-16")]
#[repr(C)]
#[derive(Debug)]
pub struct FuseBatchForgetIn {
    // fuse_batch_forget_in
    pub count: u32,
    pub dummy: u32,
}

#[cfg(feature = "abi-7-9")]
#[repr(C)]
#[derive(Debug)]
pub struct FuseGetAttrIn {
    // fuse_getattr_in
    pub getattr_flags: u32,
    pub dummy: u32,
    pub fh: u64,
}

#[cfg(all(target_os = "macos", feature = "abi-7-9"))]
pub const FUSE_COMPAT_ATTR_OUT_SIZE: usize = 112;
#[cfg(feature = "abi-7-9")]
pub const FUSE_COMPAT_ATTR_OUT_SIZE: usize = 96;

#[repr(C)]
#[derive(Debug)]
pub struct FuseAttrOut {
    // fuse_attr_out
    pub attr_valid: u64,
    pub attr_valid_nsec: u32,
    pub dummy: u32,
    pub attr: FuseAttr,
}

#[cfg(target_os = "macos")]
#[repr(C)]
#[derive(Debug)]
pub struct FuseGetXTimesOut {
    // fuse_getxtimes_out
    pub bkuptime: u64,
    pub crtime: u64,
    pub bkuptimensec: u32,
    pub crtimensec: u32,
}

#[cfg(feature = "abi-7-12")]
pub const FUSE_COMPAT_MKNOD_IN_SIZE: usize = 8;

#[repr(C)]
#[derive(Debug)]
pub struct FuseMkNodIn {
    // fuse_mknod_in
    pub mode: u32,
    pub rdev: u32,
    #[cfg(feature = "abi-7-12")]
    pub umask: u32,
    #[cfg(feature = "abi-7-12")]
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug)]
pub struct FuseMkDirIn {
    // fuse_mkdir_in
    pub mode: u32,
    #[cfg(not(feature = "abi-7-12"))]
    pub padding: u32,
    #[cfg(feature = "abi-7-12")]
    pub umask: u32,
}

#[repr(C)]
#[derive(Debug)]
pub struct FuseRenameIn {
    // fuse_rename_in
    pub newdir: u64,
}

#[cfg(feature = "abi-7-23")]
#[repr(C)]
#[derive(Debug)]
struct FuseRename2In {
    // fuse_rename2_in
    pub newdir: u64,
    pub flags: u32,
    pub padding: u32,
}

#[cfg(target_os = "macos")]
#[repr(C)]
#[derive(Debug)]
pub struct FuseExchangeIn {
    // fuse_exchange_in
    pub olddir: u64,
    pub newdir: u64,
    pub options: u64,
}

#[repr(C)]
#[derive(Debug)]
pub struct FuseLinkIn {
    // fuse_link_in
    pub oldnodeid: u64,
}

#[repr(C)]
#[derive(Debug)]
pub struct FuseSetAttrIn {
    // fuse_setattr_in
    pub valid: u32,
    pub padding: u32,
    pub fh: u64,
    pub size: u64,
    #[cfg(not(feature = "abi-7-9"))]
    pub unused1: u64,
    #[cfg(feature = "abi-7-9")]
    pub lock_owner: u64,
    pub atime: u64,
    pub mtime: u64,
    #[cfg(not(feature = "abi-7-23"))]
    pub unused2: u64,
    #[cfg(feature = "abi-7-23")]
    pub ctime: u64,
    pub atimensec: u32,
    pub mtimensec: u32,
    #[cfg(not(feature = "abi-7-23"))]
    pub unused3: u32,
    #[cfg(feature = "abi-7-23")]
    pub ctimensec: u32,
    pub mode: u32,
    pub unused4: u32,
    pub uid: u32,
    pub gid: u32,
    pub unused5: u32,
    #[cfg(target_os = "macos")]
    pub bkuptime: u64,
    #[cfg(target_os = "macos")]
    pub chgtime: u64,
    #[cfg(target_os = "macos")]
    pub crtime: u64,
    #[cfg(target_os = "macos")]
    pub bkuptimensec: u32,
    #[cfg(target_os = "macos")]
    pub chgtimensec: u32,
    #[cfg(target_os = "macos")]
    pub crtimensec: u32,
    #[cfg(target_os = "macos")]
    pub flags: u32, // see chflags(2)
}

#[repr(C)]
#[derive(Debug)]
pub struct FuseOpenIn {
    // fuse_open_in
    pub flags: u32,
    pub unused: u32,
}

#[repr(C)]
#[derive(Debug)]
pub struct FuseCreateIn {
    // fuse_create_in
    pub flags: u32,
    pub mode: u32,
    #[cfg(feature = "abi-7-12")]
    pub umask: u32,
    #[cfg(feature = "abi-7-12")]
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug)]
pub struct FuseOpenOut {
    // fuse_open_out
    pub fh: u64,
    pub open_flags: u32,
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug)]
pub struct FuseReleaseIn {
    // fuse_release_in
    pub fh: u64,
    pub flags: u32,
    pub release_flags: u32,
    pub lock_owner: u64,
}

#[repr(C)]
#[derive(Debug)]
pub struct FuseFlushIn {
    // fuse_flush_in
    pub fh: u64,
    pub unused: u32,
    pub padding: u32,
    pub lock_owner: u64,
}

#[repr(C)]
#[derive(Debug)]
pub struct FuseReadIn {
    // fuse_read_in
    pub fh: u64,
    pub offset: u64,
    pub size: u32,
    #[cfg(feature = "abi-7-9")]
    pub read_flags: u32,
    #[cfg(feature = "abi-7-9")]
    pub lock_owner: u64,
    #[cfg(feature = "abi-7-9")]
    pub flags: u32,
    pub padding: u32,
}

#[cfg(feature = "abi-7-9")]
pub const FUSE_COMPAT_WRITE_IN_SIZE: usize = 24;

#[repr(C)]
#[derive(Debug)]
pub struct FuseWriteIn {
    // fuse_write_in
    pub fh: u64,
    pub offset: u64,
    pub size: u32,
    pub write_flags: u32,
    #[cfg(feature = "abi-7-9")]
    pub lock_owner: u64,
    #[cfg(feature = "abi-7-9")]
    pub flags: u32,
    #[cfg(feature = "abi-7-9")]
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug)]
pub struct FuseWriteOut {
    // fuse_write_out
    pub size: u32,
    pub padding: u32,
}

pub const FUSE_COMPAT_STATFS_SIZE: usize = 48;

#[repr(C)]
#[derive(Debug)]
pub struct FuseStatFsOut {
    // fuse_statfs_out
    pub st: FuseKStatFs,
}

#[repr(C)]
#[derive(Debug)]
pub struct FuseFSyncIn {
    // fuse_fsync_in
    pub fh: u64,
    pub fsync_flags: u32,
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug)]
pub struct FuseSetXAttrIn {
    // fuse_setxattr_in
    pub size: u32,
    pub flags: u32,
    #[cfg(target_os = "macos")]
    pub position: u32,
    #[cfg(target_os = "macos")]
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug)]
pub struct FuseGetXAttrIn {
    // fuse_getxattr_in
    pub size: u32,
    pub padding: u32,
    #[cfg(target_os = "macos")]
    pub position: u32,
    #[cfg(target_os = "macos")]
    pub padding2: u32,
}

#[repr(C)]
#[derive(Debug)]
pub struct FuseGetXAttrOut {
    // fuse_getxattr_out
    pub size: u32,
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug)]
pub struct FuseLockIn {
    // fuse_lk_in
    pub fh: u64,
    pub owner: u64,
    pub lk: FuseFileLock,
    #[cfg(feature = "abi-7-9")]
    pub lk_flags: u32,
    #[cfg(feature = "abi-7-9")]
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug)]
pub struct FuseLockOut {
    //fuse_lk_out
    pub lk: FuseFileLock,
}

#[repr(C)]
#[derive(Debug)]
pub struct FuseAccessIn {
    // fuse_access_in
    pub mask: u32,
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug)]
pub struct FuseInitIn {
    // fuse_init_in
    pub major: u32,
    pub minor: u32,
    pub max_readahead: u32,
    pub flags: u32,
}

#[cfg(feature = "abi-7-23")]
pub const FUSE_COMPAT_INIT_OUT_SIZE: usize = 8;
#[cfg(feature = "abi-7-23")]
pub const FUSE_COMPAT_22_INIT_OUT_SIZE: usize = 24;

#[repr(C)]
#[derive(Debug)]
pub struct FuseInitOut {
    // fuse_init_out
    pub major: u32,
    pub minor: u32,
    pub max_readahead: u32,
    pub flags: u32,
    #[cfg(not(feature = "abi-7-13"))]
    pub unused: u32,
    #[cfg(feature = "abi-7-13")]
    pub max_background: u16,
    #[cfg(feature = "abi-7-13")]
    pub congestion_threshold: u16,
    pub max_write: u32,
    #[cfg(feature = "abi-7-23")]
    pub time_gran: u32,
    // unused: [u32; 9] is defined between 7-13 and 7-27
    // TODO: verify cfg works
    #[cfg(all(feature = "abi-7-23", not(feature = "abi-7-28")))]
    pub unused: [u32; 9],
    #[cfg(feature = "abi-7-28")]
    pub max_pages: u16,
    #[cfg(feature = "abi-7-28")]
    pub padding: u16,
    #[cfg(feature = "abi-7-28")]
    pub unused: [u32; 8],
}

#[cfg(feature = "abi-7-11")]
pub const CUSE_INIT_INFO_MAX: u32 = 4096;

#[cfg(feature = "abi-7-11")]
#[repr(C)]
#[derive(Debug)]
pub struct CuseInitIn {
    // cuse_init_in
    pub major: u32,
    pub minor: u32,
    pub unused: u32,
    pub flags: u32,
}

#[cfg(feature = "abi-7-11")]
#[repr(C)]
#[derive(Debug)]
pub struct CuseInitOut {
    // cuse_init_out
    pub major: u32,
    pub minor: u32,
    pub unused: u32,
    pub flags: u32,
    pub max_read: u32,
    pub max_write: u32,
    pub dev_major: u32, // chardev major
    pub dev_minor: u32, // chardev minor
    pub spare: [u32; 10],
}

#[repr(C)]
#[derive(Debug)]
pub struct FuseInterruptIn {
    // fuse_interrupt_in
    pub unique: u64,
}

#[repr(C)]
#[derive(Debug)]
pub struct FuseBMapIn {
    // fuse_bmap_in
    pub block: u64,
    pub blocksize: u32,
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug)]
pub struct FuseBMapOut {
    // fuse_bmap_out
    pub block: u64,
}

#[cfg(feature = "abi-7-11")]
#[repr(C)]
#[derive(Debug)]
pub struct FuseIoCtlIn {
    // fuse_ioctl_in
    pub fh: u64,
    pub flags: u32,
    pub cmd: u32,
    pub arg: u64,
    pub in_size: u32,
    pub out_size: u32,
}

#[cfg(feature = "abi-7-16")]
#[repr(C)]
#[derive(Debug)]
pub struct FuseIoCtlIoVec {
    // fuse_ioctl_iovec
    pub base: u64,
    pub len: u64,
}

#[cfg(feature = "abi-7-11")]
#[repr(C)]
#[derive(Debug)]
pub struct FuseIoCtlOut {
    // fuse_ioctl_out
    pub result: i32,
    pub flags: u32,
    pub in_iovs: u32,
    pub out_iovs: u32,
}

#[cfg(feature = "abi-7-11")]
#[repr(C)]
#[derive(Debug)]
pub struct FusePollIn {
    // fuse_poll_in
    pub fh: u64,
    pub kh: u64,
    pub flags: u32,
    #[cfg(feature = "abi-7-11")]
    pub padding: u32,
    #[cfg(feature = "abi-7-21")]
    pub events: u32,
}

#[cfg(feature = "abi-7-11")]
#[repr(C)]
#[derive(Debug)]
pub struct FusePollOut {
    // fuse_poll_out
    pub revents: u32,
    pub padding: u32,
}

#[cfg(feature = "abi-7-11")]
#[repr(C)]
#[derive(Debug)]
pub struct FuseNotifyPollWakeUpOut {
    // fuse_notify_poll_wakeup_out
    pub kh: u64,
}

#[cfg(feature = "abi-7-19")]
#[repr(C)]
#[derive(Debug)]
pub struct FuseFAllocateIn {
    // fuse_fallocate_in
    fh: u64,
    offset: u64,
    length: u64,
    mode: u32,
    padding: u32,
}

#[repr(C)]
#[derive(Debug)]
pub struct FuseInHeader {
    // fuse_in_header
    pub len: u32,
    pub opcode: u32,
    pub unique: u64,
    pub nodeid: u64,
    pub uid: u32,
    pub gid: u32,
    pub pid: u32,
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug)]
pub struct FuseOutHeader {
    // fuse_out_header
    pub len: u32,
    pub error: i32,
    pub unique: u64,
}

#[repr(C)]
#[derive(Debug)]
pub struct FuseDirEnt {
    // fuse_dirent
    pub ino: INum,
    pub off: u64,
    pub namelen: u32,
    pub typ: u32,
    // followed by name of namelen bytes
    // TODO: char name[],
}

// TODO: re-define it
// #define FUSE_NAME_OFFSET offsetof(struct fuse_dirent, name)
// #define FUSE_DIRENT_ALIGN(x) \
//     (((x) + sizeof(uint64_t) - 1) & ~(sizeof(uint64_t) - 1))
// #define FUSE_DIRENT_SIZE(d) \
//     FUSE_DIRENT_ALIGN(FUSE_NAME_OFFSET + (d)->namelen)

#[cfg(feature = "abi-7-21")]
#[repr(C)]
#[derive(Debug)]
pub struct FuseDirEntPlus {
    // fuse_direntplus
    entry_out: FuseEntryOut,
    dirent: FuseDirEnt,
}

// TODO: re-define it
// #[cfg(feature = "abi-7-21")]
// #define FUSE_NAME_OFFSET_DIRENTPLUS \
//     offsetof(struct fuse_direntplus, dirent.name)
// #[cfg(feature = "abi-7-21")]
// #define FUSE_DIRENTPLUS_SIZE(d) \
//     FUSE_DIRENT_ALIGN(FUSE_NAME_OFFSET_DIRENTPLUS + (d)->dirent.namelen)

#[cfg(feature = "abi-7-12")]
#[repr(C)]
#[derive(Debug)]
pub struct FuseNotifyInvalINodeOut {
    // fuse_notify_inval_inode_out
    pub ino: INum,
    pub off: i64,
    pub len: i64,
}

#[cfg(feature = "abi-7-12")]
#[repr(C)]
#[derive(Debug)]
pub struct FuseNotifyInvalEntryOut {
    // fuse_notify_inval_entry_out
    pub parent: u64,
    pub namelen: u32,
    pub padding: u32,
}

#[cfg(feature = "abi-7-18")]
#[repr(C)]
#[derive(Debug)]
pub struct FuseNotifyDeleteOut {
    // fuse_notify_delete_out
    parent: u64,
    child: u64,
    namelen: u32,
    padding: u32,
}

#[cfg(feature = "abi-7-15")]
#[repr(C)]
#[derive(Debug)]
pub struct FuseNotifyStoreOut {
    // fuse_notify_store_out
    pub nodeid: u64,
    pub offset: u64,
    pub size: u32,
    pub padding: u32,
}

#[cfg(feature = "abi-7-15")]
#[repr(C)]
#[derive(Debug)]
pub struct FuseNotifyRetrieveOut {
    // fuse_notify_retrieve_out
    pub notify_unique: u64,
    pub nodeid: u64,
    pub offset: u64,
    pub size: u32,
    pub padding: u32,
}

#[cfg(feature = "abi-7-15")]
#[repr(C)]
#[derive(Debug)]
pub struct FuseNotifyRetrieveIn {
    // fuse_notify_retrieve_in
    // matches the size of fuse_write_in
    pub dummy1: u64,
    pub offset: u64,
    pub size: u32,
    pub dummy2: u32,
    pub dummy3: u64,
    pub dummy4: u64,
}

// TODO: re-define it
// Device ioctls:
// #define FUSE_DEV_IOC_CLONE    _IOR(229, 0, uint32_t)

#[cfg(feature = "abi-7-24")]
#[repr(C)]
#[derive(Debug)]
struct FuseLSeekIn {
    // fuse_lseek_in
    pub fh: u64,
    pub offset: u64,
    pub whence: u32,
    pub padding: u32,
}

#[cfg(feature = "abi-7-24")]
#[repr(C)]
#[derive(Debug)]
struct FuseLSeekOut {
    // fuse_lseek_out
    pub offset: u64,
}

#[cfg(feature = "abi-7-28")]
#[repr(C)]
#[derive(Debug)]
struct FuseCopyFileRangeIn {
    // fuse_copy_file_range_in
    pub fh_in: u64,
    pub off_in: u64,
    pub nodeid_out: u64,
    pub fh_out: u64,
    pub off_out: u64,
    pub len: u64,
    pub flags: u64,
}
