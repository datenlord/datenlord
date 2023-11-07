//! FUSE kernel interface.
//!
//! Support FUSE ABI version from 7.8 up to 7.31.
//! Compatible with OSXFUSE ABI version from 7.8 up to 7.19.
//! <https://github.com/libfuse/libfuse/blob/master/include/fuse_kernel.h>

/// Version number of this interface
pub const FUSE_KERNEL_VERSION: u32 = 7;
/// FUSE minimum minor version number 7.8
#[cfg(not(feature = "abi-7-9"))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 8;
/// FUSE minor version number 7.9
#[cfg(all(feature = "abi-7-9", not(feature = "abi-7-10")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 9;
/// FUSE minor version number 7.10
#[cfg(all(feature = "abi-7-10", not(feature = "abi-7-11")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 10;
/// FUSE minor version number 7.11
#[cfg(all(feature = "abi-7-11", not(feature = "abi-7-12")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 11;
/// FUSE minor version number 7.12
#[cfg(all(feature = "abi-7-12", not(feature = "abi-7-13")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 12;
/// FUSE minor version number 7.13
#[cfg(all(feature = "abi-7-13", not(feature = "abi-7-14")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 13;
/// FUSE minor version number 7.14
#[cfg(all(feature = "abi-7-14", not(feature = "abi-7-15")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 14;
/// FUSE minor version number 7.15
#[cfg(all(feature = "abi-7-15", not(feature = "abi-7-16")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 15;
/// FUSE minor version number 7.16
#[cfg(all(feature = "abi-7-16", not(feature = "abi-7-17")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 16;
/// FUSE minor version number 7.17
#[cfg(all(feature = "abi-7-17", not(feature = "abi-7-18")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 17;
/// FUSE minor version number 7.18
#[cfg(all(feature = "abi-7-18", not(feature = "abi-7-19")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 18;
/// FUSE minor version number 7.19
#[cfg(all(feature = "abi-7-19", not(feature = "abi-7-20")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 19;
/// FUSE minor version number 7.20
#[cfg(all(feature = "abi-7-20", not(feature = "abi-7-21")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 20;
/// FUSE minor version number 7.21
#[cfg(all(feature = "abi-7-21", not(feature = "abi-7-22")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 21;
/// FUSE minor version number 7.22
#[cfg(all(feature = "abi-7-22", not(feature = "abi-7-23")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 22;
/// FUSE minor version number 7.23
#[cfg(all(feature = "abi-7-23", not(feature = "abi-7-24")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 23;
/// FUSE minor version number 7.24
#[cfg(all(feature = "abi-7-24", not(feature = "abi-7-25")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 24;
/// FUSE minor version number 7.25
#[cfg(all(feature = "abi-7-25", not(feature = "abi-7-26")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 25;
/// FUSE minor version number 7.26
#[cfg(all(feature = "abi-7-26", not(feature = "abi-7-27")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 26;
/// FUSE minor version number 7.27
#[cfg(all(feature = "abi-7-27", not(feature = "abi-7-28")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 27;
/// FUSE minor version number 7.28
#[cfg(all(feature = "abi-7-28", not(feature = "abi-7-29")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 28;
/// FUSE minor version number 7.29
#[cfg(all(feature = "abi-7-29", not(feature = "abi-7-30")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 29;
/// FUSE minor version number 7.30
#[cfg(all(feature = "abi-7-30", not(feature = "abi-7-31")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 30;
/// FUSE minor version number 7.31
#[cfg(all(feature = "abi-7-31", not(feature = "abi-7-32")))]
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 31;
/// The node ID of the root inode
pub const FUSE_ROOT_ID: u64 = 1;

/// The type of i-number
pub type INum = u64;

/// FUSE attribute `fuse_attr`
#[derive(Debug)]
#[repr(C)]
pub struct FuseAttr {
    /// Node i-number
    pub ino: INum,
    /// File size
    pub size: u64,
    /// Block numbers
    pub blocks: u64,
    /// Access time seconds
    pub atime: u64,
    /// Content modified time seconds
    pub mtime: u64,
    /// Meta-data changed time seconds
    pub ctime: u64,
    /// Creation time seconds
    #[cfg(target_os = "macos")]
    pub crtime: u64,
    /// Access time nano-seconds
    pub atimensec: u32,
    /// Content modified time nano-seconds
    pub mtimensec: u32,
    /// Meta-data changed time nano-seconds
    pub ctimensec: u32,
    /// Creation time nano-seconds
    #[cfg(target_os = "macos")]
    pub crtimensec: u32,
    /// File mode
    pub mode: u32,
    /// Link numbers
    pub nlink: u32,
    /// User ID
    pub uid: u32,
    /// Group IP
    pub gid: u32,
    /// The device ID that this file (inode) represents if special file
    pub rdev: u32,
    /// see chflags(2)
    #[cfg(target_os = "macos")]
    pub flags: u32,
    /// Block size
    #[cfg(feature = "abi-7-9")]
    pub blksize: u32,
    /// Alignment padding
    #[cfg(feature = "abi-7-9")]
    pub padding: u32,
}

/// FUSE kstatfs `fuse_kstatfs`
#[derive(Debug)]
#[repr(C)]
pub struct FuseKStatFs {
    /// Total blocks (in units of frsize)
    pub blocks: u64,
    /// Free blocks
    pub bfree: u64,
    /// Free blocks for unprivileged users
    pub bavail: u64,
    /// Total inodes
    pub files: u64,
    /// Free inodes
    pub ffree: u64,
    /// Filesystem block size
    pub bsize: u32,
    /// Maximum filename length
    pub namelen: u32,
    /// Fundamental file system block size
    pub frsize: u32,
    /// Alignment padding
    pub padding: u32,
    /// For future use
    pub spare: [u32; 6],
}

/// FUSE file lock `fuse_file_lock`
#[derive(Debug)]
#[repr(C)]
pub struct FuseFileLock {
    /// The starting offset of the lock
    pub start: u64,
    /// The ending offset of the lock
    pub end: u64,
    /// The type of the lock
    pub typ: u32,
    /// The process ID of the lock, tgid
    pub pid: u32,
}

/// Bitmasks for `fuse_setattr_in.valid`
#[allow(dead_code)]
pub mod setattr_flags {
    /// To set file mode
    pub const FATTR_MODE: u32 = 1 << 0_i32;
    /// To set file user ID
    pub const FATTR_UID: u32 = 1 << 1_i32;
    /// To set file group ID
    pub const FATTR_GID: u32 = 1 << 2_i32;
    /// To set file size
    pub const FATTR_SIZE: u32 = 1 << 3_i32;
    /// To set file access time
    pub const FATTR_ATIME: u32 = 1 << 4_i32;
    /// To set content modified time
    pub const FATTR_MTIME: u32 = 1 << 5_i32;
    /// To set file handler
    pub const FATTR_FH: u32 = 1 << 6_i32;
    /// To set atime as of now
    #[cfg(feature = "abi-7-9")]
    pub const FATTR_ATIME_NOW: u32 = 1 << 7_i32;
    /// To set mtime as of now
    #[cfg(feature = "abi-7-9")]
    pub const FATTR_MTIME_NOW: u32 = 1 << 8_i32;
    /// To set file lock owner
    #[cfg(feature = "abi-7-9")]
    pub const FATTR_LOCKOWNER: u32 = 1 << 9_i32;
    /// To set meta-data change time
    #[cfg(feature = "abi-7-23")]
    pub const FATTR_CTIME: u32 = 1 << 10_i32;
    /// To set creation time
    #[cfg(target_os = "macos")]
    pub const FATTR_CRTIME: u32 = 1 << 28_i32;
    /// To set change time
    #[cfg(target_os = "macos")]
    pub const FATTR_CHGTIME: u32 = 1 << 29_i32;
    /// To set backup time
    #[cfg(target_os = "macos")]
    pub const FATTR_BKUPTIME: u32 = 1 << 30_i32;
    /// To set flags, see chflags(2)
    #[cfg(target_os = "macos")]
    pub const FATTR_FLAGS: u32 = 1 << 31_i32;
}

pub use setattr_flags::*;

/// Flags returned by the OPEN request
///
/// `FOPEN_DIRECT_IO`: bypass page cache for this open file
///
/// `FOPEN_KEEP_CACHE`: don't invalidate the data cache on open
///
/// `FOPEN_NONSEEKABLE`: the file is not seekable
///
/// `FOPEN_CACHE_DIR`: allow caching this directory
///
/// `FOPEN_STREAM`: the file is stream-like (no file position at all)
#[allow(dead_code)]
pub mod fopen_flags {
    /// bypass page cache for this open file
    pub const FOPEN_DIRECT_IO: u32 = 1;

    /// don't invalidate the data cache on open
    pub const FOPEN_KEEP_CACHE: u32 = 1 << 1_i32;

    /// the file is not seekable
    #[cfg(feature = "abi-7-10")]
    pub const FOPEN_NONSEEKABLE: u32 = 1 << 2_i32;

    /// allow caching this directory
    #[cfg(feature = "abi-7-28")]
    pub const FOPEN_CACHE_DIR: u32 = 1 << 3_i32;

    /// the file is stream-like (no file position at all)
    #[cfg(feature = "abi-7-31")]
    pub const FOPEN_STREAM: u32 = 1 << 4_i32;

    /// macOS purge attribute
    #[cfg(target_os = "macos")]
    pub const FOPEN_PURGE_ATTR: u32 = 1 << 30_i32;

    /// macOS purge UBC
    #[cfg(target_os = "macos")]
    pub const FOPEN_PURGE_UBC: u32 = 1 << 31_i32;
}

pub use fopen_flags::*;

/// INIT request/reply flags
#[allow(dead_code)]
pub mod init_flags {
    /// `FUSE_ASYNC_READ`: asynchronous read requests
    pub const FUSE_ASYNC_READ: u32 = 1;
    /// `FUSE_POSIX_LOCKS`: remote locking for POSIX file locks
    pub const FUSE_POSIX_LOCKS: u32 = 1 << 1_i32;
    /// `FUSE_FILE_OPS`: kernel sends file handle for fstat, etc... (not yet
    /// supported)
    #[cfg(feature = "abi-7-9")]
    pub const FUSE_FILE_OPS: u32 = 1 << 2_i32;
    /// `FUSE_ATOMIC_O_TRUNC`: handles the `O_TRUNC` open flag in the filesystem
    #[cfg(feature = "abi-7-9")]
    pub const FUSE_ATOMIC_O_TRUNC: u32 = 1 << 3_i32;
    /// `FUSE_EXPORT_SUPPORT`: filesystem handles lookups of "." and ".."
    #[cfg(feature = "abi-7-10")]
    pub const FUSE_EXPORT_SUPPORT: u32 = 1 << 4_i32;
    /// `FUSE_BIG_WRITES`: filesystem can handle write size larger than 4kB
    #[cfg(feature = "abi-7-9")]
    pub const FUSE_BIG_WRITES: u32 = 1 << 5_i32;
    /// `FUSE_DONT_MASK`: don't apply umask to file mode on create operations
    #[cfg(feature = "abi-7-12")]
    pub const FUSE_DONT_MASK: u32 = 1 << 6_i32;
    /// `FUSE_SPLICE_WRITE`: kernel supports splice write on the device
    #[cfg(feature = "abi-7-14")]
    pub const FUSE_SPLICE_WRITE: u32 = 1 << 7_i32;
    /// `FUSE_SPLICE_MOVE`: kernel supports splice move on the device
    #[cfg(feature = "abi-7-14")]
    pub const FUSE_SPLICE_MOVE: u32 = 1 << 8_i32;
    /// `FUSE_SPLICE_READ`: kernel supports splice read on the device
    #[cfg(feature = "abi-7-14")]
    pub const FUSE_SPLICE_READ: u32 = 1 << 9_i32;
    /// `FUSE_FLOCK_LOCKS`: remote locking for BSD style file locks
    #[cfg(feature = "abi-7-17")]
    pub const FUSE_FLOCK_LOCKS: u32 = 1 << 10_i32;
    /// `FUSE_HAS_IOCTL_DIR`: kernel supports ioctl on directories
    #[cfg(feature = "abi-7-18")]
    pub const FUSE_HAS_IOCTL_DIR: u32 = 1 << 11_i32;
    /// `FUSE_AUTO_INVAL_DATA`: automatically invalidate cached pages
    #[cfg(feature = "abi-7-20")]
    pub const FUSE_AUTO_INVAL_DATA: u32 = 1 << 12_i32;
    /// `FUSE_DO_READDIRPLUS`: do READDIRPLUS (READDIR+LOOKUP in one)
    #[cfg(feature = "abi-7-21")]
    pub const FUSE_DO_READDIRPLUS: u32 = 1 << 13_i32;

    // TODO: verify it's added in 7.21
    /// `FUSE_READDIRPLUS_AUTO`: adaptive readdirplus
    #[cfg(feature = "abi-7-21")]
    pub const FUSE_READDIRPLUS_AUTO: u32 = 1 << 14_i32;
    /// `FUSE_ASYNC_DIO`: asynchronous direct I/O submission
    #[cfg(feature = "abi-7-22")]
    pub const FUSE_ASYNC_DIO: u32 = 1 << 15_i32;
    /// `FUSE_WRITEBACK_CACHE`: use writeback cache for buffered writes
    #[cfg(feature = "abi-7-23")]
    pub const FUSE_WRITEBACK_CACHE: u32 = 1 << 16_i32;
    /// `FUSE_NO_OPEN_SUPPORT`: kernel supports zero-message opens
    #[cfg(feature = "abi-7-23")]
    pub const FUSE_NO_OPEN_SUPPORT: u32 = 1 << 17_i32;
    /// `FUSE_PARALLEL_DIROPS`: allow parallel lookups and readdir
    #[cfg(feature = "abi-7-25")]
    pub const FUSE_PARALLEL_DIROPS: u32 = 1 << 18_i32;
    /// `FUSE_HANDLE_KILLPRIV`: fs handles killing suid/sgid/cap on
    /// write/chown/trunc
    #[cfg(feature = "abi-7-26")]
    pub const FUSE_HANDLE_KILLPRIV: u32 = 1 << 19_i32;
    /// `FUSE_POSIX_ACL`: filesystem supports posix acls
    #[cfg(feature = "abi-7-26")]
    pub const FUSE_POSIX_ACL: u32 = 1 << 20_i32;
    /// `FUSE_ABORT_ERROR`: reading the device after abort returns ECONNABORTED
    #[cfg(feature = "abi-7-27")]
    pub const FUSE_ABORT_ERROR: u32 = 1 << 21_i32;
    /// `FUSE_MAX_PAGES`: `init_out.max_pages` contains the max number of req
    /// pages
    #[cfg(feature = "abi-7-28")]
    pub const FUSE_MAX_PAGES: u32 = 1 << 22_i32;
    /// `FUSE_CACHE_SYMLINKS`: cache READLINK responses
    #[cfg(feature = "abi-7-28")]
    pub const FUSE_CACHE_SYMLINKS: u32 = 1 << 23_i32;
    /// `FUSE_NO_OPENDIR_SUPPORT`: kernel supports zero-message opendir
    #[cfg(feature = "abi-7-29")]
    pub const FUSE_NO_OPENDIR_SUPPORT: u32 = 1 << 24_i32;
    /// `FUSE_EXPLICIT_INVAL_DATA`: only invalidate cached pages on explicit
    /// request
    #[cfg(feature = "abi-7-30")]
    pub const FUSE_EXPLICIT_INVAL_DATA: u32 = 1 << 25_i32;

    /// macOS allocate
    #[cfg(target_os = "macos")]
    pub const FUSE_ALLOCATE: u32 = 1 << 27_i32;
    /// macOS exchange data
    #[cfg(target_os = "macos")]
    pub const FUSE_EXCHANGE_DATA: u32 = 1 << 28_i32;
    /// macOS case insensitive
    #[cfg(target_os = "macos")]
    pub const FUSE_CASE_INSENSITIVE: u32 = 1 << 29_i32;
    /// macOS volume rename
    #[cfg(target_os = "macos")]
    pub const FUSE_VOL_RENAME: u32 = 1 << 30_i32;
    /// macOS extended times
    #[cfg(target_os = "macos")]
    pub const FUSE_XTIMES: u32 = 1 << 31_i32;
}

pub use init_flags::*;

/// CUSE INIT request/reply flags
///
/// `CUSE_UNRESTRICTED_IOCTL`:  use unrestricted ioctl
#[allow(dead_code)]
#[cfg(feature = "abi-7-11")]
pub const CUSE_UNRESTRICTED_IOCTL: u32 = 1 << 0_i32; // use unrestricted ioctl

/// Release with flush
pub const FUSE_RELEASE_FLUSH: u32 = 1 << 0_i32;
/// Release with `flock` unlock
#[allow(dead_code)]
#[cfg(feature = "abi-7-17")]
pub const FUSE_RELEASE_FLOCK_UNLOCK: u32 = 1 << 1_i32;

/// Getattr flags
#[allow(dead_code)]
#[cfg(feature = "abi-7-9")]
pub const FUSE_GETATTR_FH: u32 = 1 << 0_i32;

/// Lock flags
#[allow(dead_code)]
#[cfg(feature = "abi-7-9")]
pub const FUSE_LK_FLOCK: u32 = 1 << 0_i32;

/// WRITE flags
#[allow(dead_code)]
pub mod write_flags {
    /// `FUSE_WRITE_CACHE`: delayed write from page cache, file handle is
    /// guessed
    #[cfg(feature = "abi-7-9")]
    pub const FUSE_WRITE_CACHE: u32 = 1 << 0_i32;
    /// `FUSE_WRITE_LOCKOWNER`: `lock_owner` field is valid
    #[cfg(feature = "abi-7-9")]
    pub const FUSE_WRITE_LOCKOWNER: u32 = 1 << 1_i32;
    /// `FUSE_WRITE_KILL_PRIV`: kill suid and sgid bits
    #[cfg(feature = "abi-7-31")]
    pub const FUSE_WRITE_KILL_PRIV: u32 = 1 << 2_i32;
}

pub use write_flags::*;

/// Read flags
#[allow(dead_code)]
#[cfg(feature = "abi-7-9")]
pub const FUSE_READ_LOCKOWNER: u32 = 1 << 1_i32;

/// Ioctl flags
#[allow(dead_code)]
pub mod ioctl_flags {
    /// `FUSE_IOCTL_COMPAT`: 32bit compat ioctl on 64bit machine
    #[cfg(feature = "abi-7-11")]
    pub const FUSE_IOCTL_COMPAT: u32 = 1 << 0_i32;
    /// `FUSE_IOCTL_UNRESTRICTED`: not restricted to well-formed ioctls, retry
    /// allowed
    #[cfg(feature = "abi-7-11")]
    pub const FUSE_IOCTL_UNRESTRICTED: u32 = 1 << 1_i32;
    /// `FUSE_IOCTL_RETRY`: retry with new iovecs
    #[cfg(feature = "abi-7-11")]
    pub const FUSE_IOCTL_RETRY: u32 = 1 << 2_i32;
    /// `FUSE_IOCTL_32BIT`: 32bit ioctl
    #[cfg(feature = "abi-7-16")]
    pub const FUSE_IOCTL_32BIT: u32 = 1 << 3_i32;
    /// `FUSE_IOCTL_DIR`: is a directory
    #[cfg(feature = "abi-7-18")]
    pub const FUSE_IOCTL_DIR: u32 = 1 << 4_i32;
    /// `FUSE_IOCTL_COMPAT_X32`: x32 compat ioctl on 64bit machine (64bit
    /// `time_t`)
    #[cfg(feature = "abi-7-30")]
    pub const FUSE_IOCTL_COMPAT_X32: u32 = 1 << 5_i32;

    /// `FUSE_IOCTL_MAX_IOV`: maximum of `in_iovecs + out_iovecs`
    #[cfg(feature = "abi-7-11")]
    pub const FUSE_IOCTL_MAX_IOV: u32 = 256;
}

pub use ioctl_flags::*;

/// Poll flags
///
/// `FUSE_POLL_SCHEDULE_NOTIFY`: request poll notify
#[allow(dead_code)]
#[cfg(feature = "abi-7-11")]
pub const FUSE_POLL_SCHEDULE_NOTIFY: u32 = 1 << 0_i32;

/// Fsync flags
///
/// `FUSE_FSYNC_FDATASYNC`: sync data only, not metadata
#[allow(dead_code)]
#[cfg(feature = "abi-7-31")]
pub const FUSE_FSYNC_FDATASYNC: u32 = 1 << 0_i32;

/// FUSE operation code `fuse_opcode`
#[allow(
    non_camel_case_types,
    // clippy::upper_case_acronyms,
)]
#[derive(Debug)]
#[non_exhaustive]
#[repr(C)]
pub enum FuseOpCode {
    /// Look up a directory entry by name and get its attributes
    FUSE_LOOKUP = 1,
    /// Forget about an inode, no reply
    FUSE_FORGET = 2,
    /// Get file attributes
    FUSE_GETATTR = 3,
    /// Set file attributes
    FUSE_SETATTR = 4,
    /// Read symbolic link
    FUSE_READLINK = 5,
    /// Create a symbolic link
    FUSE_SYMLINK = 6,
    /// Create file node
    FUSE_MKNOD = 8,
    /// Create a directory
    FUSE_MKDIR = 9,
    /// Remove a file
    FUSE_UNLINK = 10,
    /// Remove a directory
    FUSE_RMDIR = 11,
    /// Rename a file
    FUSE_RENAME = 12,
    /// Create a hard link
    FUSE_LINK = 13,
    /// Open a file
    FUSE_OPEN = 14,
    /// Read data from file
    FUSE_READ = 15,
    /// Write data to file
    FUSE_WRITE = 16,
    /// Get file system statistics
    FUSE_STATFS = 17,
    /// Release an open file
    FUSE_RELEASE = 18,
    /// Synchronize file contents
    FUSE_FSYNC = 20,
    /// Set an extended attribute
    FUSE_SETXATTR = 21,
    /// Get an extended attribute
    FUSE_GETXATTR = 22,
    /// List extended attribute names
    FUSE_LISTXATTR = 23,
    /// Remove an extended attribute
    FUSE_REMOVEXATTR = 24,
    /// Flush file
    FUSE_FLUSH = 25,
    /// Initialize filesystem
    FUSE_INIT = 26,
    /// Open a directory
    FUSE_OPENDIR = 27,
    /// Read directory
    FUSE_READDIR = 28,
    /// Release an open directory
    FUSE_RELEASEDIR = 29,
    /// Synchronize directory contents
    FUSE_FSYNCDIR = 30,
    /// Test for a POSIX file lock
    FUSE_GETLK = 31,
    /// Acquire, modify or release a POSIX file lock
    FUSE_SETLK = 32,
    /// Acquire, modify or release a POSIX file lock and wait
    FUSE_SETLKW = 33,
    /// Check file access permissions
    FUSE_ACCESS = 34,
    /// Create and open a file
    FUSE_CREATE = 35,
    /// Interrupt a previous FUSE request
    FUSE_INTERRUPT = 36,
    /// Map block index withClean up filesystemin file to block index within
    /// device
    FUSE_BMAP = 37,
    /// Clean up filesystem
    FUSE_DESTROY = 38,
    /// Ioctl
    // #[cfg(feature = "abi-7-11")]
    FUSE_IOCTL = 39,
    /// Poll for IO readiness
    // #[cfg(feature = "abi-7-11")]
    FUSE_POLL = 40,
    /// A reply to a NOTIFY_RETRIEVE notification
    // #[cfg(feature = "abi-7-15")]
    FUSE_NOTIFY_REPLY = 41,
    /// Batch forget inodes
    // #[cfg(feature = "abi-7-16")]
    FUSE_BATCH_FORGET = 42,
    /// Allocate requested space
    // #[cfg(feature = "abi-7-19")]
    FUSE_FALLOCATE = 43,
    /// Read directory with attributes
    // #[cfg(feature = "abi-7-21")]
    FUSE_READDIRPLUS = 44,
    /// Rename2
    #[cfg(feature = "abi-7-23")]
    FUSE_RENAME2 = 45,
    /// Find next data or hole after the specified offset
    // #[cfg(feature = "abi-7-24")]
    FUSE_LSEEK = 46,
    /// Copy a range of data from an opened file to another
    // #[cfg(feature = "abi-7-28")]
    FUSE_COPY_FILE_RANGE = 47,

    /// Set volume name
    #[cfg(target_os = "macos")]
    FUSE_SETVOLNAME = 61,
    /// Get extended times
    #[cfg(target_os = "macos")]
    FUSE_GETXTIMES = 62,
    /// Rename exchange
    #[cfg(target_os = "macos")]
    FUSE_EXCHANGE = 63,

    /// CUSE specific operations
    #[cfg(feature = "abi-7-11")]
    CUSE_INIT = 4096,
}

/// FUSE notify code `fuse_notify_code`
#[allow(dead_code)]
#[allow(
    non_camel_case_types,
    // clippy::upper_case_acronyms,
)]
#[cfg(feature = "abi-7-11")]
#[derive(Debug)]
#[repr(C)]
pub enum FuseNotifyCode {
    /// Poll
    FUSE_POLL = 1,
    /// Notify invalid inode
    #[cfg(feature = "abi-7-12")]
    FUSE_NOTIFY_INVAL_INODE = 2,
    /// Notify invalid entry
    #[cfg(feature = "abi-7-12")]
    FUSE_NOTIFY_INVAL_ENTRY = 3,
    /// Notify store
    #[cfg(feature = "abi-7-15")]
    FUSE_NOTIFY_STORE = 4,
    /// Notify retrieve
    #[cfg(feature = "abi-7-15")]
    FUSE_NOTIFY_RETRIEVE = 5,
    /// Notify delete
    #[cfg(feature = "abi-7-18")]
    FUSE_NOTIFY_DELETE = 6,
    /// Max notify code
    FUSE_NOTIFY_CODE_MAX,
}

/// The read buffer is required to be at least 8k, but may be much larger
#[allow(dead_code)]
pub const FUSE_MIN_READ_BUFFER: usize = 8192;

/// FUSE compatible configurations
#[allow(dead_code)]
pub mod fuse_compat_configs {
    /// FUSE compatible statfs size when minior version lower than 4
    pub const FUSE_COMPAT_STATFS_SIZE: usize = 48;
    /// FUSE compatible directory entry related response size for macOS and
    /// version < 7.9
    #[cfg(all(target_os = "macos", feature = "abi-7-9"))]
    pub const FUSE_COMPAT_ENTRY_OUT_SIZE: usize = 136;
    /// FUSE compatible directory entry related response size for version < 7.9
    #[cfg(feature = "abi-7-9")]
    pub const FUSE_COMPAT_ENTRY_OUT_SIZE: usize = 120;
    /// FUSE compatible attribute related response size for macOS and version <
    /// 7.9
    #[cfg(all(target_os = "macos", feature = "abi-7-9"))]
    pub const FUSE_COMPAT_ATTR_OUT_SIZE: usize = 112;
    /// FUSE compatible attribute related response size for version < 7.9
    #[cfg(feature = "abi-7-9")]
    pub const FUSE_COMPAT_ATTR_OUT_SIZE: usize = 96;
    /// FUSE compatible `mknod` request size for version < 7.12
    #[cfg(feature = "abi-7-12")]
    pub const FUSE_COMPAT_MKNOD_IN_SIZE: usize = 8;
    /// FUSE compatible `write` request size for version < 7.9
    #[cfg(feature = "abi-7-9")]
    pub const FUSE_COMPAT_WRITE_IN_SIZE: usize = 24;
    /// FUSE compatible `init` response size for version < 7.5
    #[cfg(feature = "abi-7-23")]
    pub const FUSE_COMPAT_INIT_OUT_SIZE: usize = 8;
    /// FUSE compatible `init` response size for version < 7.23
    #[cfg(feature = "abi-7-23")]
    pub const FUSE_COMPAT_22_INIT_OUT_SIZE: usize = 24;
}

pub use fuse_compat_configs::*;

/// FUSE entry response `fuse_entry_out`
#[derive(Debug)]
#[repr(C)]
pub struct FuseEntryOut {
    /// Inode ID
    pub nodeid: u64,
    /// Inode generation: nodeid:gen must be unique for the fs's lifetime
    pub generation: u64,
    /// Cache timeout seconds for the name
    pub entry_valid: u64,
    /// Cache timeout seconds for the attributes
    pub attr_valid: u64,
    /// Cache timeout nano-seconds for the name
    pub entry_valid_nsec: u32,
    /// Cache timeout nano-seconds for the attributes
    pub attr_valid_nsec: u32,
    /// FUSE attributes
    pub attr: FuseAttr,
}

/// FUSE forget request input `fuse_forget_in`
#[derive(Debug)]
#[repr(C)]
pub struct FuseForgetIn {
    /// The number of lookup to forget
    pub nlookup: u64,
}

/// FUSE forget request input `fuse_forget_one`
// #[cfg(feature = "abi-7-16")]
#[derive(Debug)]
#[repr(C)]
pub struct FuseForgetOne {
    /// The node i-number
    pub nodeid: u64,
    /// The number of lookup to forget
    pub nlookup: u64,
}

/// FUSE batch forget request input `fuse_batch_forget_in`
// #[cfg(feature = "abi-7-16")]
#[derive(Debug)]
#[repr(C)]
pub struct FuseBatchForgetIn {
    /// Batch count
    pub count: u32,
    /// Alignment padding
    pub dummy: u32,
    // Followed by `count` number of FuseForgetOne
    // forgets: &[FuseForgetOne]
}

/// FUSE get attribute request input `fuse_getattr_in`
#[cfg(feature = "abi-7-9")]
#[derive(Debug)]
#[repr(C)]
pub struct FuseGetAttrIn {
    /// Get attribute flags
    pub getattr_flags: u32,
    /// Alignment padding
    pub dummy: u32,
    /// File handler
    pub fh: u64,
}

/// FUSE get attribute response `fuse_attr_out`
#[derive(Debug)]
#[repr(C)]
pub struct FuseAttrOut {
    /// Cache timeout seconds for the attributes
    pub attr_valid: u64,
    /// Cache timeout nano-seconds for the attributes
    pub attr_valid_nsec: u32,
    /// Alignment padding
    pub dummy: u32,
    /// FUSE file attribute
    pub attr: FuseAttr,
}

/// FUSE get extended timestamp response `fuse_getxtimes_out`
#[cfg(target_os = "macos")]
#[derive(Debug)]
#[repr(C)]
pub struct FuseGetXTimesOut {
    /// Backup time seconds
    pub bkuptime: u64,
    /// Creation time seconds
    pub crtime: u64,
    /// Backup time nano-seconds
    pub bkuptimensec: u32,
    /// Creation time nano-seconds
    pub crtimensec: u32,
}

/// FUSE make node request input `fuse_mknod_in`
#[derive(Debug)]
#[repr(C)]
pub struct FuseMkNodIn {
    /// File mode
    pub mode: u32,
    /// The device ID that this file (inode) represents if special file
    pub rdev: u32,
    /// The user file creation mode mask
    #[cfg(feature = "abi-7-12")]
    pub umask: u32,
    /// Alignment padding
    #[cfg(feature = "abi-7-12")]
    pub padding: u32,
}

/// FUSE make directory request input `fuse_mkdir_in`
#[derive(Debug)]
#[repr(C)]
pub struct FuseMkDirIn {
    /// Directory mode
    pub mode: u32,
    /// Alignment padding
    #[cfg(not(feature = "abi-7-12"))]
    pub padding: u32,
    /// The user directory creation mode mask
    #[cfg(feature = "abi-7-12")]
    pub umask: u32,
}

/// FUSE rename request input `fuse_rename_in`
#[derive(Debug)]
#[repr(C)]
pub struct FuseRenameIn {
    /// The new directory i-number
    pub newdir: u64,
}

/// FUSE rename2 request input `fuse_rename2_in`
///
/// Available when the protocol version is greater than 7.22.
/// This is checked by the kernel so that the app won't receive such a request.
///
/// See [here](https://github.com/torvalds/linux/blob/8f6f76a6a29f36d2f3e4510d0bde5046672f6924/fs/fuse/dir.c#L1077C2-L1088C3)
/// for the source code of checking.
#[cfg(feature = "abi-7-23")]
#[derive(Debug)]
#[repr(C)]
pub struct FuseRename2In {
    /// The new directory i-number
    pub newdir: u64,
    /// The flags maybe either `RENAME_NOREPLACE`=1 or `RENAME_EXCHANGE`=2
    pub flags: u32,
    /// Alignment padding
    pub padding: u32,
}

/// FUSE exchange request input `fuse_exchange_in`
#[cfg(target_os = "macos")]
#[derive(Debug)]
#[repr(C)]
pub struct FuseExchangeIn {
    /// Old directory i-number
    pub olddir: u64,
    /// New directory i-number
    pub newdir: u64,
    /// Exchange options
    pub options: u64,
}

/// FUSE link request input `fuse_link_in`
#[derive(Debug)]
#[repr(C)]
pub struct FuseLinkIn {
    /// The old node i-number
    pub oldnodeid: u64,
}

/// FUSE set attribute request input `fuse_setattr_in`
#[derive(Debug)]
#[repr(C)]
pub struct FuseSetAttrIn {
    /// FUSE set attribute bit mask
    pub valid: u32,
    /// Alignment padding
    pub padding: u32,
    /// File handler
    pub fh: u64,
    /// File size
    pub size: u64,
    /// Alignment padding
    #[cfg(not(feature = "abi-7-9"))]
    pub unused1: u64,
    /// Lock owner
    #[cfg(feature = "abi-7-9")]
    pub lock_owner: u64,
    /// Access time seconds
    pub atime: u64,
    /// Content modified time seconds
    pub mtime: u64,
    /// Alignment padding
    #[cfg(not(feature = "abi-7-23"))]
    pub unused2: u64,
    /// Meta-data changed time seconds
    #[cfg(feature = "abi-7-23")]
    pub ctime: u64,
    /// Access time nano-seconds
    pub atimensec: u32,
    /// Content modified time nano-seconds
    pub mtimensec: u32,
    /// Alignment padding
    #[cfg(not(feature = "abi-7-23"))]
    pub unused3: u32,
    /// Meta-data changed time nano-seconds
    #[cfg(feature = "abi-7-23")]
    pub ctimensec: u32,
    /// File mode
    pub mode: u32,
    /// Alignment padding
    pub unused4: u32,
    /// User ID
    pub uid: u32,
    /// Group ID
    pub gid: u32,
    /// Alignment padding
    pub unused5: u32,
    /// Backup time seconds
    #[cfg(target_os = "macos")]
    pub bkuptime: u64,
    /// Change time seconds
    #[cfg(target_os = "macos")]
    pub chgtime: u64,
    /// Creation time seconds
    #[cfg(target_os = "macos")]
    pub crtime: u64,
    /// Backup time nano-seconds
    #[cfg(target_os = "macos")]
    pub bkuptimensec: u32,
    /// Change time nano-seconds
    #[cfg(target_os = "macos")]
    pub chgtimensec: u32,
    /// Creation time nano-seconds
    #[cfg(target_os = "macos")]
    pub crtimensec: u32,
    /// See chflags(2)
    #[cfg(target_os = "macos")]
    pub flags: u32,
}

/// FUSE open request input `fuse_open_in`
#[derive(Debug)]
#[repr(C)]
pub struct FuseOpenIn {
    /// Open flags
    pub flags: u32,
    /// Alignment padding
    pub unused: u32,
}

/// FUSE create request input `fuse_create_in`
#[derive(Debug)]
#[repr(C)]
pub struct FuseCreateIn {
    /// Creation flags
    pub flags: u32,
    /// File mode
    pub mode: u32,
    /// The user file creation mode mask
    #[cfg(feature = "abi-7-12")]
    pub umask: u32,
    /// Alignment padding
    #[cfg(feature = "abi-7-12")]
    pub padding: u32,
}

/// FUSE open resoponse `fuse_open_out`
#[derive(Debug)]
#[repr(C)]
pub struct FuseOpenOut {
    /// File handler
    pub fh: u64,
    /// Open flags
    pub open_flags: u32,
    /// Alignment padding
    pub padding: u32,
}

/// FUSE release request input `fuse_release_in`
#[derive(Debug)]
#[repr(C)]
pub struct FuseReleaseIn {
    /// File handler
    pub fh: u64,
    /// Open flags
    pub flags: u32,
    /// Release flags
    pub release_flags: u32,
    /// Lock owner
    pub lock_owner: u64,
}

/// FUSE flush request input `fuse_flush_in`
#[derive(Debug)]
#[repr(C)]
pub struct FuseFlushIn {
    /// File handler
    pub fh: u64,
    /// Alignment padding
    pub unused: u32,
    /// Alignment padding
    pub padding: u32,
    /// Lock owner
    pub lock_owner: u64,
}

/// FUSE read request input `fuse_read_in`
#[derive(Debug)]
#[repr(C)]
pub struct FuseReadIn {
    /// File handler
    pub fh: u64,
    /// Read offset
    pub offset: u64,
    /// Read size
    pub size: u32,
    /// Read flags
    #[cfg(feature = "abi-7-9")]
    pub read_flags: u32,
    /// Lock owner
    #[cfg(feature = "abi-7-9")]
    pub lock_owner: u64,
    /// Open flags
    #[cfg(feature = "abi-7-9")]
    pub flags: u32,
    /// Alignment padding
    pub padding: u32,
}

/// FUSE write request input `fuse_write_in`
#[derive(Debug)]
#[repr(C)]
pub struct FuseWriteIn {
    /// File handler
    pub fh: u64,
    /// Write offset
    pub offset: u64,
    /// Write size
    pub size: u32,
    /// Write flags
    pub write_flags: u32,
    /// Lock owner
    #[cfg(feature = "abi-7-9")]
    pub lock_owner: u64,
    /// Open flags
    #[cfg(feature = "abi-7-9")]
    pub flags: u32,
    /// Alignment padding
    #[cfg(feature = "abi-7-9")]
    pub padding: u32,
}

/// FUSE write response `fuse_write_out`
#[derive(Debug)]
#[repr(C)]
pub struct FuseWriteOut {
    /// Write size
    pub size: u32,
    /// Alignment padding
    pub padding: u32,
}

/// FUSE statfs response `fuse_statfs_out`
#[derive(Debug)]
#[repr(C)]
pub struct FuseStatFsOut {
    /// FUSE kstatfs
    pub st: FuseKStatFs,
}

/// FUSE fsync request input `fuse_fsync_in`
#[derive(Debug)]
#[repr(C)]
pub struct FuseFSyncIn {
    /// File handler
    pub fh: u64,
    /// File sync flags
    pub fsync_flags: u32,
    /// Alignment padding
    pub padding: u32,
}

/// FUSE set extended attribute request input `fuse_setxattr_in`
#[derive(Debug)]
#[repr(C)]
pub struct FuseSetXAttrIn {
    /// The size of extended attribute value to set
    pub size: u32,
    /// The flags that specifies the meanings of this operation
    pub flags: u32,
    /// Attribute position
    #[cfg(target_os = "macos")]
    pub position: u32,
    /// Alignment padding
    #[cfg(target_os = "macos")]
    pub padding: u32,
}

/// FUSE get extended attribute request input `fuse_getxattr_in`
#[derive(Debug)]
#[repr(C)]
pub struct FuseGetXAttrIn {
    /// The maximum length of the attribute value to be replied
    pub size: u32,
    /// Alignment padding
    pub padding: u32,
    /// Attribute position
    #[cfg(target_os = "macos")]
    pub position: u32,
    /// Alignment padding
    #[cfg(target_os = "macos")]
    pub padding2: u32,
}

/// FUSE get extended attribute response `fuse_getxattr_out`
#[derive(Debug)]
#[repr(C)]
pub struct FuseGetXAttrOut {
    /// The size of the extended attribute value
    pub size: u32,
    /// Alignment padding
    pub padding: u32,
}

/// FUSE lock request input `fuse_lk_in`
#[derive(Debug)]
#[repr(C)]
pub struct FuseLockIn {
    /// File handler
    pub fh: u64,
    /// Lock owner
    pub owner: u64,
    /// FUSE file lock
    pub lk: FuseFileLock,
    /// Lock flags
    #[cfg(feature = "abi-7-9")]
    pub lk_flags: u32,
    /// Alignment padding
    #[cfg(feature = "abi-7-9")]
    pub padding: u32,
}

/// FUSE lock response `fuse_lk_out`
#[derive(Debug)]
#[repr(C)]
pub struct FuseLockOut {
    /// FUSE file lock
    pub lk: FuseFileLock,
}

/// FUSE access request input `fuse_access_in`
#[derive(Debug)]
#[repr(C)]
pub struct FuseAccessIn {
    /// The requested access mode
    pub mask: u32,
    /// Alignment padding
    pub padding: u32,
}

/// FUSE init request input `fuse_init_in`
#[derive(Debug)]
#[repr(C)]
pub struct FuseInitIn {
    /// FUSE protocol major version
    pub major: u32,
    /// FUSE protocol minor version
    pub minor: u32,
    /// FUSE maximum readahead size
    pub max_readahead: u32,
    /// FUSE init flags
    pub flags: u32,
}

/// FUSE init response `fuse_init_out`
#[derive(Debug)]
#[repr(C)]
pub struct FuseInitOut {
    /// FUSE protocol major version
    pub major: u32,
    /// FUSE protocol minor version
    pub minor: u32,
    /// FUSE maximum readahead size
    pub max_readahead: u32,
    /// FUSE init flags
    pub flags: u32,
    /// Alignment padding
    #[cfg(not(feature = "abi-7-13"))]
    pub unused: u32,
    /// Max background pending requests under processing
    #[cfg(feature = "abi-7-13")]
    pub max_background: u16,
    /// Notify FUSE kernel module to mark the filesystem as "congested"
    /// if the number of pending requests above this threshold
    #[cfg(feature = "abi-7-13")]
    pub congestion_threshold: u16,
    /// The max size of write requests from the kernel
    pub max_write: u32,
    /// The timestamp granularity supported by the FUSE filesystem
    /// The default is 1 for full nano-second resolution, 1000000000 for second
    /// resolution
    #[cfg(feature = "abi-7-23")]
    pub time_gran: u32,
    // unused: [u32; 9] is defined between 7-13 and 7-27
    // TODO: verify cfg works
    /// Alignment padding
    #[cfg(all(feature = "abi-7-23", not(feature = "abi-7-28")))]
    pub unused: [u32; 9],
    ///
    #[cfg(feature = "abi-7-28")]
    pub max_pages: u16,
    #[cfg(feature = "abi-7-28")]
    /// Alignment padding
    pub padding: u16,
    /// For future use
    #[cfg(feature = "abi-7-28")]
    pub unused: [u32; 8],
}

/// CUSE device info max size
#[allow(dead_code)]
#[cfg(feature = "abi-7-11")]
pub const CUSE_INIT_INFO_MAX: u32 = 4096;

/// CUSE init request input `cuse_init_in`
#[cfg(feature = "abi-7-11")]
#[derive(Debug)]
#[repr(C)]
pub struct CuseInitIn {
    /// Protocol major version
    pub major: u32,
    /// Protocol minor version
    pub minor: u32,
    /// Alignment padding
    pub unused: u32,
    /// CUSE flags
    pub flags: u32,
}

/// CUSE init response `cuse_init_out`
#[cfg(feature = "abi-7-11")]
#[derive(Debug)]
#[repr(C)]
pub struct CuseInitOut {
    /// Protocol major version
    pub major: u32,
    /// Protocol minor version
    pub minor: u32,
    /// Alignment padding
    pub unused: u32,
    /// CUSE flags
    pub flags: u32,
    /// Max read size
    pub max_read: u32,
    /// Max write size
    pub max_write: u32,
    /// Device major version
    pub dev_major: u32,
    /// Device minor version
    pub dev_minor: u32,
    /// For future use
    pub spare: [u32; 10],
}

/// FUSE interrupt request input `fuse_interrupt_in`
#[derive(Debug)]
#[repr(C)]
pub struct FuseInterruptIn {
    /// Interrupted FUSE request unique ID
    pub unique: u64,
}

/// FUSE bmap request input `fuse_bmap_in`
#[derive(Debug)]
#[repr(C)]
pub struct FuseBMapIn {
    /// The block index within file to be mapped
    pub block: u64,
    /// The unit of block index
    pub blocksize: u32,
    /// Alignment padding
    pub padding: u32,
}

/// FUSE bmap response `fuse_bmap_out`
#[derive(Debug)]
#[repr(C)]
pub struct FuseBMapOut {
    /// The block index to be mapped
    pub block: u64,
}

/// FUSE ioctl request input `fuse_ioctl_in`
// #[cfg(feature = "abi-7-11")]
#[derive(Debug)]
#[repr(C)]
pub struct FuseIoCtlIn {
    /// File handler
    pub fh: u64,
    /// FUSE ioctl flags
    pub flags: u32,
    /// FUSE ioctl command
    pub cmd: u32,
    /// FUSE ioctl command argument
    pub arg: u64,
    /// The number of fetched bytes
    pub in_size: u32,
    /// The maximum size of output data
    pub out_size: u32,
}

/// FUSE ioctl iovec `fuse_ioctl_iovec`
#[cfg(feature = "abi-7-16")]
#[derive(Debug)]
#[repr(C)]
pub struct FuseIoCtlIoVec {
    /// iovec starting address
    pub base: u64,
    /// Number of bytes to transfer
    pub len: u64,
}

/// FUSE ioctl response `fuse_ioctl_out`
#[cfg(feature = "abi-7-11")]
#[derive(Debug)]
#[repr(C)]
pub struct FuseIoCtlOut {
    /// Result to be passed to the caller
    pub result: i32,
    /// `FUSE_IOCTL_*` flags
    pub flags: u32,
    /// iovec specifying data to fetch from the caller
    pub in_iovs: u32,
    /// iovec specifying addresses to write output to
    pub out_iovs: u32,
}

/// FUSE poll request input `fuse_poll_in`
// #[cfg(feature = "abi-7-11")]
#[derive(Debug)]
#[repr(C)]
pub struct FusePollIn {
    /// File handler
    pub fh: u64,
    /// Wakeup handler
    pub kh: u64,
    /// Poll flags
    pub flags: u32,
    /// Alignment padding
    #[cfg(feature = "abi-7-11")]
    pub padding: u32,
    /// Poll events
    #[cfg(feature = "abi-7-21")]
    pub events: u32,
}

/// FUSE poll response `fuse_poll_out`
#[cfg(feature = "abi-7-11")]
#[derive(Debug)]
#[repr(C)]
pub struct FusePollOut {
    /// Poll result event mask
    pub revents: u32,
    /// Padding
    pub padding: u32,
}

/// FUSE notify poll wakeup response `fuse_notify_poll_wakeup_out`
#[cfg(feature = "abi-7-11")]
#[derive(Debug)]
#[repr(C)]
pub struct FuseNotifyPollWakeUpOut {
    /// Wakeup handler
    pub kh: u64,
}

/// FUSE file allocate request input `fuse_fallocate_in`
// #[cfg(feature = "abi-7-19")]
#[derive(Debug)]
#[repr(C)]
pub struct FuseFAllocateIn {
    /// File handler
    pub fh: u64,
    /// File allocate offset
    pub offset: u64,
    /// File allocate length
    pub length: u64,
    /// File mode
    pub mode: u32,
    /// Alignment padding
    pub padding: u32,
}

/// FUSE request header `fuse_in_header`
#[derive(Debug)]
#[repr(C)]
pub struct FuseInHeader {
    /// Request size
    pub len: u32,
    /// FUSE operation code
    pub opcode: u32,
    /// The request unique ID
    pub unique: u64,
    /// The i-number of the node
    pub nodeid: u64,
    /// User ID
    pub uid: u32,
    /// Group ID
    pub gid: u32,
    /// Process ID
    pub pid: u32,
    /// Alignment padding
    pub padding: u32,
}

/// FUSE response header `fuse_out_header`
#[derive(Debug)]
#[repr(C)]
pub struct FuseOutHeader {
    /// Response size
    pub len: u32,
    /// Response error code
    pub error: i32,
    /// The associated request unique ID of this response
    pub unique: u64,
}

/// FUSE directory entry `fuse_dirent`
#[derive(Debug)]
#[repr(C)]
pub struct FuseDirEnt {
    /// The i-number of the entry
    pub ino: INum,
    /// Entry offset in the directory
    pub off: u64,
    /// Entry name length
    pub namelen: u32,
    /// Entry type
    pub typ: u32,
    // Followed by name of namelen bytes
    // char name[],
}

// TODO: re-define it
// #define FUSE_NAME_OFFSET offsetof(struct fuse_dirent, name)
// #define FUSE_DIRENT_ALIGN(x) \
//     (((x) + sizeof(uint64_t) - 1) & ~(sizeof(uint64_t) - 1))
// #define FUSE_DIRENT_SIZE(d) \
//     FUSE_DIRENT_ALIGN(FUSE_NAME_OFFSET + (d)->namelen)

/// FUSE directory entry plus `fuse_direntplus`
/// used in `readdirplus()`
#[cfg(feature = "abi-7-21")]
#[derive(Debug)]
#[repr(C)]
pub struct FuseDirEntPlus {
    /// FUSE directory entry extra info
    pub entry_out: FuseEntryOut,
    /// FUSE directory entry
    pub dirent: FuseDirEnt,
}

// TODO: re-define it
// #[cfg(feature = "abi-7-21")]
// #define FUSE_NAME_OFFSET_DIRENTPLUS \
//     offsetof(struct fuse_direntplus, dirent.name)
// #[cfg(feature = "abi-7-21")]
// #define FUSE_DIRENTPLUS_SIZE(d) \
//     FUSE_DIRENT_ALIGN(FUSE_NAME_OFFSET_DIRENTPLUS + (d)->dirent.namelen)

/// FUSE notify invalid inode response `fuse_notify_inval_inode_out`
#[cfg(feature = "abi-7-12")]
#[derive(Debug)]
#[repr(C)]
pub struct FuseNotifyInvalINodeOut {
    /// Node ID
    pub ino: INum,
    /// Offset
    pub off: i64,
    /// Length
    pub len: i64,
}

/// FUSE notify invalid entry response `fuse_notify_inval_entry_out`
#[cfg(feature = "abi-7-12")]
#[derive(Debug)]
#[repr(C)]
pub struct FuseNotifyInvalEntryOut {
    /// Parent inode
    pub parent: INum,
    /// Name length
    pub namelen: u32,
    /// Padding
    pub padding: u32,
}

/// Fuse notify delete response `fuse_notify_delete_out`
#[cfg(feature = "abi-7-18")]
#[derive(Debug)]
#[repr(C)]
pub struct FuseNotifyDeleteOut {
    /// Parent inode
    pub parent: INum,
    /// Child inode
    pub child: INum,
    /// Name length
    pub namelen: u32,
    /// Padding
    pub padding: u32,
}

/// FUSE notify store response `fuse_notify_store_out`
#[cfg(feature = "abi-7-15")]
#[derive(Debug)]
#[repr(C)]
pub struct FuseNotifyStoreOut {
    /// Node ID
    pub nodeid: INum,
    /// Offset
    pub offset: u64,
    /// Size
    pub size: u32,
    /// Padding
    pub padding: u32,
}

/// FUSE notify retrieve response `fuse_notify_retrieve_out`
#[cfg(feature = "abi-7-15")]
#[derive(Debug)]
#[repr(C)]
pub struct FuseNotifyRetrieveOut {
    /// Unique ID
    pub notify_unique: u64,
    /// Node ID
    pub nodeid: u64,
    /// Offset
    pub offset: u64,
    /// Size
    pub size: u32,
    /// Padding
    pub padding: u32,
}

/// FUSE notify retrieve request input `fuse_notify_retrieve_in`
/// matches the size of `fuse_write_in`
#[cfg(feature = "abi-7-15")]
#[derive(Debug)]
#[repr(C)]
pub struct FuseNotifyRetrieveIn {
    /// Padding
    pub dummy1: u64,
    /// Offset
    pub offset: u64,
    /// Size
    pub size: u32,
    /// Padding
    pub dummy2: u32,
    /// Padding
    pub dummy3: u64,
    /// Padding
    pub dummy4: u64,
}

// TODO: re-define it
// Device ioctls:
// #define FUSE_DEV_IOC_CLONE    _IOR(229, 0, uint32_t)

/// FUSE lseek request input `fuse_lseek_in`
// #[cfg(feature = "abi-7-24")]
#[derive(Debug)]
#[repr(C)]
pub struct FuseLSeekIn {
    /// File handler
    pub fh: u64,
    /// Seek offset
    pub offset: u64,
    /// The directive that tells lseek what the offset is relative to
    pub whence: u32,
    /// Alignment padding
    pub padding: u32,
}

/// FUSE lseek response `fuse_lseek_out`
// #[cfg(feature = "abi-7-24")]
#[derive(Debug)]
#[repr(C)]
pub struct FuseLSeekOut {
    /// Seek offset
    pub offset: u64,
}

/// FUSE copy file range request input `fuse_copy_file_range_in`
// #[cfg(feature = "abi-7-28")]
#[derive(Debug)]
#[repr(C)]
pub struct FuseCopyFileRangeIn {
    /// The file handler of the source file
    pub fh_in: u64,
    /// The starting point from were the data should be read
    pub off_in: u64,
    /// The i-number or the destination file
    pub nodeid_out: u64,
    /// The file handler of the destination file
    pub fh_out: u64,
    /// The starting point where the data should be written
    pub off_out: u64,
    /// The maximum size of the data to copy
    pub len: u64,
    /// The flags passed along with the `copy_file_range()` syscall
    pub flags: u64,
}
