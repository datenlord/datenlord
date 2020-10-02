//! The implementation of filesystem related utilities

use anyhow::{self, Context};
use log::debug;
use nix::fcntl::{self, OFlag};
use nix::sys::stat::{self, FileStat, Mode, SFlag};
use smol::blocking;
use std::ffi::OsString;
use std::os::unix::io::RawFd;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use utilities::Cast;

use super::super::protocol::{FuseAttr, INum};

/// File attributes
#[derive(Clone, Copy, Debug)]
pub struct FileAttr {
    /// Inode number
    pub ino: INum,
    /// Size in bytes
    pub size: u64,
    /// Size in blocks
    pub blocks: u64,
    /// Time of last access
    pub atime: SystemTime,
    /// Time of last modification
    pub mtime: SystemTime,
    /// Time of last change
    pub ctime: SystemTime,
    /// Time of creation (macOS only)
    pub crtime: SystemTime,
    /// Kind of file (directory, file, pipe, etc)
    pub kind: SFlag,
    /// Permissions
    pub perm: u16,
    /// Number of hard links
    pub nlink: u32,
    /// User id
    pub uid: u32,
    /// Group id
    pub gid: u32,
    /// Rdev
    pub rdev: u32,
    /// Flags (macOS only, see chflags(2))
    pub flags: u32,
}

/// Parse `OFlag`
pub fn parse_oflag(flags: u32) -> OFlag {
    debug_assert!(
        flags < std::i32::MAX.cast(),
        "helper_parse_oflag() found flags={} overflow, larger than u16::MAX",
        flags,
    );
    let o_flags = OFlag::from_bits_truncate(flags.cast());
    debug!("helper_parse_oflag() read file flags={:?}", o_flags);
    o_flags
}

/// Parse file mode
pub fn parse_mode(mode: u32) -> Mode {
    debug_assert!(
        mode < std::u16::MAX.cast(),
        "helper_parse_mode() found mode={} overflow, larger than u16::MAX",
        mode,
    );

    #[cfg(target_os = "linux")]
    let file_mode = Mode::from_bits_truncate(mode);
    #[cfg(target_os = "macos")]
    let file_mode = Mode::from_bits_truncate(mode as u16);
    debug!("helper_parse_mode() read file mode={:?}", file_mode);
    file_mode
}

/// Parse file mode bits
pub fn parse_mode_bits(mode: u32) -> u16 {
    #[cfg(target_os = "linux")]
    let bits = parse_mode(mode).bits().cast();
    #[cfg(target_os = "macos")]
    let bits = parse_mode(mode).bits();

    bits
}

/// Parse `SFlag`
pub fn parse_sflag(flags: u32) -> SFlag {
    debug_assert!(
        flags < std::u16::MAX.cast(),
        "parse_sflag() found flags={} overflow, larger than u16::MAX",
        flags,
    );

    #[cfg(target_os = "linux")]
    let sflag = SFlag::from_bits_truncate(flags);
    #[cfg(target_os = "macos")]
    let sflag = SFlag::from_bits_truncate(flags as u16);
    debug!("convert_sflag() read file type={:?}", sflag);
    sflag
}

/// Open directory
pub async fn open_dir(path: &Path) -> nix::Result<RawFd> {
    let oflags = OFlag::O_RDONLY | OFlag::O_DIRECTORY;
    let path = path.to_path_buf();
    let dfd = blocking!(fcntl::open(path.as_os_str(), oflags, Mode::empty()))?;
    Ok(dfd)
}

/// Open directory relative to current working directory
pub async fn open_dir_at(dfd: RawFd, child_name: OsString) -> nix::Result<RawFd> {
    let oflags = OFlag::O_RDONLY | OFlag::O_DIRECTORY;
    let dir_fd = blocking!(fcntl::openat(
        dfd,
        child_name.as_os_str(),
        oflags,
        Mode::empty()
    ))?;
    Ok(dir_fd)
}

/// Load file attribute by fd
pub async fn load_attr(fd: RawFd) -> nix::Result<FileAttr> {
    /// Build creation timestamp
    #[cfg(target_os = "macos")]
    const fn build_crtime(st: &FileStat) -> Option<SystemTime> {
        UNIX_EPOCH.checked_add(Duration::new(
            st.st_birthtime as u64,
            st.st_birthtime_nsec as u32,
        ))
    }
    /// Build creation timestamp
    #[cfg(target_os = "linux")]
    const fn build_crtime(_st: &FileStat) -> Option<SystemTime> {
        None
    }

    let st = blocking!(stat::fstat(fd))?;

    let a_time = UNIX_EPOCH.checked_add(Duration::new(st.st_atime.cast(), st.st_atime_nsec.cast()));
    let m_time = UNIX_EPOCH.checked_add(Duration::new(st.st_mtime.cast(), st.st_mtime_nsec.cast()));
    let c_time = UNIX_EPOCH.checked_add(Duration::new(st.st_ctime.cast(), st.st_ctime_nsec.cast()));
    let creation_time = build_crtime(&st);

    let perm = parse_mode_bits(st.st_mode);
    debug!("load_attr() got file permission={}", perm);
    let kind = parse_sflag(st.st_mode);

    let nt = SystemTime::now();
    let attr = FileAttr {
        ino: st.st_ino,
        size: st.st_size.cast(),
        blocks: st.st_blocks.cast(),
        atime: a_time.unwrap_or(nt),
        mtime: m_time.unwrap_or(nt),
        ctime: c_time.unwrap_or(nt),
        crtime: creation_time.unwrap_or(nt),
        kind,
        perm,
        #[cfg(target_arch = "aarch64")]
        nlink: st.st_nlink,
        #[cfg(target_arch = "x86_64")]
        nlink: st.st_nlink.cast(), // TODO: need safe check for u64 to u32
        uid: st.st_uid,
        gid: st.st_gid,
        rdev: st.st_rdev.cast(), // TODO: need safe check for u64 to u32
        #[cfg(target_os = "linux")]
        flags: 0,
        #[cfg(target_os = "macos")]
        flags: st.st_flags,
    };
    Ok(attr)
}

/// Convert system time to timestamp in seconds and nano-seconds
pub fn time_from_system_time(system_time: &SystemTime) -> anyhow::Result<(u64, u32)> {
    let duration = system_time.duration_since(UNIX_EPOCH).context(format!(
        "failed to convert SystemTime={:?} to Duration",
        system_time
    ))?;
    Ok((duration.as_secs(), duration.subsec_nanos()))
}

/// Build file mode from `SFlag` and file permission
pub fn mode_from_kind_and_perm(kind: SFlag, perm: u16) -> u32 {
    let file_type: u32 = match kind {
        SFlag::S_IFIFO => libc::S_IFIFO,
        SFlag::S_IFCHR => libc::S_IFCHR,
        SFlag::S_IFBLK => libc::S_IFBLK,
        SFlag::S_IFDIR => libc::S_IFDIR,
        SFlag::S_IFREG => libc::S_IFREG,
        SFlag::S_IFLNK => libc::S_IFLNK,
        SFlag::S_IFSOCK => libc::S_IFSOCK,
        _ => panic!("unknown SFlag type={:?}", kind),
    };
    let file_perm: u32 = perm.into();
    file_type | file_perm
}

/// Convert `FileAttr` to `FuseAttr`
pub fn convert_to_fuse_attr(attr: FileAttr) -> anyhow::Result<FuseAttr> {
    let (a_time_secs, a_time_nanos) = time_from_system_time(&attr.atime)?;
    let (m_time_secs, m_time_nanos) = time_from_system_time(&attr.mtime)?;
    let (c_time_secs, c_time_nanos) = time_from_system_time(&attr.ctime)?;
    #[cfg(target_os = "macos")]
    let (cr_time_secs, cr_time_nanos) = time_from_system_time(&attr.crtime)?;

    Ok(FuseAttr {
        ino: attr.ino,
        size: attr.size,
        blocks: attr.blocks,
        atime: a_time_secs,
        mtime: m_time_secs,
        ctime: c_time_secs,
        #[cfg(target_os = "macos")]
        crtime: cr_time_secs,
        atimensec: a_time_nanos,
        mtimensec: m_time_nanos,
        ctimensec: c_time_nanos,
        #[cfg(target_os = "macos")]
        crtimensec: cr_time_nanos,
        mode: mode_from_kind_and_perm(attr.kind, attr.perm),
        nlink: attr.nlink,
        uid: attr.uid,
        gid: attr.gid,
        rdev: attr.rdev,
        #[cfg(target_os = "macos")]
        flags: attr.flags,
        #[cfg(feature = "abi-7-9")]
        blksize: 0, // TODO: find a proper way to set block size
        #[cfg(feature = "abi-7-9")]
        padding: 0,
    })
}
