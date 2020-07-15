use anyhow::{self, Context};
use log::debug;
use nix::fcntl::{self, OFlag};
use nix::sys::stat::{self, FileStat, Mode, SFlag};
use smol::blocking;
use std::ffi::OsString;
use std::os::unix::io::RawFd;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::super::protocol::{FuseAttr, INum};

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

pub fn parse_oflag(flags: u32) -> OFlag {
    debug_assert!(
        flags < std::i32::MAX as u32,
        "helper_parse_oflag() found flags={} overflow, larger than u16::MAX",
        flags,
    );
    let oflags = OFlag::from_bits_truncate(flags as i32);
    debug!("helper_parse_oflag() read file flags={:?}", oflags);
    oflags
}

pub fn parse_mode(mode: u32) -> Mode {
    debug_assert!(
        mode < std::u16::MAX as u32,
        "helper_parse_mode() found mode={} overflow, larger than u16::MAX",
        mode,
    );

    #[cfg(target_os = "linux")]
    let fmode = Mode::from_bits_truncate(mode);
    #[cfg(target_os = "macos")]
    let fmode = Mode::from_bits_truncate(mode as u16);
    debug!("helper_parse_mode() read file mode={:?}", fmode);
    fmode
}

pub fn parse_mode_bits(mode: u32) -> u16 {
    #[cfg(target_os = "linux")]
    let bits = parse_mode(mode).bits() as u16;
    #[cfg(target_os = "macos")]
    let bits = parse_mode(mode).bits();

    bits
}

pub fn parse_sflag(flags: u32) -> SFlag {
    debug_assert!(
        flags < std::u16::MAX as u32,
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

pub async fn open_dir(path: impl AsRef<Path>) -> nix::Result<RawFd> {
    let oflags = OFlag::O_RDONLY | OFlag::O_DIRECTORY;
    let path = path.as_ref().to_path_buf();
    let dfd = blocking!(fcntl::open(path.as_os_str(), oflags, Mode::empty()))?;
    Ok(dfd)
}

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

pub async fn load_attr(fd: RawFd) -> nix::Result<FileAttr> {
    let st = blocking!(stat::fstat(fd))?;

    #[cfg(target_os = "macos")]
    fn build_crtime(st: &FileStat) -> Option<SystemTime> {
        UNIX_EPOCH.checked_add(Duration::new(
            st.st_birthtime as u64,
            st.st_birthtime_nsec as u32,
        ))
    }
    #[cfg(target_os = "linux")]
    fn build_crtime(_st: &FileStat) -> Option<SystemTime> {
        None
    }

    let atime = UNIX_EPOCH.checked_add(Duration::new(st.st_atime as u64, st.st_atime_nsec as u32));
    let mtime = UNIX_EPOCH.checked_add(Duration::new(st.st_mtime as u64, st.st_mtime_nsec as u32));
    let ctime = UNIX_EPOCH.checked_add(Duration::new(st.st_ctime as u64, st.st_ctime_nsec as u32));
    let crtime = build_crtime(&st);

    let perm = parse_mode_bits(st.st_mode as u32);
    debug!("load_attr() got file permission={}", perm);
    let kind = parse_sflag(st.st_mode as u32);

    let nt = SystemTime::now();
    let attr = FileAttr {
        ino: st.st_ino,
        size: st.st_size as u64,
        blocks: st.st_blocks as u64,
        atime: atime.unwrap_or(nt),
        mtime: mtime.unwrap_or(nt),
        ctime: ctime.unwrap_or(nt),
        crtime: crtime.unwrap_or(nt),
        kind,
        perm,
        nlink: st.st_nlink as u32,
        uid: st.st_uid,
        gid: st.st_gid,
        rdev: st.st_rdev as u32,
        #[cfg(target_os = "linux")]
        flags: 0,
        #[cfg(target_os = "macos")]
        flags: st.st_flags,
    };
    Ok(attr)
}

pub fn time_from_system_time(system_time: &SystemTime) -> anyhow::Result<(u64, u32)> {
    let duration = system_time.duration_since(UNIX_EPOCH).context(format!(
        "failed to convert SystemTime={:?} to Duration",
        system_time
    ))?;
    Ok((duration.as_secs(), duration.subsec_nanos()))
}

pub fn mode_from_kind_and_perm(kind: SFlag, perm: u16) -> u32 {
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

pub fn convert_to_fuse_attr(attr: FileAttr) -> anyhow::Result<FuseAttr> {
    let (atime_secs, atime_nanos) = time_from_system_time(&attr.atime)?;
    let (mtime_secs, mtime_nanos) = time_from_system_time(&attr.mtime)?;
    let (ctime_secs, ctime_nanos) = time_from_system_time(&attr.ctime)?;
    #[cfg(target_os = "macos")]
    let (crtime_secs, crtime_nanos) = time_from_system_time(&attr.crtime)?;

    Ok(FuseAttr {
        ino: attr.ino,
        size: attr.size,
        blocks: attr.blocks,
        atime: atime_secs,
        mtime: mtime_secs,
        ctime: ctime_secs,
        #[cfg(target_os = "macos")]
        crtime: crtime_secs,
        atimensec: atime_nanos,
        mtimensec: mtime_nanos,
        ctimensec: ctime_nanos,
        #[cfg(target_os = "macos")]
        crtimensec: crtime_nanos,
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
