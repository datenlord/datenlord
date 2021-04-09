//! The implementation of filesystem related utilities

use super::dir::{Dir, DirEntry};
use crate::fuse::protocol::{FuseAttr, INum};

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::os::unix::io::RawFd;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use log::debug;
use nix::fcntl::{self, OFlag};
use nix::sys::stat::{self, FileStat, Mode, SFlag};
use utilities::Cast;

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
    let file_mode = Mode::from_bits_truncate(mode.cast());
    debug!("parse_mode() read mode={:?}", file_mode);
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
    let sflag = SFlag::from_bits_truncate(flags.cast());
    debug!("convert_sflag() read file type={:?}", sflag);
    sflag
}

/// Get directory open flags
pub fn get_dir_oflags() -> OFlag {
    OFlag::O_RDONLY | OFlag::O_DIRECTORY
}

/// Open directory
pub async fn open_dir(path: &Path) -> anyhow::Result<RawFd> {
    let dir_path = path.to_path_buf();
    let oflags = get_dir_oflags();
    let path = path.to_path_buf();
    let dfd = smol::unblock(move || fcntl::open(dir_path.as_os_str(), oflags, Mode::empty()))
        .await
        .context(format!("open_dir() failed to open directory={:?}", path))?;
    Ok(dfd)
}

/// Open directory relative to current working directory
pub async fn open_dir_at(dfd: RawFd, child_name: OsString) -> anyhow::Result<RawFd> {
    let sub_dir_name = child_name.clone();
    let oflags = get_dir_oflags();
    let dir_fd =
        smol::unblock(move || fcntl::openat(dfd, sub_dir_name.as_os_str(), oflags, Mode::empty()))
            .await
            .context(format!(
                "open_dir_at() failed to open sub-directory={:?} under parent fd={}",
                child_name, dfd
            ))?;
    Ok(dir_fd)
}

/// Convert `FileStat` to `FileAttr`
fn convert_to_file_attr(st: FileStat) -> FileAttr {
    /// Build creation timestamp
    #[cfg(target_os = "macos")]
    fn build_crtime(st: &FileStat) -> Option<SystemTime> {
        UNIX_EPOCH.checked_add(Duration::new(
            st.st_birthtime.cast(),
            st.st_birthtime_nsec.cast(),
        ))
    }
    /// Build creation timestamp
    #[cfg(target_os = "linux")]
    const fn build_crtime(_st: &FileStat) -> Option<SystemTime> {
        None
    }

    let a_time = UNIX_EPOCH.checked_add(Duration::new(st.st_atime.cast(), st.st_atime_nsec.cast()));
    let m_time = UNIX_EPOCH.checked_add(Duration::new(st.st_mtime.cast(), st.st_mtime_nsec.cast()));
    let c_time = UNIX_EPOCH.checked_add(Duration::new(st.st_ctime.cast(), st.st_ctime_nsec.cast()));
    let creation_time = build_crtime(&st);

    #[cfg(target_os = "linux")]
    let (perm, kind) = (parse_mode_bits(st.st_mode), parse_sflag(st.st_mode));
    #[cfg(target_os = "macos")]
    let (perm, kind) = (
        parse_mode_bits(st.st_mode.cast()),
        parse_sflag(st.st_mode.cast()),
    );
    debug!(
        "load_attr() got file permission={:#o}={} and kind={:?} from mode bits={:#o}={}",
        perm, perm, kind, st.st_mode, st.st_mode,
    );

    let nt = SystemTime::now();
    FileAttr {
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
    }
}

// /// Load symlink target attribute
// pub async fn load_symlink_target_attr(
//     symlink_fd: RawFd,
//     target_path: PathBuf,
// ) -> anyhow::Result<FileAttr> {
//     let nix_attr = blocking!(stat::fstatat(
//         symlink_fd,
//         target_path.as_os_str(),
//         fcntl::AtFlags::AT_SYMLINK_FOLLOW
//     ))?;
//     Ok(convert_to_file_attr(nix_attr))
// }

/// Load file attribute by fd
pub async fn load_attr(fd: RawFd) -> anyhow::Result<FileAttr> {
    let st = smol::unblock(move || stat::fstat(fd))
        .await
        .context(format!(
            "load_attr() failed get the file attribute of fd={}",
            fd,
        ))?;

    Ok(convert_to_file_attr(st))
}

/// Convert system time to timestamp in seconds and nano-seconds
pub fn time_from_system_time(system_time: &SystemTime) -> (u64, u32) {
    let duration = system_time
        .duration_since(UNIX_EPOCH)
        .context(format!(
            "failed to convert SystemTime={:?} to Duration",
            system_time
        ))
        .unwrap_or_else(|e| {
            panic!(
                "time_from_system_time() failed to convert SystemTime={:?} \
                to timestamp(seconds, nano-seconds), the error is: {}",
                system_time,
                crate::util::format_anyhow_error(&e),
            )
        });
    (duration.as_secs(), duration.subsec_nanos())
}

/// Convert `FileAttr` to `FuseAttr`
pub fn convert_to_fuse_attr(attr: FileAttr) -> FuseAttr {
    let (a_time_secs, a_time_nanos) = time_from_system_time(&attr.atime);
    let (m_time_secs, m_time_nanos) = time_from_system_time(&attr.mtime);
    let (c_time_secs, c_time_nanos) = time_from_system_time(&attr.ctime);
    #[cfg(target_os = "macos")]
    let (creat_time_secs, creat_time_nanos) = time_from_system_time(&attr.crtime);

    FuseAttr {
        ino: attr.ino,
        size: attr.size,
        blocks: attr.blocks,
        atime: a_time_secs,
        mtime: m_time_secs,
        ctime: c_time_secs,
        #[cfg(target_os = "macos")]
        crtime: creat_time_secs,
        atimensec: a_time_nanos,
        mtimensec: m_time_nanos,
        ctimensec: c_time_nanos,
        #[cfg(target_os = "macos")]
        crtimensec: creat_time_nanos,
        mode: crate::util::mode_from_kind_and_perm(attr.kind, attr.perm),
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
    }
}

/// Helper funtion to load directory data
pub async fn load_dir_data(dirfd: RawFd) -> anyhow::Result<BTreeMap<OsString, DirEntry>> {
    smol::unblock(move || {
        let dir = Dir::opendirat(dirfd, ".", OFlag::empty())
            .with_context(|| format!("failed to build Dir from fd={}", dirfd))?;
        let mut dir_entry_map = BTreeMap::new();
        for entry in dir {
            let entry = entry?;
            if let SFlag::S_IFDIR | SFlag::S_IFREG | SFlag::S_IFLNK = entry.entry_type() {
                let name = entry.entry_name().to_owned();
                let _ = dir_entry_map.insert(name, entry);
            }
        }
        Ok(dir_entry_map)
    })
    .await
}

/// Helper function to load file data
pub async fn load_file_data(fd: RawFd, offset: usize, len: usize) -> anyhow::Result<Vec<u8>> {
    let file_data_vec = smol::unblock(move || {
        let mut file_data_vec: Vec<u8> = Vec::with_capacity(len);

        let read_size = unsafe {
            let res = libc::pread(
                fd,
                file_data_vec.as_mut_ptr().cast(),
                len.cast(),
                offset.cast(),
            );

            if res < 0 {
                return Err(anyhow::Error::msg(format!(
                    "linux pread failed with Error code: {}",
                    res
                )));
            } else {
                res.cast::<usize>()
            }
        };
        unsafe {
            file_data_vec.set_len(read_size);
        }
        // Should explicitly highlight the error type
        Ok::<Vec<u8>, anyhow::Error>(file_data_vec)
    })
    .await?;
    debug_assert_eq!(file_data_vec.len(), len);
    Ok(file_data_vec)
}
