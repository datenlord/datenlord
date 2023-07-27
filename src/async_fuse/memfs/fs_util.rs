//! The implementation of filesystem related utilities

use std::collections::BTreeMap;
use std::os::unix::io::RawFd;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use clippy_utilities::Cast;
use nix::errno::Errno;
use nix::fcntl::{self, OFlag};
use nix::sys::stat::{self, FileStat, Mode, SFlag};
use tracing::debug;

use super::dir::{Dir, DirEntry};
use crate::async_fuse::fuse::protocol::{FuseAttr, INum};
use crate::async_fuse::util;
use crate::common::error::DatenLordResult;

/// File attributes
#[derive(Copy, Clone, Debug)]
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

impl FileAttr {
    /// New a `FileAttr`
    pub(crate) fn now() -> Self {
        let now = SystemTime::now();
        Self {
            ino: 0,
            size: 4096,
            blocks: 8,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: SFlag::S_IFREG,
            perm: 0o775,
            nlink: 0,
            uid: 0,
            gid: 0,
            rdev: 0,
            flags: 0,
        }
    }

    // ```
    //     File permissions in Unix/Linux systems are represented as a 12-bit structure, laid out as follows:
    //     ┌─────────────┬─────────┬─────────┬─────────┐
    //     │   Special   │  User   │  Group  │  Other  │
    //     ├─────────────┼─────────┼─────────┼─────────┤
    //     │2 Bits       │3 Bits   │3 Bits   │3 Bits   │
    //     ├─────────────┼─────────┼─────────┼─────────┤
    //     │suid |sgid   │r  w  x  │r  w  x  │r  w  x  │
    //     └──────┬──────┴───┬────┴───┬────┴───┬──────┘
    //            │          │        │        │
    //            │          │        │        └─ Other: Read, Write, Execute permissions for users
    //            |          |        |                   other than the owner or members of the group.
    //            │          │        └─ Group: Read, Write, Execute permissions for group members.
    //            │          └─ User: Read, Write, Execute permissions for the owner of the file.
    //            └─ Special: Set User ID (suid) and Set Group ID (sgid).
    //  The suid and sgid bits are beyond the scope of a simple permissions
    //  check and are not considered in this function.
    // ```
    pub fn check_perm(&self, uid: u32, gid: u32, access_mode: u8) -> DatenLordResult<()> {
        debug_assert!(
            access_mode <= 0o7 && access_mode != 0,
            "check_perm() found access_mode={access_mode} invalid",
        );
        if uid == 0 {
            return Ok(());
        }

        let mode = self.get_access_mode(uid, gid);
        debug!(
            "check_perm() got access_mode={access_mode} and mode={mode} from uid={uid} gid={gid}",
            access_mode = access_mode,
            mode = mode,
            uid = uid,
            gid = gid,
        );
        if mode & access_mode != access_mode {
            return util::build_error_result_from_errno(
                Errno::EACCES,
                format!("check_perm() failed {uid} {gid} {mode}"),
            );
        }
        Ok(())
    }

    fn get_access_mode(&self, uid: u32, gid: u32) -> u8 {
        let perm = self.perm;
        let mode = if uid == self.uid {
            (perm >> 6) & 0o7
        } else if gid == self.gid {
            (perm >> 3) & 0o7
        } else {
            perm & 0o7
        };
        mode.cast()
    }
}

impl Default for FileAttr {
    fn default() -> Self {
        Self {
            ino: 0,
            size: 4096,
            blocks: 8,
            atime: SystemTime::UNIX_EPOCH,
            mtime: SystemTime::UNIX_EPOCH,
            ctime: SystemTime::UNIX_EPOCH,
            crtime: SystemTime::UNIX_EPOCH,
            kind: SFlag::S_IFREG,
            perm: 0o775,
            nlink: 0,
            uid: 0,
            gid: 0,
            rdev: 0,
            flags: 0,
        }
    }
}

/// Parse `OFlag`
pub fn parse_oflag(flags: u32) -> OFlag {
    debug_assert!(
        flags < std::i32::MAX.cast::<u32>(),
        "helper_parse_oflag() found flags={flags} overflow, larger than u16::MAX",
    );
    let o_flags = OFlag::from_bits_truncate(flags.cast());
    debug!("helper_parse_oflag() read file flags={:?}", o_flags);
    o_flags
}

/// Parse file mode
pub fn parse_mode(mode: u32) -> Mode {
    debug_assert!(
        mode < std::u16::MAX.cast::<u32>(),
        "helper_parse_mode() found mode={mode} overflow, larger than u16::MAX",
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
        flags < std::u16::MAX.cast::<u32>(),
        "parse_sflag() found flags={flags} overflow, larger than u16::MAX",
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
    let dfd = tokio::task::spawn_blocking(move || {
        fcntl::open(dir_path.as_os_str(), oflags, Mode::empty())
    })
    .await?
    .context(format!("open_dir() failed to open directory={path:?}"))?;
    Ok(dfd)
}

/// Open directory relative to current working directory
pub async fn open_dir_at(dfd: RawFd, child_name: &str) -> anyhow::Result<RawFd> {
    let sub_dir_name = child_name.to_owned();
    let oflags = get_dir_oflags();
    let dir_fd = tokio::task::spawn_blocking(move || {
        fcntl::openat(dfd, sub_dir_name.as_str(), oflags, Mode::empty())
    })
    .await?
    .context(format!(
        "open_dir_at() failed to open sub-directory={child_name:?} under parent fd={dfd}"
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
// ) -> anyhow::Result<FileAttr> { let nix_attr = blocking!(stat::fstatat(
//   symlink_fd, target_path.as_os_str(), fcntl::AtFlags::AT_SYMLINK_FOLLOW ))?;
//   Ok(convert_to_file_attr(nix_attr))
// }

/// Load file attribute by fd
pub async fn load_attr(fd: RawFd) -> anyhow::Result<FileAttr> {
    let st = tokio::task::spawn_blocking(move || stat::fstat(fd))
        .await?
        .context(format!(
            "load_attr() failed get the file attribute of fd={fd}",
        ))?;

    Ok(convert_to_file_attr(st))
}

/// Convert system time to timestamp in seconds and nano-seconds
pub fn time_from_system_time(system_time: &SystemTime) -> (u64, u32) {
    let duration = system_time
        .duration_since(UNIX_EPOCH)
        .context(format!(
            "failed to convert SystemTime={system_time:?} to Duration"
        ))
        .unwrap_or_else(|e| {
            panic!(
                "time_from_system_time() failed to convert SystemTime={:?} \
                to timestamp(seconds, nano-seconds), the error is: {}",
                system_time,
                crate::common::util::format_anyhow_error(&e),
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
        mode: crate::async_fuse::util::mode_from_kind_and_perm(attr.kind, attr.perm),
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

/// Helper function to load directory data
pub async fn load_dir_data(dirfd: RawFd) -> anyhow::Result<BTreeMap<String, DirEntry>> {
    tokio::task::spawn_blocking(move || {
        let dir = Dir::opendirat(dirfd, ".", OFlag::empty())
            .with_context(|| format!("failed to build Dir from fd={dirfd}"))?;
        let mut dir_entry_map = BTreeMap::new();
        for entry in dir {
            let entry = entry?;
            if let SFlag::S_IFDIR | SFlag::S_IFREG | SFlag::S_IFLNK = entry.entry_type() {
                let name = entry.entry_name().to_owned();
                let _map = dir_entry_map.insert(name, entry);
            }
        }
        Ok(dir_entry_map)
    })
    .await?
}

/// Helper function to load file data
pub async fn load_file_data(fd: RawFd, offset: usize, len: usize) -> anyhow::Result<Vec<u8>> {
    let file_data_vec = tokio::task::spawn_blocking(move || {
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
                    "linux pread failed with Error code: {res}"
                )));
            }
            res.cast::<usize>()
        };
        unsafe {
            file_data_vec.set_len(read_size);
        }
        // Should explicitly highlight the error type
        Ok::<Vec<u8>, anyhow::Error>(file_data_vec)
    })
    .await??;
    debug_assert_eq!(file_data_vec.len(), len);
    Ok(file_data_vec)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_permission_check() {
        let file = FileAttr {
            ino: 0,
            size: 0,
            blocks: 0,
            atime: SystemTime::now(),
            mtime: SystemTime::now(),
            ctime: SystemTime::now(),
            crtime: SystemTime::now(),
            kind: SFlag::S_IFREG,
            perm: 0o741,
            nlink: 0,
            uid: 1000,
            gid: 1000,
            rdev: 0,
            flags: 0,
        };

        // Owner permission checks
        assert!(file.check_perm(1000, 0, 7).is_ok());
        assert!(file.check_perm(1000, 0, 6).is_err());
        assert!(file.check_perm(1000, 0, 5).is_err());
        assert!(file.check_perm(1000, 0, 4).is_ok());
        assert!(file.check_perm(1000, 0, 3).is_err());
        assert!(file.check_perm(1000, 0, 2).is_err());
        assert!(file.check_perm(1000, 0, 1).is_err());

        // Group permission checks
        assert!(file.check_perm(0, 1000, 7).is_err());
        assert!(file.check_perm(0, 1000, 6).is_err());
        assert!(file.check_perm(0, 1000, 5).is_err());
        assert!(file.check_perm(0, 1000, 4).is_ok());
        assert!(file.check_perm(0, 1000, 3).is_err());
        assert!(file.check_perm(0, 1000, 2).is_err());
        assert!(file.check_perm(0, 1000, 1).is_err());

        // Other permission checks
        assert!(file.check_perm(0, 0, 7).is_err());
        assert!(file.check_perm(0, 0, 6).is_err());
        assert!(file.check_perm(0, 0, 5).is_err());
        assert!(file.check_perm(0, 0, 4).is_err());
        assert!(file.check_perm(0, 0, 3).is_err());
        assert!(file.check_perm(0, 0, 2).is_err());
        assert!(file.check_perm(0, 0, 1).is_ok());
    }

    // Continue writing more tests for group permissions and other permissions...
}
