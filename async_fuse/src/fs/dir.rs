use nix::sys::stat::SFlag;
use std::ffi::{CStr, OsStr, OsString};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::RawFd;
use std::ptr;

use super::super::protocol::INum;

#[cfg(target_os = "linux")]
use libc::{dirent64 as dirent, readdir64_r as readdir_r};

#[cfg(not(target_os = "linux"))]
use libc::{dirent, readdir_r};

#[derive(Debug)]
pub(crate) struct Dir(ptr::NonNull<libc::DIR>);

impl Dir {
    /// Converts from a file descriptor, closing it on success or failure.
    pub fn from_fd(fd: RawFd) -> nix::Result<Self> {
        let d = unsafe { libc::fdopendir(fd) };
        if d.is_null() {
            let e = nix::Error::last();
            unsafe { libc::close(fd) };
            return Err(e);
        };
        // Always guaranteed to be non-null by the previous check
        Ok(Dir(ptr::NonNull::new(d).unwrap()))
    }
}

// `Dir` is safe to pass from one thread to another, as it's not reference-counted.
unsafe impl Send for Dir {}

#[derive(Debug)]
pub(crate) struct DirEntry {
    ino: INum,
    entry_type: SFlag,
    name: OsString,
}

impl DirEntry {
    pub fn new(ino: INum, name: OsString, entry_type: SFlag) -> Self {
        Self {
            ino,
            name,
            entry_type,
        }
    }
    /// Returns the inode number (`d_ino`) of the underlying `dirent`.
    pub fn ino(&self) -> u64 {
        self.ino
    }

    /// Returns the bare file name of this directory entry without any other leading path component.
    pub fn entry_name(&self) -> &OsStr {
        self.name.as_os_str()
    }

    /// Returns the type of this directory entry, if known.
    ///
    /// See platform `readdir(3)` or `dirent(5)` manpage for when the file type is known;
    /// notably, some Linux filesystems don't implement this. The caller should use `stat` or
    /// `fstat` if this returns `None`.
    pub fn entry_type(&self) -> SFlag {
        self.entry_type
    }

    fn from_dirent(de: dirent) -> Self {
        let ino = de.d_ino as INum;

        let name = unsafe { OsStr::from_bytes(CStr::from_ptr(de.d_name.as_ptr()).to_bytes()) };

        let entry_type = match de.d_type {
            libc::DT_FIFO => SFlag::S_IFIFO,
            libc::DT_CHR => SFlag::S_IFCHR,
            libc::DT_BLK => SFlag::S_IFBLK,
            libc::DT_DIR => SFlag::S_IFDIR,
            libc::DT_REG => SFlag::S_IFREG,
            libc::DT_LNK => SFlag::S_IFLNK,
            libc::DT_SOCK => SFlag::S_IFSOCK,
            /* libc::DT_UNKNOWN | */ _ => panic!("failed to recognize file type"),
        };

        Self {
            ino,
            name: name.into(),
            entry_type,
        }
    }
}

impl Iterator for Dir {
    type Item = nix::Result<DirEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            // Note: POSIX specifies that portable applications should dynamically allocate a
            // buffer with room for a `d_name` field of size `pathconf(..., _PC_NAME_MAX)` plus 1
            // for the NUL byte. It doesn't look like the std library does this; it just uses
            // fixed-sized buffers (and libc's dirent seems to be sized so this is appropriate).
            // Probably fine here too then.
            let mut ent = std::mem::MaybeUninit::<dirent>::uninit();
            let mut result = ptr::null_mut();
            if let Err(e) =
                nix::errno::Errno::result(readdir_r(self.0.as_ptr(), ent.as_mut_ptr(), &mut result))
            {
                return Some(Err(e));
            }
            if result.is_null() {
                return None;
            }
            assert_eq!(result, ent.as_mut_ptr());
            let dirent = ent.assume_init();
            Some(Ok(DirEntry::from_dirent(dirent)))
        }
    }
}

#[cfg(test)]
mod test {
    use futures::stream::StreamExt;
    use nix::fcntl::{self, OFlag};
    use nix::sys::stat::Mode;
    use smol::blocking;

    use super::Dir;

    #[test]
    fn test_dir() -> nix::Result<()> {
        smol::run(async {
            let oflags = OFlag::O_RDONLY | OFlag::O_DIRECTORY;
            let fd = blocking!(fcntl::open(".", oflags, Mode::empty()))?;
            let dir = Dir::from_fd(fd)?;
            let mut dir = smol::iter(dir);

            while let Some(entry) = dir.next().await {
                let entry = entry?;
                println!(
                    "read file name={:?}, ino={}, type:={:?}",
                    entry.entry_name(),
                    entry.ino(),
                    entry.entry_type()
                );
            }

            Ok(())
        })
    }
}
