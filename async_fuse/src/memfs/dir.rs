//! The implementation of directory related functionalities

#[cfg(not(all(target_os = "linux", target_pointer_width = "64")))]
compile_error!("async-fuse does not support this target now");

use crate::util::{clear_errno, cstr_to_bytes, errno, nix_to_io_error, with_c_str};

use std::io;
use std::iter::FusedIterator;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::RawFd;
use std::path::Path;
use std::ptr::NonNull;

use libc::ino_t;
use memchr::memchr;
use nix::fcntl::OFlag;
use nix::sys::stat::{Mode, SFlag};

/// Directory meta-data
pub struct Dir(NonNull<libc::DIR>);

unsafe impl Send for Dir {}
unsafe impl Sync for Dir {}

impl Drop for Dir {
    fn drop(&mut self) {
        let dirp = self.0.as_ptr();
        let ret = unsafe { libc::closedir(dirp) };
        debug_assert_eq!(ret, 0, "failed to closedir: {}", io::Error::last_os_error());
    }
}

impl Dir {
    /// Calls opendir(3)
    /// # Errors
    /// This method will return `io::Error` if the underlying syscalls fails.
    #[allow(dead_code)]
    pub fn opendir<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        with_c_str(path.as_ref().as_os_str().as_bytes(), |p| unsafe {
            let dirname = p.as_ptr();
            let dirp = libc::opendir(dirname);
            if dirp.is_null() {
                return Err(io::Error::last_os_error());
            }
            Ok(Self(NonNull::new_unchecked(dirp)))
        })
    }

    /// Calls fdopendir(3)
    /// # Errors
    /// This method will return `io::Error` if the underlying syscalls fails.
    /// When an error occurs, closes the `fd` and returns the previous error.
    /// # Safety
    /// This function **consumes ownership** of the specified file descriptor.
    /// The returned object will take responsibility for closing it when the object goes out of scope.
    pub unsafe fn fdopendir(fd: RawFd) -> io::Result<Self> {
        let dirp = libc::fdopendir(fd);
        if dirp.is_null() {
            let err = io::Error::last_os_error();
            let _ = libc::close(fd);
            return Err(err);
        }
        Ok(Self(NonNull::new_unchecked(dirp)))
    }

    /// See [`Dir::fdopendir`]
    #[allow(dead_code)]
    pub unsafe fn try_from_raw_fd(fd: RawFd) -> io::Result<Self> {
        Self::fdopendir(fd)
    }

    /// Opens the directory relative to `dirfd`.
    ///
    /// See also <https://github.com/coreutils/gnulib/blob/master/lib/opendirat.c>
    ///
    /// # Errors
    /// This method will return `io::Error` if the underlying syscalls fails.
    pub fn opendirat<P: AsRef<Path>>(
        dirfd: RawFd,
        pathname: P,
        extra_oflag: OFlag,
    ) -> io::Result<Self> {
        let oflag: OFlag =
            OFlag::O_RDONLY | OFlag::O_CLOEXEC | OFlag::O_DIRECTORY | OFlag::O_NOCTTY | extra_oflag;

        let result = nix::fcntl::openat(dirfd, pathname.as_ref(), oflag, Mode::empty());
        let new_fd = result.map_err(nix_to_io_error)?;

        unsafe { Self::fdopendir(new_fd) }
    }
}

impl IntoIterator for Dir {
    type Item = io::Result<DirEntry>;

    type IntoIter = IntoIter;

    /// Returns an iterator over the entries within a directory.
    fn into_iter(self) -> Self::IntoIter {
        IntoIter {
            dir: self,
            end_of_stream: false,
        }
    }
}

/// Directory entry
#[derive(Debug)]
pub struct DirEntry {
    /// The i-number of the entry
    ino: ino_t,
    /// The `SFlag` type of the entry
    entry_type: SFlag,
    /// The entry name
    name: String,
}

impl DirEntry {
    /// Create `DirEntry`
    pub const fn new(ino: ino_t, name: String, entry_type: SFlag) -> Self {
        Self {
            ino,
            name,
            entry_type,
        }
    }
    /// Returns the inode number (`d_ino`) of the underlying `dirent`.
    pub const fn ino(&self) -> ino_t {
        self.ino
    }

    /// Returns the bare file name of this directory entry without any other leading path component.
    pub fn entry_name(&self) -> &str {
        self.name.as_str()
    }

    /// Returns the type of this directory entry, if known.
    ///
    /// See platform `readdir(3)` or `dirent(5)` manpage for when the file type is known;
    /// notably, some Linux filesystems don't implement this. The caller should use `stat` or
    /// `fstat` if this returns `None`.
    pub const fn entry_type(&self) -> SFlag {
        self.entry_type
    }

    /// Build `DirEntry` from `libc::dirent64`
    fn from_dirent(entry: &libc::dirent64) -> Self {
        let ino = entry.d_ino;

        let name_bytes = cstr_to_bytes(&entry.d_name);

        let name = match memchr(0, name_bytes) {
            None => panic!("entry name has no nul byte: {:?}", name_bytes),
            Some(idx) => {
                debug_assert!(idx < 256);
                String::from_utf8(unsafe { name_bytes.get_unchecked(..idx) }.to_vec()).unwrap()
            }
        };

        let entry_type = match entry.d_type {
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
            name,
            entry_type,
        }
    }
}

/// Iterator over the entries in a directory.
pub struct IntoIter {
    /// dir
    dir: Dir,
    /// end flag
    end_of_stream: bool,
}

/// Reads next entry and set "end of stream" flag into `eos`
///
/// See [readdir(3)](https://man7.org/linux/man-pages/man3/readdir.3.html)
///
unsafe fn next_entry(dirp: *mut libc::DIR, eos: &mut bool) -> Option<io::Result<DirEntry>> {
    loop {
        let p_dirent = libc::readdir64(dirp);
        if p_dirent.is_null() {
            *eos = true;
            clear_errno();
            if errno() == 0 {
                return None;
            }
            return Some(Err(io::Error::last_os_error()));
        }
        let dirent = &*p_dirent;
        let name_bytes = cstr_to_bytes(&dirent.d_name);
        match name_bytes {
            [b'.', 0, ..] | [b'.', b'.', 0, ..] => continue,
            _ => {
                *eos = false;
                return Some(Ok(DirEntry::from_dirent(dirent)));
            }
        }
    }
}

impl Iterator for IntoIter {
    type Item = io::Result<DirEntry>;

    fn next(&mut self) -> Option<io::Result<DirEntry>> {
        if self.end_of_stream {
            return None;
        }
        unsafe { next_entry(self.dir.0.as_ptr(), &mut self.end_of_stream) }
    }
}

impl FusedIterator for IntoIter {}

#[cfg(test)]
mod test {
    use super::Dir;

    use std::io;

    use futures::StreamExt;

    #[test]
    fn test_dir() -> io::Result<()> {
        smol::block_on(async {
            let dir = smol::unblock(|| Dir::opendir(".")).await?;
            let mut stream = smol::stream::iter(dir.into_iter());

            while let Some(entry) = stream.next().await {
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
