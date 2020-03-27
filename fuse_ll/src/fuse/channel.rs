//! FUSE kernel driver communication
//!
//! Raw communication channel to the FUSE kernel driver.

// use libc::{c_void, size_t};
use log::{debug, error};
use nix::sys::uio::{self, IoVec};
use nix::unistd;
use std::ffi::{CString, OsStr, OsString};
use std::io;
use std::os::raw::{c_char, c_int};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use super::mount::{mount, umount};
use super::reply::ReplySender;

#[repr(C)]
#[derive(Debug)]
pub struct fuse_args {
    pub argc: c_int,
    pub argv: *const *const c_char,
    pub allocated: c_int,
}

/// Helper function to provide options as a fuse_args struct
/// (which contains an argc count and an argv pointer)
fn with_fuse_args<T, F: FnOnce(&fuse_args) -> T>(options: &[&OsStr], f: F) -> T {
    let mut args = vec![CString::new("fuse-rs").unwrap()];
    args.extend(options.iter().map(|s| CString::new(s.as_bytes()).unwrap()));
    let argptrs: Vec<_> = args.iter().map(|s| s.as_ptr()).collect();
    f(&fuse_args {
        argc: argptrs.len() as i32,
        argv: argptrs.as_ptr(),
        allocated: 0,
    })
}

/// A raw communication channel to the FUSE kernel driver
#[derive(Debug)]
pub struct Channel {
    mountpoint: OsString,
    fd: c_int,
}

impl Channel {
    /// Create a new communication channel to the kernel driver by mounting the
    /// given path. The kernel driver will delegate filesystem operations of
    /// the given path to the channel. If the channel is dropped, the path is
    /// unmounted.
    pub fn new(mountpoint: OsString, options: &[&OsStr]) -> io::Result<Channel> {
        with_fuse_args(options, |_args| {
            // let mnt = CString::new(mountpoint.as_os_str().as_bytes())?;
            // let fd = unsafe { fuse_mount_compat25(mnt.as_ptr(), args) };
            let fd = mount(mountpoint.as_ref());
            if fd < 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(Channel { mountpoint, fd })
            }
        })
    }

    /// Return path of the mounted filesystem
    pub fn mountpoint(&self) -> &OsStr {
        &self.mountpoint
    }

    /// Receives data up to the capacity of the given buffer (can block).
    pub fn receive(&self, buffer: &mut Vec<u8>) -> io::Result<()> {
        // let rc = unsafe {
        //     libc::read(
        //         self.fd,
        //         buffer.as_ptr() as *mut c_void,
        //         buffer.capacity() as size_t,
        //     )
        // };
        // if rc < 0 {
        //     Err(io::Error::last_os_error())
        // } else {
        //     unsafe {
        //         buffer.set_len(rc as usize);
        //     }
        //     Ok(())
        // }
        unsafe {
            buffer.set_len(buffer.capacity());
        }
        let res = unistd::read(self.fd, &mut *buffer);
        match res {
            Ok(s) => {
                unsafe {
                    buffer.set_len(s as usize);
                }
                debug!("receive successfully {} byte data", s);
                Ok(())
            }
            Err(e) => {
                error!("receive failed, the error is: {:?}", e);
                Err(io::Error::last_os_error())
            }
        }
    }

    /// Returns a sender object for this channel. The sender object can be
    /// used to send to the channel. Multiple sender objects can be used
    /// and they can safely be sent to other threads.
    pub fn sender(&self) -> ChannelSender {
        // Since write/writev syscalls are threadsafe, we can simply create
        // a sender by using the same fd and use it in other threads. Only
        // the channel closes the fd when dropped. If any sender is used after
        // dropping the channel, it'll return an EBADF error.
        ChannelSender { fd: self.fd }
    }
}

impl Drop for Channel {
    fn drop(&mut self) {
        // TODO: send ioctl FUSEDEVIOCSETDAEMONDEAD on macOS before closing the fd
        // Close the communication channel to the kernel driver
        // (closing it before unnmount prevents sync unmount deadlock)
        // unsafe { libc::close(self.fd); }
        let _ = unistd::close(self.fd);
        // Unmount this channel's mount point
        let _ = unmount(self.mountpoint.as_ref());
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ChannelSender {
    fd: c_int,
}

impl ChannelSender {
    /// Send all data in the slice of slice of bytes in a single write (can block).
    pub fn send(&self, buffer: &[&[u8]]) -> io::Result<()> {
        let iovecs: Vec<_> = buffer.iter().map(|d| IoVec::from_slice(d)).collect();
        let res = uio::writev(self.fd, &iovecs);
        match res {
            Ok(s) => {
                debug!("send successfully {} byte data", s);
                Ok(())
            }
            Err(e) => {
                error!("send failed, the error is: {:?}", e);
                Err(io::Error::last_os_error())
            }
        }
    }
}

impl ReplySender for ChannelSender {
    fn send(&self, data: &[&[u8]]) {
        if let Err(err) = ChannelSender::send(self, data) {
            error!("Failed to send FUSE reply: {}", err);
        }
    }
}

/// Unmount an arbitrary mount point
pub fn unmount(mountpoint: &Path) -> io::Result<()> {
    // fuse_unmount_compat22 unfortunately doesn't return a status. Additionally,
    // it attempts to call realpath, which in turn calls into the filesystem. So
    // if the filesystem returns an error, the unmount does not take place, with
    // no indication of the error available to the caller. So we call unmount
    // directly, which is what osxfuse does anyway, since we already converted
    // to the real path when we first mounted.

    let res = umount(mountpoint);
    if res == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
    // #[cfg(any(
    //     target_os = "macos",
    //     target_os = "freebsd",
    //     target_os = "dragonfly",
    //     target_os = "openbsd",
    //     target_os = "bitrig",
    //     target_os = "netbsd"
    // ))]
    // #[inline]
    // fn libc_umount(mountpoint: &Path) -> c_int {
    //     // unsafe { libc::unmount(mnt.as_ptr(), 0) }
    //     umount(mountpoint)
    // }

    // #[cfg(not(any(target_os = "macos", target_os = "freebsd", target_os = "dragonfly",
    //               target_os = "openbsd", target_os = "bitrig", target_os = "netbsd")))]
    // #[inline]
    // fn libc_umount(mountpoint: &Path) -> c_int {
    //     // use fuse_sys::fuse_unmount_compat22;
    //     use std::io::ErrorKind::PermissionDenied;

    //     let mnt = CString::new(mountpoint.as_os_str().as_bytes()).unwrap();
    //     let rc = unsafe { libc::umount(mnt.as_ptr()) };
    //     if rc < 0 && io::Error::last_os_error().kind() == PermissionDenied {
    //         // Linux always returns EPERM for non-root users.  We have to let the
    //         // library go through the setuid-root "fusermount -u" to unmount.
    //         // unsafe { fuse_unmount_compat22(mnt.as_ptr()); }
    //         0
    //     } else {
    //         rc
    //     }
    // }

    // let rc = libc_umount(mountpoint);
    // if rc < 0 {
    //     Err(io::Error::last_os_error())
    // } else {
    //     Ok(())
    // }
}

#[cfg(test)]
mod test {
    use super::with_fuse_args;
    use std::ffi::{CStr, OsStr};

    #[test]
    fn fuse_args() {
        with_fuse_args(&[OsStr::new("foo"), OsStr::new("bar")], |args| {
            assert_eq!(args.argc, 3);
            assert_eq!(
                unsafe { CStr::from_ptr(*args.argv.offset(0)).to_bytes() },
                b"fuse-rs"
            );
            assert_eq!(
                unsafe { CStr::from_ptr(*args.argv.offset(1)).to_bytes() },
                b"foo"
            );
            assert_eq!(
                unsafe { CStr::from_ptr(*args.argv.offset(2)).to_bytes() },
                b"bar"
            );
        });
    }
}
