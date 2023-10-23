//! The implementation of FUSE channel

use std::os::unix::io::RawFd;

use anyhow::{anyhow, Context};
use clippy_utilities::Cast;
use nix::fcntl::{self, FcntlArg, FdFlag, OFlag};
use nix::ioctl_read;
use nix::sys::stat::Mode;
use nix::unistd::close;

use super::file_system::FileSystem;
use super::session::Session;

/// FUSE channel
#[derive(Debug)]
pub struct Channel {
    /// FUSE channel fd cloned from session fd
    chan_fd: RawFd,
}

impl Channel {
    /// Create FUSE channel
    #[allow(dead_code)]
    pub async fn new<F: FileSystem + Send + Sync + 'static>(
        session: &Session<F>,
    ) -> anyhow::Result<Self> {
        let devname = "/dev/fuse";
        let clonefd = tokio::task::spawn_blocking(move || {
            fcntl::open(devname, OFlag::O_RDWR | OFlag::O_CLOEXEC, Mode::empty())
        })
        .await?;

        let clonefd = match clonefd {
            Err(err) => {
                return Err(anyhow!("fuse: failed to open {:?}: {:?}", devname, err));
            }
            Ok(fd) => fd,
        };

        if let Err(err) = tokio::task::spawn_blocking(move || {
            fcntl::fcntl(clonefd, FcntlArg::F_SETFD(FdFlag::FD_CLOEXEC))
        })
        .await?
        {
            return Err(anyhow!(
                "fuse: failed to set clonefd to FD_CLOEXEC: {:?}",
                err,
            ));
        }

        ioctl_read!(clone, 229, 0, u32);
        let masterfd = session.dev_fd();
        let mut masterfd_u32 = masterfd.cast();
        let res = tokio::task::spawn_blocking(move || unsafe { clone(clonefd, &mut masterfd_u32) })
            .await?;
        if let Err(err) = res {
            close(clonefd).context("fuse: failed to close clone device")?;
            return Err(anyhow!("fuse: failed to clone device fd: {:?}", err,));
        }

        Ok(Self { chan_fd: clonefd })
    }

    /// Get channel fd
    #[allow(dead_code)]
    #[must_use]
    pub const fn fd(&self) -> RawFd {
        self.chan_fd
    }
}
