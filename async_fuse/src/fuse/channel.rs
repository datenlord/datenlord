//! The implementation of FUSE channel

use anyhow::{anyhow, Context};
use nix::{
    fcntl::{self, FcntlArg, FdFlag, OFlag},
    ioctl_read,
    sys::stat::Mode,
    unistd::close,
};
use std::os::unix::io::RawFd;
use utilities::Cast;

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
    pub async fn new(session: &Session) -> anyhow::Result<Self> {
        let devname = "/dev/fuse";
        let clonefd = smol::unblock(move || {
            fcntl::open(devname, OFlag::O_RDWR | OFlag::O_CLOEXEC, Mode::empty())
        })
        .await;

        let clonefd = match clonefd {
            Err(err) => {
                return Err(anyhow!("fuse: failed to open {:?}: {:?}", devname, err));
            }
            Ok(fd) => fd,
        };

        if let Err(err) =
            smol::unblock(move || fcntl::fcntl(clonefd, FcntlArg::F_SETFD(FdFlag::FD_CLOEXEC)))
                .await
        {
            return Err(anyhow!(
                "fuse: failed to set clonefd to FD_CLOEXEC: {:?}",
                err,
            ));
        }

        ioctl_read!(clone, 229, 0, u32);
        let masterfd = session.dev_fd();
        let mut masterfd_u32 = masterfd.cast();
        let res = smol::unblock(move || unsafe { clone(clonefd, &mut masterfd_u32) }).await;
        if let Err(err) = res {
            close(clonefd).context("fuse: failed to close clone device")?;
            return Err(anyhow!("fuse: failed to clone device fd: {:?}", err,));
        }

        Ok(Self { chan_fd: clonefd })
    }

    /// Get channel fd
    #[allow(dead_code)]
    pub const fn fd(&self) -> RawFd {
        self.chan_fd
    }
}
