use anyhow::{anyhow, Context, Result};
use nix::{
    fcntl::{self, FcntlArg, FdFlag, OFlag},
    ioctl_read,
    sys::stat::Mode,
    unistd::close,
};
use std::os::unix::io::RawFd;

use super::session::Session;

#[derive(Debug)]
pub(crate) struct Channel {
    // channel fd cloned from session fd
    chan_fd: RawFd,
}

impl Channel {
    #[allow(dead_code)]
    pub async fn new(session: &Session) -> Result<Channel> {
        let devname = "/dev/fuse";
        let clonefd = fcntl::open(devname, OFlag::O_RDWR | OFlag::O_CLOEXEC, Mode::empty());

        let clonefd = match clonefd {
            Err(err) => {
                return Err(anyhow!("fuse: failed to open {:?}: {:?}", devname, err));
            }
            Ok(fd) => fd,
        };

        if let Err(err) = fcntl::fcntl(clonefd, FcntlArg::F_SETFD(FdFlag::FD_CLOEXEC)) {
            return Err(anyhow::anyhow!(
                "fuse: failed to set clonefd to FD_CLOEXEC: {:?}",
                err
            ));
        }

        ioctl_read!(clone, 299, 0, i32);
        let mut masterfd = session.fd();
        let res = unsafe { clone(clonefd, &mut masterfd) };
        if let Err(err) = res {
            close(clonefd).context("fuse: failed to close clone device")?;
            return Err(anyhow::anyhow!(
                "fuse: failed to clone device fd: {:?}\n",
                err
            ));
        }

        Ok(Channel { chan_fd: clonefd })
    }

    #[allow(dead_code)]
    pub fn fd(&self) -> RawFd {
        self.chan_fd
    }
}
