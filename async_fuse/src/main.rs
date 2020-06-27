use anyhow;
use futures::prelude::*;
use futures::stream::{StreamExt, TryStreamExt};
use log::{debug, error};
use smol::{self, blocking};

mod fs;
mod fuse_read;
mod fuse_reply;
mod fuse_request;
mod mount;
mod protocal;
mod session;
mod channel;
use fuse_read::*;
use fuse_reply::*;
use fuse_request::*;
use protocal::*;
use session::*;

// #[async_std::main]
fn main() -> anyhow::Result<()> {
    env_logger::init();
    let mountpoint = match std::env::args_os().nth(1) {
        Some(path) => path,
        None => {
            return Err(anyhow::anyhow!(
                "no mount path input, the usage: {} <MOUNTPOINT>",
                std::env::args().nth(0).unwrap(), // safe to use unwrap here
            ));
        }
    };

    smol::run(async move {
        {
            let ss = Session::new(&mountpoint).await?;
            ss.run().await?;
            return Ok(());
        }
        {
            let dir = blocking!(std::fs::read_dir("fuse_reqs"))?;
            let mut dir = smol::iter(dir);
            while let Some(entry) = dir.next().await {
                let path = entry?.path();
                debug!("read file: {:?}", path);
                let mut file = blocking!(std::fs::File::open(path))?;
                let mut file = smol::reader(file);
                let mut buf = vec![];
                file.read_to_end(&mut buf).await?;
                debug!("read bytes: {:?}", buf);
                let fuse_req = Request::new(&*buf);
                debug!("Fuse Req: {:?}", fuse_req);
            }
        }
        {
            let capacity = 1024;
            let dir = blocking!(std::fs::read_dir("fuse_reqs"))?;
            let mut dir = smol::iter(dir);
            dir.try_for_each_concurrent(3, |entry| async move {
                let path = entry.path();
                debug!("read file: {:?}", path);
                let file = blocking!(std::fs::File::open(path))?;
                let file = smol::reader(file);
                FuseBufReadStream::with_capacity(capacity, file)
                    .for_each_concurrent(2, |res| async move {
                        match res {
                            Ok(byte_vec) => {
                                debug!("read {} bytes", byte_vec.len());
                                let req = Request::new(&byte_vec);
                                debug!("build fuse read req: {:?}", req);
                                // Ok::<(), Error>(())
                            }
                            Err(err) => {
                                error!("receive failed, the error is: {:?}", err);
                            }
                        }
                    })
                    .await;
                Ok(())
            })
            .await?;
        }
        {
            let fd = blocking!(nix::fcntl::open(
                "fuse_reply.log",
                nix::fcntl::OFlag::O_CREAT | nix::fcntl::OFlag::O_TRUNC | nix::fcntl::OFlag::O_RDWR,
                nix::sys::stat::Mode::all(),
            )
            .expect("failed to open file"));

            let attr = FuseAttr {
                ino: 64,
                size: 64,
                blocks: 64,
                atime: 64,
                mtime: 64,
                ctime: 64,
                #[cfg(target_os = "macos")]
                crtime: 64,
                atimensec: 32,
                mtimensec: 32,
                ctimensec: 32,
                #[cfg(target_os = "macos")]
                crtimensec: 32,
                mode: 32,
                nlink: 32,
                uid: 32,
                gid: 32,
                rdev: 32,
                #[cfg(target_os = "macos")]
                flags: 32, // see chflags(2)
                #[cfg(feature = "abi-7-9")]
                blksize: 32,
                #[cfg(feature = "abi-7-9")]
                padding: 32,
            };

            let unique = 12345;
            let reply_attr = ReplyAttr::new(unique, fd);
            // let wsize = blocking!(
            //     unistd::write(fd, &[127u8]).expect("failed to write file")
            // );
            // dbg!(wsize);
            use std::time::Duration;
            reply_attr.attr(Duration::from_secs(1), attr).await?;

            let file = blocking!(std::fs::File::open("fuse_reply.log"))?;
            let mut file = smol::reader(file);
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes).await?;

            let mut bs = ByteSlice::new(&bytes);
            let foh: &FuseOutHeader = bs.fetch().unwrap();
            let fa: &FuseAttrOut = bs.fetch().unwrap();

            dbg!(foh, fa);
        }
        {
            let capacity = 1024;
            let dir = blocking!(std::fs::read_dir("fuse_reqs"))?;
            let mut dir = smol::iter(dir);
            dir.try_for_each_concurrent(3, |entry| async move {
                let path = entry.path();
                debug!("read file: {:?}", path);
                let file = blocking!(std::fs::File::open(path))?;
                let file = smol::reader(file);
                FuseBufReadStream::with_capacity(capacity, file)
                    .for_each_concurrent(2, |res| async move {
                        match res {
                            Ok(byte_vec) => {
                                debug!("read {} bytes", byte_vec.len());
                                let req = Request::new(&byte_vec);
                                debug!("build fuse read req={:?}", req);
                                // Ok::<(), Error>(())
                            }
                            Err(err) => {
                                error!("receive failed, the error is: {:?}", err);
                            }
                        }
                    })
                    .await;
                Ok(())
            })
            .await?;
        }
        Ok(())
    })
}

#[cfg(test)]
mod test {
    #[test]
    fn test_ascii() {
        let fs: u8 = 28;
        println!("{} is ASCII {}", fs, fs.is_ascii());
        let a = '♥' as u8;
        println!("{} is ASCII {}", a, a.is_ascii());
        let b = '♥';
        println!("{} is ASCII {}", b, b.is_ascii());
    }

    #[test]
    fn test() -> std::io::Result<()> {
        use std::fs;
        fs::write("bar.txt", "AAAAA")?;
        fs::write("bar.txt", "BBBBB")?;
        Ok(())
    }
    #[test]
    fn test_buffer() -> anyhow::Result<()> {
        use std::fs::{self, File};
        use std::io::Read;
        let mut buffer = Vec::new();
        for entry in fs::read_dir(".")? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                // let fd = nix::fcntl::open(
                //     &entry.path(),
                //     nix::fcntl::OFlag::O_RDONLY,
                //     nix::sys::stat::Mode::all(),
                // )?;
                let mut file = File::open(&entry.path())?;
                file.read_to_end(&mut buffer)?;
                let content = String::from_utf8_lossy(&buffer[..]);
                println!("file content is: {}", content);
            }
        }
        Ok(())
    }
}
