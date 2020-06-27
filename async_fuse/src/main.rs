use anyhow;
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
use fuse_request::*;
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
    debug!("mount point: {:?}", mountpoint);

    smol::run(async move {
        {
            let ss = Session::new(&mountpoint).await?;
            ss.run().await?;
            return Ok(());
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
    use futures::prelude::*;
    use futures::stream::StreamExt;
    use smol::{self, blocking};
    use std::fs::{self, File};
    use std::io::{self, Read};

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
    fn test_buffer() -> io::Result<()> {
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
    #[test]
    fn test_async_iter() -> io::Result<()> {
        smol::run(async move {
            let dir = blocking!(fs::read_dir("."))?;
            let mut dir = smol::iter(dir);
            while let Some(entry) = dir.next().await {
                let path = entry?.path();
                if path.is_file() {
                    println!("read file: {:?}", path);
                    let file = blocking!(File::open(path))?;
                    let mut file = smol::reader(file);
                    let mut buf = vec![];
                    file.read_to_end(&mut buf).await?;
                    let output_length = 16;
                    if buf.len() > output_length {
                        println!("first {} bytes: {:?}", output_length, &buf[..output_length]);
                    } else {
                        println!("total bytes: {:?}", buf);
                    }
                } else {
                    println!("skip directory: {:?}", path);
                }
            }
            Ok(())
        })
    }
}
