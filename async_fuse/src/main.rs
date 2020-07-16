#![deny(
    // The following are allowed by default lints according to
    // https://doc.rust-lang.org/rustc/lints/listing/allowed-by-default.html
    anonymous_parameters,
    bare_trait_objects,
    // box_pointers, TODO: fix box pointers
    elided_lifetimes_in_paths,
    missing_copy_implementations,
    missing_debug_implementations,
    // missing_docs, TODO: add documents
    // single_use_lifetimes, TODO: fix lifetime names only used once
    // trivial_casts, TODO: remove trivial casts in code
    trivial_numeric_casts,
    // unreachable_pub, TODO: fix unreachable pub
    // unsafe_code,
    unstable_features,
    unused_extern_crates,
    unused_import_braces,
    unused_qualifications,
    // unused_results, TODO: fix unused results
    variant_size_differences,

    // Treat warnings as errors
    // warnings, TODO: treat all wanings as errors
)]

use log::debug;

mod channel;
mod fs;
mod fuse_read;
mod fuse_reply;
mod fuse_request;
mod mount;
mod protocol;
mod session;
use session::*;

fn main() -> anyhow::Result<()> {
    env_logger::init();
    let mountpoint = match std::env::args_os().nth(1) {
        Some(path) => path,
        None => {
            return Err(anyhow::anyhow!(
                "no mount path input, the usage: {} <MOUNTPOINT>",
                std::env::args().next().unwrap(), // safe to use unwrap here
            ));
        }
    };
    debug!("mount point: {:?}", mountpoint);

    smol::run(async move {
        let ss = Session::new(&mountpoint).await?;
        ss.run().await?;
        Ok(())
    })
}

#[cfg(test)]
mod test {
    mod integration_tests;
    mod test_util;

    use futures::prelude::*;
    use futures::stream::StreamExt;
    use smol::{self, blocking};
    use std::fs::{self, File};
    use std::io;

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
