//! FUSE async implementation

#![deny(
    // The following are allowed by default lints according to
    // https://doc.rust-lang.org/rustc/lints/listing/allowed-by-default.html
    anonymous_parameters,
    bare_trait_objects,
    box_pointers,
    elided_lifetimes_in_paths, // allow anonymous lifetime
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs, // TODO: add documents
    single_use_lifetimes, // TODO: fix lifetime names only used once
    trivial_casts, // TODO: remove trivial casts in code
    trivial_numeric_casts,
    // unreachable_pub, allow clippy::redundant_pub_crate lint instead
    // unsafe_code,
    unstable_features,
    unused_extern_crates,
    unused_import_braces,
    unused_qualifications,
    // unused_results, // TODO: fix unused results
    variant_size_differences,

    warnings, // treat all wanings as errors

    clippy::all,
    clippy::restriction,
    clippy::pedantic,
    clippy::nursery,
    clippy::cargo
)]
#![allow(
    // Some explicitly allowed Clippy lints, must have clear reason to allow
    clippy::blanket_clippy_restriction_lints, // allow clippy::restriction
    clippy::implicit_return, // actually omitting the return keyword is idiomatic Rust code
    clippy::module_name_repetitions, // repeation of module name in a struct name is not big deal
    clippy::multiple_crate_versions, // multi-version dependency crates is not able to fix
    clippy::panic, // allow debug_assert, panic in production code
    clippy::panic_in_result_fn, // allow debug_assert, panic in production code
)]

use log::debug;

mod fuse;
mod memfs;
pub mod util;
use fuse::session::Session;

/// Argument name of FUSE mount point
const MOUNT_POINT_ARG_NAME: &str = "mountpoint";

fn main() -> anyhow::Result<()> {
    env_logger::init();

    let matches = clap::App::new("DatenLord")
        .about("Cloud Native Storage")
        .arg(
            clap::Arg::with_name(MOUNT_POINT_ARG_NAME)
                .short("m")
                .long(MOUNT_POINT_ARG_NAME)
                .value_name("MOUNT_DIR")
                .takes_value(true)
                .required(true)
                .help(
                    "Set the mount point of FUSE, \
                        required argument, no default value",
                ),
        )
        .get_matches();
    let mount_point = match matches.value_of(MOUNT_POINT_ARG_NAME) {
        Some(mp) => mp,
        None => panic!("No mount point input"),
    };

    debug!("FUSE mount point: {}", mount_point);

    smol::run(async move {
        let ss = Session::new(std::path::Path::new(&mount_point)).await?;
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
    use log::debug;
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
                    debug!("read file: {:?}", path);
                    let file = blocking!(File::open(path))?;
                    let mut file = smol::reader(file);
                    let mut buf = vec![];
                    file.read_to_end(&mut buf).await?;
                    let output_length = 16;
                    if buf.len() > output_length {
                        debug!(
                            "first {} bytes: {:?}",
                            output_length,
                            &buf.get(..output_length)
                        );
                    } else {
                        debug!("total bytes: {:?}", buf);
                    }
                } else {
                    debug!("skip directory: {:?}", path);
                }
            }
            Ok(())
        })
    }
}
