//! FUSE async implementation

#![deny(
    // The following are allowed by default lints according to
    // https://doc.rust-lang.org/rustc/lints/listing/allowed-by-default.html
    anonymous_parameters,
    bare_trait_objects,
    // box_pointers,
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
    // clippy::panic_in_result_fn,
    clippy::unknown_clippy_lints,  // allow rustc and clippy verison mismatch
)]

mod fuse;
mod memfs;
/// Datenlord metrics
pub mod metrics;
pub mod proactor;
pub mod util;

use common::etcd_delegate::EtcdDelegate;
use fuse::session::Session;
use metrics::start_metrics_server;
use std::env;
use log::debug;
use memfs::dist;
use memfs::s3_wrapper::{DoNothingImpl, S3BackEndImpl};
use std::net::IpAddr;

/// Service port number
const PORT_NUM_ARG_NAME: &str = "port";
/// Argument name of FUSE mount point
const MOUNT_POINT_ARG_NAME: &str = "mountpoint";
/// Argument name of FUSE mount point
const CACHE_CAPACITY_ARG_NAME: &str = "capacity";
/// Argument name of ETCD addresses
const ETCD_ADDRESS_ARG_NAME: &str = "etcd";
/// Argument name of Node ID
const NODE_ID_ARG_NAME: &str = "nodeid";
/// Argument name of Node IP
const NODE_IP_ARG_NAME: &str = "nodeip";
/// Argument name of Volume information
const VOLUME_INFO_ARG_NAME: &str = "volume_info";
/// Argument name of Volume type
const VOLUME_TYPE_ARG_NAME: &str = "volume_type";
/// The default capacity in bytes, 10GB
const CACHE_DEFAULT_CAPACITY: usize = 10 * 1024 * 1024 * 1024;
// TODO: Duplicated definition
/// The default service port number
const DEFAULT_PORT_NUM: &str = "8089";

enum VolumeType {
    None,
    S3,
    Local,
}

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
        .arg(
            clap::Arg::with_name(CACHE_CAPACITY_ARG_NAME)
                .short("c")
                .long(CACHE_CAPACITY_ARG_NAME)
                .value_name("CACHE_CAPACITY")
                .takes_value(true)
                .required(false)
                .help(
                    "Set cache capacity in bytes, \
                        required argument, no default value",
                ),
        )
        .arg(
            clap::Arg::with_name(ETCD_ADDRESS_ARG_NAME)
                .short("e")
                .long(ETCD_ADDRESS_ARG_NAME)
                .value_name("ETCD IP:PORT,ETCD IP:PORT")
                .takes_value(true)
                .required(true)
                .help(
                    "Set the etcd addresses of format ip:port, \
                        if multiple etcd addresses use comma to seperate, \
                        required argument, no default value",
                ),
        )
        .arg(
            clap::Arg::with_name(NODE_ID_ARG_NAME)
                .long(NODE_ID_ARG_NAME)
                .value_name("NODE ID")
                .takes_value(true)
                .required(true)
                .help(
                    "Set the name of the node, \
                        should be a real host name, \
                        required argument, no default value",
                ),
        )
        .arg(
            clap::Arg::with_name(NODE_IP_ARG_NAME)
                .long(NODE_IP_ARG_NAME)
                .value_name("NODE IP")
                .takes_value(true)
                .help("Set the ip of the node"),
        )
        .arg(
            clap::Arg::with_name(VOLUME_INFO_ARG_NAME)
                .short("v")
                .long(VOLUME_INFO_ARG_NAME)
                .value_name("VOLUME_INFO")
                .takes_value(true)
                .required(true)
                .help(
                    "Set volume backend information, \
                        required argument, no default value",
                ),
        )
        .arg(
            clap::Arg::with_name(VOLUME_TYPE_ARG_NAME)
                .long(VOLUME_TYPE_ARG_NAME)
                .value_name("VOLUME_TYPE")
                .takes_value(true)
                .help(
                    "Set volume backend type, \
                        required argument",
                ),
        )
        .arg(
            clap::Arg::with_name(PORT_NUM_ARG_NAME)
                .long(PORT_NUM_ARG_NAME)
                .value_name("PORT_NUM")
                .takes_value(true)
                .help(&format!(
                    "Set service port number, \
                        required argument, default value is {}",
                    DEFAULT_PORT_NUM
                )),
        )
        .get_matches();
    let mount_point_str = match matches.value_of(MOUNT_POINT_ARG_NAME) {
        Some(mp) => mp,
        None => panic!("No mount point input"),
    };

    let etcd_address_vec: Vec<String> = match matches.value_of(ETCD_ADDRESS_ARG_NAME) {
        Some(a) => a
            .split(',')
            .map(|address| {
                let etcd_ip_address = match address.strip_prefix("http://") {
                    Some(strip_address) => strip_address,
                    None => address,
                };
                etcd_ip_address.to_owned()
            })
            .collect(),
        None => panic!("etcd addresses must be set, no default value"),
    };

    let node_id = match matches.value_of(NODE_ID_ARG_NAME) {
        Some(n) => n.to_owned(),
        None => panic!("No input node ID"),
    };
    let ip_address: IpAddr = match matches.value_of(NODE_IP_ARG_NAME) {
        Some(n) => n.parse().unwrap_or_else(|_| panic!("Invalid IP address")),
        None => panic!("No input node ip"),
    };
    let node_ip = ip_address.to_string();

    let volume_info = match matches.value_of(VOLUME_INFO_ARG_NAME) {
        Some(vi) => vi,
        None => panic!("No volume information input"),
    };
    let etcd_delegate = EtcdDelegate::new(etcd_address_vec)?;
    debug!("FUSE mount point: {}", mount_point_str);

    let cache_capacity = match matches.value_of(CACHE_CAPACITY_ARG_NAME) {
        Some(cc) => cc.parse::<usize>().unwrap_or_else(|_| {
            panic!(format!(
                "cannot parse cache capacity in usize, the input is: {}",
                cc
            ))
        }),
        None => CACHE_DEFAULT_CAPACITY,
    };

    let volume_type = match matches.value_of(VOLUME_TYPE_ARG_NAME) {
        Some(vt) => {
            if vt == "s3" {
                VolumeType::S3
            } else if vt == "none" {
                VolumeType::None
            } else {
                VolumeType::Local
            }
        }
        None => VolumeType::Local,
    };

    let port = match matches.value_of(PORT_NUM_ARG_NAME) {
        Some(p) => p,
        None => DEFAULT_PORT_NUM,
    };

    start_metrics_server();

    smol::block_on(async move {
        dist::etcd::register_node_id(&etcd_delegate, &node_id, &node_ip.to_string(), port).await?;
        dist::etcd::register_volume(&etcd_delegate, &node_id, volume_info).await?;
        let mount_point = std::path::Path::new(&mount_point_str);
        match volume_type {
            VolumeType::Local => {
                let fs: memfs::MemFs<memfs::DefaultMetaData> = memfs::MemFs::new(
                    mount_point_str,
                    cache_capacity,
                    &node_ip,
                    port,
                    etcd_delegate,
                    &node_id,
                    volume_info,
                )
                .await?;
                let ss = Session::new(mount_point, fs).await?;
                ss.run().await?;
            }
            VolumeType::S3 => {
                let fs: memfs::MemFs<memfs::S3MetaData<S3BackEndImpl>> = memfs::MemFs::new(
                    volume_info,
                    cache_capacity,
                    &node_ip,
                    port,
                    etcd_delegate,
                    &node_id,
                    volume_info,
                )
                .await?;
                let ss = Session::new(mount_point, fs).await?;
                ss.run().await?;
            }
            VolumeType::None => {
                let fs: memfs::MemFs<memfs::S3MetaData<DoNothingImpl>> = memfs::MemFs::new(
                    volume_info,
                    cache_capacity,
                    &node_ip,
                    port,
                    etcd_delegate,
                    &node_id,
                    volume_info,
                )
                .await?;
                let ss = Session::new(mount_point, fs).await?;
                ss.run().await?;
            }
        }
        Ok(())
    })
}

#[cfg(test)]
mod test {
    mod integration_tests;
    mod test_util;

    use log::debug;
    use std::fs;
    use std::io;

    use futures::StreamExt;

    #[test]
    fn test_async_iter() -> io::Result<()> {
        smol::block_on(async move {
            let dir = smol::unblock(|| fs::read_dir(".")).await?;
            let mut dir = smol::stream::iter(dir);
            while let Some(entry) = dir.next().await {
                let path = entry?.path();
                if path.is_file() {
                    debug!("read file: {:?}", path);
                    let buf = smol::fs::read(path).await?;
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
