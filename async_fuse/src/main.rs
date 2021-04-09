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
)]

use log::debug;

mod fuse;
mod memfs;
pub mod proactor;
pub mod util;
use common::error::Context;
use common::etcd_delegate::EtcdDelegate;
use fuse::session::Session;
use std::env;

/// Ip address environment var name
const ENV_IP_ADDRESS: &str = "pod_ip_address";
/// Argument name of FUSE mount point
const MOUNT_POINT_ARG_NAME: &str = "mountpoint";
/// Argument name of FUSE mount point
const CACHE_CAPACITY_ARG_NAME: &str = "capacity";
/// Argument name of ETCD addresses
const ETCD_ADDRESS_ARG_NAME: &str = "etcd";
/// Argument name of Volume information
const VOLUME_INFO_ARG_NAME: &str = "volume_info";
/// ETCD node id lock
const ETCD_NODE_ID_LOCK: &str = "datenlord_etcd_node_id_lock";
/// ETCD node id counter key
const ETCD_NODE_ID_COUNTER_KEY: &str = "datenlord_etcd_node_id_counter";
/// ETCD ndoe id info
const ETCD_NODE_ID_INFO_PREFIX: &str = "datenlord_etcd_node_id_info_";
/// ETCD volume information lock
const ETCD_VOLUME_INFO_LOCK: &str = "datenlord_etcd_volume_info_lock";
/// ETCD volume information prefix
const ETCD_VOLUME_INFO_PREFIX: &str = "datenlord_etcd_volume_info_";

/// The default capacity in bytes, 10GB
const CACHE_DEFAULT_CAPACITY: usize = 10 * 1024 * 1024 * 1024;

/// Register current node to etcd and get a dedicated node id.
/// The registered information contains IP.
async fn register_node_id(etcd_client: &EtcdDelegate) -> anyhow::Result<u64> {
    let lock_key = etcd_client
        .lock(ETCD_NODE_ID_LOCK.as_bytes(), 10)
        .await
        .with_context(|| "lock fail while register node_id")?;

    let node_id: Option<Vec<u8>> = etcd_client
        .get_at_most_one_value(ETCD_NODE_ID_COUNTER_KEY)
        .await
        .with_context(|| format!("get {} from etcd fail", ETCD_NODE_ID_COUNTER_KEY))?;

    let node_id = match node_id {
        Some(current_id_str) => {
            let current_id = std::str::from_utf8(current_id_str.as_slice())?;
            let (new_id, overflow) = current_id
                .parse::<u64>()
                .unwrap_or_else(|_| {
                    panic!("{} can't be parsed as node id (integer)", current_id);
                })
                .overflowing_add(1);
            if overflow {
                panic!("node_id({}) is too large, it overflows", current_id);
            } else {
                new_id
            }
        }
        None => 0,
    };

    let node_id_string = node_id.to_string();

    etcd_client
        .write_or_update_kv(ETCD_NODE_ID_COUNTER_KEY, &node_id_string)
        .await
        .with_context(|| {
            format!(
                "update {} to value {} failed",
                ETCD_NODE_ID_COUNTER_KEY, node_id_string
            )
        })?;

    etcd_client
        .unlock(lock_key)
        .await
        .with_context(|| "unlock fail while register node_id")?;

    let ip = env::var(ENV_IP_ADDRESS).unwrap_or_else(|_| {
        panic!(
            "pod Ip address should be assigned via environment variable: {}!",
            ENV_IP_ADDRESS
        );
    });
    etcd_client
        .write_or_update_kv(
            &format!("{}{}", ETCD_NODE_ID_INFO_PREFIX, node_id_string),
            &ip,
        )
        .await
        .with_context(|| {
            format!(
                "Update Node Ip address failed, node_id:{}, ip: {}",
                node_id_string, ip
            )
        })?;

    Ok(node_id)
}

/// Register volume information, add the volume to `node_id` list mapping
async fn register_volume(
    etcd_client: &EtcdDelegate,
    node_id: u64,
    volume_info: &str,
) -> anyhow::Result<()> {
    let lock_key = etcd_client
        .lock(ETCD_VOLUME_INFO_LOCK.as_bytes(), 10)
        .await
        .with_context(|| "lock fail while register volume")?;

    let volume_info_key = &format!("{}{}", ETCD_VOLUME_INFO_PREFIX, volume_info);
    let volume_node_list: Option<Vec<u8>> = etcd_client
        .get_at_most_one_value(volume_info_key)
        .await
        .with_context(|| format!("get {} from etcd fail", ETCD_NODE_ID_COUNTER_KEY))?;

    let node_id_str = node_id.to_string();
    let volume_node_list = match volume_node_list {
        Some(node_list) => {
            let list_str = std::str::from_utf8(node_list.as_slice())?;
            if list_str.split(',').filter(|s| &node_id_str == s).count() == 0 {
                format!("{},{}", list_str, node_id_str)
            } else {
                node_id_str
            }
        }
        None => node_id_str,
    };

    etcd_client
        .write_or_update_kv(volume_info_key, &volume_node_list)
        .await
        .with_context(|| {
            format!(
                "Update Volume to Node Id mapping failed, volume:{}, node id: {}",
                volume_info, node_id
            )
        })?;

    etcd_client
        .unlock(lock_key)
        .await
        .with_context(|| "unlock fail while register volume")?;
    Ok(())
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
        .get_matches();
    let mount_point = match matches.value_of(MOUNT_POINT_ARG_NAME) {
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

    let volume_info = match matches.value_of(VOLUME_INFO_ARG_NAME) {
        Some(vi) => vi,
        None => panic!("No volume information input"),
    };
    let etcd_delegate = EtcdDelegate::new(etcd_address_vec)?;
    debug!("FUSE mount point: {}", mount_point);

    let cache_capacity = match matches.value_of(CACHE_CAPACITY_ARG_NAME) {
        Some(cc) => cc.parse::<usize>().unwrap_or_else(|_| {
            panic!(format!(
                "cannot parse cache capacity in usize, the input is: {}",
                cc
            ))
        }),
        None => CACHE_DEFAULT_CAPACITY,
    };

    smol::block_on(async move {
        let node_id = register_node_id(&etcd_delegate).await?;
        register_volume(&etcd_delegate, node_id, volume_info).await?;
        let mount_point = std::path::Path::new(&mount_point);
        let fs = memfs::MemFs::new(mount_point, cache_capacity).await?;
        let ss = Session::new(mount_point, fs).await?;
        ss.run().await?;
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
