//! `DatenLord`

#![deny(
    // The following are allowed by default lints according to
    // https://doc.rust-lang.org/rustc/lints/listing/allowed-by-default.html
    anonymous_parameters,
    bare_trait_objects,
    // box_pointers,
    // elided_lifetimes_in_paths, // allow anonymous lifetime
    // missing_copy_implementations, // Copy may cause unnecessary memory copy
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
    clippy::missing_errors_doc, // TODO: add error docs
    clippy::exhaustive_structs,
    clippy::exhaustive_enums,
    clippy::missing_panics_doc, // TODO: add panic docs
    clippy::panic_in_result_fn,
)]

pub mod async_fuse;
mod common;
mod csi;

use crate::common::etcd_delegate::EtcdDelegate;
use clap::{App, Arg, ArgMatches};
use csi::meta_data::MetaData;
use csi::scheduler_extender::SchdulerExtender;
use csi::util;
use log::debug;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

/// Service port number
const SERVER_PORT_NUM_ARG_NAME: &str = "serverport";
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
/// Argument name of end point
const END_POINT_ARG_NAME: &str = "endpoint";
/// Argument name of worker port
const WORKER_PORT_ARG_NAME: &str = "workerport";
/// Argument name of driver name
const DRIVER_NAME_ARG_NAME: &str = "drivername";
/// Argument name of run as role
const RUN_AS_ARG_NAME: &str = "runas";
/// Argument name of scheduler extender port
const SCHEDULER_EXTENDER_PORT_ARG_NAME: &str = "scheduler-extender-port";

/// The runtime role of `DatenLord`
#[derive(Clone, Copy, Debug)]
pub enum RunAsRole {
    /// Run as controller
    Controller,
    /// Run as node
    Node,
    /// Run as scheduler extender
    SchedulerExtender,
    /// Run async fuse
    AsyncFuse,
}

#[derive(Clone, Debug)]
/// CLI arguments
pub struct CliArgs {
    /// End point
    pub end_point: String,
    /// Worker port
    pub worker_port: u16,
    /// Node ID
    pub node_id: String,
    /// Node IP
    pub ip_address: IpAddr,
    /// Driver name
    pub driver_name: String,
    /// Mount dir
    pub mount_dir: String,
    /// Role name
    pub run_as: RunAsRole,
    /// Etcd address
    pub etcd_address_vec: Vec<String>,
    /// Scheduler extender port
    pub scheduler_extender_port: u16,
    /// Cache capacity
    pub cache_capacity: usize,
    /// Server port
    pub server_port: String,
    /// Volume info
    pub volume_info: String,
    /// Volume type
    pub volume_type: VolumeType,
}

/// Volume type
#[derive(Clone, Copy, Debug)]
pub enum VolumeType {
    /// Do nothing S3 volume
    None,
    /// S3 volume
    S3,
    /// Local volume
    Local,
}

/// Parse command line arguments
#[allow(clippy::too_many_lines)] //allow this for argument parser function as there is no other logic in this function
fn parse_args() -> CliArgs {
    let matches = App::new("DatenLord")
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
            Arg::with_name(END_POINT_ARG_NAME)
                .short("s")
                .long(END_POINT_ARG_NAME)
                .value_name("SOCKET_FILE")
                .takes_value(true)
                .required(true)
                .help(
                    "Set the socket end point of CSI service, \
                        required argument, no default value",
                ),
        )
        .arg(
            Arg::with_name(WORKER_PORT_ARG_NAME)
                .short("p")
                .long(WORKER_PORT_ARG_NAME)
                .value_name("PORT")
                .takes_value(true)
                .required(true)
                .help(
                    "Set the port of worker service port, \
                        no default value",
                ),
        )
        .arg(
            Arg::with_name(NODE_ID_ARG_NAME)
                .short("n")
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
            Arg::with_name(NODE_IP_ARG_NAME)
                .long(NODE_IP_ARG_NAME)
                .value_name("NODE IP")
                .takes_value(true)
                .required(true)
                .help("Set the ip of the node"),
        )
        .arg(
            Arg::with_name(DRIVER_NAME_ARG_NAME)
                .short("d")
                .long(DRIVER_NAME_ARG_NAME)
                .value_name("DRIVER NAME")
                .takes_value(true)
                .help(&format!(
                    "Set the CSI driver name, default as {}",
                    util::CSI_PLUGIN_NAME,
                )),
        )
        .arg(
            Arg::with_name(RUN_AS_ARG_NAME)
                .short("r")
                .long(RUN_AS_ARG_NAME)
                .value_name("ROLE NAME")
                .takes_value(true)
                .help(
                    "Set the runtime service, \
                        set as controller, node, async-fuse or scheduler-extender, \
                        default as node",
                ),
        )
        .arg(
            Arg::with_name(ETCD_ADDRESS_ARG_NAME)
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
            Arg::with_name(SCHEDULER_EXTENDER_PORT_ARG_NAME)
                .long(SCHEDULER_EXTENDER_PORT_ARG_NAME)
                .value_name("SCHEDULER EXTENDER PORT")
                .takes_value(true)
                .help("Set the port of the scheduler extender"),
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
            clap::Arg::with_name(SERVER_PORT_NUM_ARG_NAME)
                .long(SERVER_PORT_NUM_ARG_NAME)
                .value_name("PORT_NUM")
                .takes_value(true)
                .help(&format!(
                    "Set service port number, \
                        required argument, default value is {}",
                    DEFAULT_PORT_NUM
                )),
        )
        .get_matches();
    get_args(&matches)
}

/// Get arguments value
#[allow(clippy::too_many_lines)] //allow for this function as there is no other logic in this function
fn get_args(matches: &ArgMatches) -> CliArgs {
    let end_point = match matches.value_of(END_POINT_ARG_NAME) {
        Some(s) => {
            let sock = s.to_owned();
            if !sock.starts_with("unix:///") {
                panic!(
                    "invalid socket end point: {}, should start with unix:///",
                    sock
                );
            }
            sock
        }
        None => panic!("No valid socket end point"),
    };
    let worker_port = match matches.value_of(WORKER_PORT_ARG_NAME) {
        Some(p) => match p.parse::<u16>() {
            Ok(port) => port,
            Err(e) => panic!("failed to parse port, the error is: {}", e),
        },
        None => panic!("No valid worker port"),
    };
    let node_id = match matches.value_of(NODE_ID_ARG_NAME) {
        Some(n) => n.to_owned(),
        None => panic!("No input node ID"),
    };

    let ip_address = match matches.value_of(NODE_IP_ARG_NAME) {
        Some(n) => n.parse().unwrap_or_else(|_| panic!("Invalid IP address")),
        None => util::get_ip_of_node(&node_id),
    };

    let driver_name = match matches.value_of(DRIVER_NAME_ARG_NAME) {
        Some(d) => d.to_owned(),
        None => util::CSI_PLUGIN_NAME.to_owned(),
    };
    let mount_dir = match matches.value_of(MOUNT_POINT_ARG_NAME) {
        Some(mp) => mp,
        None => panic!("No mount point input"),
    };
    let run_as = match matches.value_of(RUN_AS_ARG_NAME) {
        Some(r) => match r {
            "controller" => RunAsRole::Controller,
            "node" => RunAsRole::Node,
            "scheduler-extender" => RunAsRole::SchedulerExtender,
            "async-fuse" => RunAsRole::AsyncFuse,
            _ => panic!(
                "invalid {} argument {}, must be one of controller, node, scheduler-extender, async-fuse",
                RUN_AS_ARG_NAME, r,
            ),
        },
        None => RunAsRole::Node,
    };
    let etcd_address_vec = match matches.value_of(ETCD_ADDRESS_ARG_NAME) {
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
        None => Vec::new(),
    };
    let scheduler_extender_port = match matches.value_of(SCHEDULER_EXTENDER_PORT_ARG_NAME) {
        Some(p) => match p.parse::<u16>() {
            Ok(port) => port,
            Err(e) => panic!("failed to parse port, the error is: {}", e),
        },
        None => 12345,
    };

    let cache_capacity = match matches.value_of(CACHE_CAPACITY_ARG_NAME) {
        Some(cc) => cc.parse::<usize>().unwrap_or_else(|_| {
            panic!("cannot parse cache capacity in usize, the input is: {}", cc)
        }),
        None => CACHE_DEFAULT_CAPACITY,
    };

    let volume_info = match matches.value_of(VOLUME_INFO_ARG_NAME) {
        Some(vi) => vi,
        None => panic!("No volume information input"),
    };
    let server_port = match matches.value_of(SERVER_PORT_NUM_ARG_NAME) {
        Some(p) => p,
        None => DEFAULT_PORT_NUM,
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
    CliArgs {
        end_point,
        worker_port,
        node_id,
        ip_address,
        driver_name,
        mount_dir: mount_dir.to_owned(),
        run_as,
        etcd_address_vec,
        scheduler_extender_port,
        cache_capacity,
        server_port: server_port.to_owned(),
        volume_info: volume_info.to_owned(),
        volume_type,
    }
}

#[allow(clippy::too_many_lines)]
fn main() -> anyhow::Result<()> {
    env_logger::init();
    let args = parse_args();

    debug!(
        "{}={}, {}={}, {}={}, {}={}, {}={}, {}={}, {}={:?}, {}={:?}, {}={}, {}={}, {}={}, {}={}, {}={:?}",
        END_POINT_ARG_NAME,
        args.end_point,
        WORKER_PORT_ARG_NAME,
        args.worker_port,
        NODE_ID_ARG_NAME,
        args.node_id,
        NODE_IP_ARG_NAME,
        args.ip_address,
        DRIVER_NAME_ARG_NAME,
        args.driver_name,
        MOUNT_POINT_ARG_NAME,
        args.mount_dir,
        RUN_AS_ARG_NAME,
        args.run_as,
        ETCD_ADDRESS_ARG_NAME,
        args.etcd_address_vec,
        SCHEDULER_EXTENDER_PORT_ARG_NAME,
        args.scheduler_extender_port,
        CACHE_CAPACITY_ARG_NAME,
        args.cache_capacity,
        SERVER_PORT_NUM_ARG_NAME,
        args.server_port,
        VOLUME_INFO_ARG_NAME,
        args.volume_info,
        VOLUME_TYPE_ARG_NAME,
        args.volume_type,
    );

    let args_clone = args.clone();
    let etcd_delegate = EtcdDelegate::new(args.etcd_address_vec)?;
    let etcd_delegate_clone = etcd_delegate.clone();
    let meta_data = csi::build_meta_data(
        args.worker_port,
        args.node_id,
        args.ip_address,
        args.mount_dir,
        args.run_as,
        etcd_delegate,
    )?;
    let md = Arc::new(meta_data);
    let async_server = true;

    match args.run_as {
        RunAsRole::Controller => {
            let controller_server = csi::build_grpc_controller_server(
                &args.end_point,
                &args.driver_name,
                Arc::<MetaData>::clone(&md),
            )?;
            csi::run_grpc_servers(&mut [controller_server], async_server);
        }
        RunAsRole::Node => {
            let worker_server = csi::build_grpc_worker_server(Arc::<MetaData>::clone(&md))?;
            let node_server = csi::build_grpc_node_server(&args.end_point, &args.driver_name, md)?;
            let csi_thread = std::thread::spawn(move || {
                csi::run_grpc_servers(&mut [node_server, worker_server], async_server);
            });
            let async_fuse_thread = std::thread::spawn(move || {
                if let Err(e) = async_fuse::start_async_fuse(etcd_delegate_clone, &args_clone) {
                    panic!("failed to start async fuse, error is {:?}", e);
                }
            });
            csi_thread
                .join()
                .unwrap_or_else(|e| panic!("csi thread error: {:?}", e));
            async_fuse_thread
                .join()
                .unwrap_or_else(|e| panic!("csi thread error: {:?}", e));
        }
        RunAsRole::SchedulerExtender => {
            let scheduler_extender = SchdulerExtender::new(
                Arc::<MetaData>::clone(&md),
                SocketAddr::new(args.ip_address, args.scheduler_extender_port),
            );
            let scheduler_extender_thread = std::thread::spawn(move || {
                scheduler_extender.start();
            });
            scheduler_extender_thread
                .join()
                .unwrap_or_else(|e| panic!("scheduler extender error: {:?}", e));
        }
        RunAsRole::AsyncFuse => {
            if let Err(e) = async_fuse::start_async_fuse(etcd_delegate_clone, &args_clone) {
                panic!("failed to start async fuse, error is {:?}", e);
            }
        }
    }

    Ok(())
}
