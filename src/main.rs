//! `DatenLord`

#![deny(
    // The following are allowed by default lints according to
    // https://doc.rust-lang.org/rustc/lints/listing/allowed-by-default.html
    anonymous_parameters,
    bare_trait_objects,
    // box_pointers,
    // elided_lifetimes_in_paths, // allow anonymous lifetime
    // missing_copy_implementations, // Copy may cause unnecessary memory copy
    // missing_debug_implementations,
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
use clap::{App, Arg, ArgMatches, SubCommand};
use csi::meta_data::MetaData;
use csi::scheduler_extender::SchdulerExtender;
use csi::util;
use std::collections::HashMap;
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

/// Async fuse args type
#[derive(Debug)]
pub struct AsyncFuseArgs {
    /// Node id
    pub node_id: String,
    /// IP address
    pub ip_address: IpAddr,
    /// Server port
    pub server_port: String,
    /// Volume type
    pub volume_type: VolumeType,
    /// Mount dir
    pub mount_dir: String,
    /// Cache capacity
    pub cache_capacity: usize,
    /// Volume info
    pub volume_info: String,
}

/// Arg for generate default command
#[derive(Debug)]
pub struct ArgParam {
    /// Arg name
    pub name: &'static str,
    /// In short
    pub short: Option<&'static str>,
    /// In long
    pub long: Option<&'static str>,
    /// The value name
    pub value_name: &'static str,
    /// Is take value
    pub take_value: bool,
    /// Is required
    pub required: bool,
    /// The help message
    pub help: &'static str,
}

impl ArgParam {
    /// Get the arg name for index
    pub fn get_name(&self) -> &'static str {
        return self.name;
    }

    /// Generate the arg with short&long or not
    pub fn new_arg<'a>(&self) -> Arg<'a, 'a> {
        match (self.short, self.long) {
            (Some(s), Some(l)) => {
                return Arg::with_name(&self.name)
                .short(s)
                .long(l)
                .value_name(self.value_name)
                .takes_value(self.take_value)
                .required(self.required)
                .help(self.help);
            },
            (Some(s), None) => {
                return Arg::with_name(&self.name)
                .short(s)
                .value_name(self.value_name)
                .takes_value(self.take_value)
                .required(self.required)
                .help(self.help);
            },
            (None, Some(l)) => {
                return Arg::with_name(&self.name)
                .long(l)
                .value_name(self.value_name)
                .takes_value(self.take_value)
                .required(self.required)
                .help(self.help);
            },
            (None, None) => {
                return Arg::with_name(&self.name)
                .value_name(self.value_name)
                .takes_value(self.take_value)
                .required(self.required)
                .help(self.help);
            },
        }
    }
}

/// Generate the default arg
pub fn get_default_arg_map() -> HashMap<&'static str, Arg<'static, 'static>> {
    let vec = vec![
        ArgParam {
            name: MOUNT_POINT_ARG_NAME,
            short: Some("m"),
            long: Some(MOUNT_POINT_ARG_NAME),
            value_name: "MOUNT_DIR",
            take_value: true,
            required: true,
            help: "Set the mount point of FUSE, \
            required argument, no default value",
        },
        ArgParam {
            name: CACHE_CAPACITY_ARG_NAME,
            short: Some("c"),
            long: Some(CACHE_CAPACITY_ARG_NAME),
            value_name: "CACHE_CAPACITY",
            take_value: true,
            required: true,
            help: "Set cache capacity in bytes, \
            required argument, no default value",
        },
        ArgParam {
            name: END_POINT_ARG_NAME,
            short: Some("s"),
            long: Some(END_POINT_ARG_NAME),
            value_name: "SOCKET_FILE",
            take_value: true,
            required: true,
            help: "Set the socket end point of CSI service, \
            required argument, no default value",
        },
        ArgParam {
            name: WORKER_PORT_ARG_NAME,
            short: Some("p"),
            long: Some(WORKER_PORT_ARG_NAME),
            value_name: "PORT",
            take_value: true,
            required: true,
            help: "Set the port of worker service port, \
            no default value",
        },
        ArgParam {
            name: NODE_ID_ARG_NAME,
            short: Some("n"),
            long: Some(NODE_ID_ARG_NAME),
            value_name: "NODE ID",
            take_value: true,
            required: true,
            help: "Set the name of the node, \
            should be a real host name, \
            required argument, no default value",
        },
        ArgParam {
            name: NODE_IP_ARG_NAME,
            short: Some("i"),
            long: Some(NODE_IP_ARG_NAME),
            value_name: "NODE IP",
            take_value: true,
            required: true,
            help: "Set the ip of the node",
        },
        ArgParam {
            name: DRIVER_NAME_ARG_NAME,
            short: Some("d"),
            long: Some(DRIVER_NAME_ARG_NAME),
            value_name: "DRIVER NAME",
            take_value: true,
            required: true,
            help: "Set the CSI driver name, default as io.datenlord.csi.plugin",
        },
        ArgParam {
            name: ETCD_ADDRESS_ARG_NAME,
            short: Some("e"),
            long: Some(ETCD_ADDRESS_ARG_NAME),
            value_name: "ETCD IP:PORT,ETCD IP:PORT",
            take_value: true,
            required: true,
            help: "Set the etcd addresses of format ip:port, \
            if multiple etcd addresses use comma to seperate, \
            required argument, no default value",
        },
        ArgParam {
            name: SCHEDULER_EXTENDER_PORT_ARG_NAME,
            short: Some("S"),
            long: Some(SCHEDULER_EXTENDER_PORT_ARG_NAME),
            value_name: "SCHEDULER EXTENDER PORT",
            take_value: true,
            required: true,
            help: "Set the port of the scheduler extender",
        },
        ArgParam {
            name: VOLUME_INFO_ARG_NAME,
            short: Some("v"),
            long: Some(VOLUME_INFO_ARG_NAME),
            value_name: "VOLUME_INFO",
            take_value: true,
            required: true,
            help: "Set volume backend information, \
            required argument, no default value",
        },
        ArgParam {
            name: VOLUME_TYPE_ARG_NAME,
            short: Some("V"),
            long: Some(VOLUME_TYPE_ARG_NAME),
            value_name: "VOLUME_TYPE",
            take_value: true,
            required: true,
            help: "Set volume backend type, \
            required argument",
        },
        ArgParam {
            name: SERVER_PORT_NUM_ARG_NAME,
            short: Some("P"),
            long: Some(SERVER_PORT_NUM_ARG_NAME),
            value_name: "PORT_NUM",
            take_value: true,
            required: true,
            help: "Set service port number, \
                    required argument, default value is 8089",
        },
    ];
    let m: HashMap<&'static str, Arg<'static, 'static>> =
        vec.iter().map(|s| (s.get_name(), s.new_arg())).collect();
    return m;
}

/// Get endpoint
pub fn get_end_point(matches: &ArgMatches) -> String {
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
    end_point
}

/// get worker port
pub fn get_worker_port(matches: &ArgMatches) -> u16 {
    let worker_port = match matches.value_of(WORKER_PORT_ARG_NAME) {
        Some(p) => match p.parse::<u16>() {
            Ok(port) => port,
            Err(e) => panic!("failed to parse port, the error is: {}", e),
        },
        None => panic!("No valid worker port"),
    };
    worker_port
}

/// Get node id
pub fn get_node_id(matches: &ArgMatches) -> String {
    let node_id = match matches.value_of(NODE_ID_ARG_NAME) {
        Some(n) => n.to_owned(),
        None => panic!("No input node ID"),
    };
    node_id
}

/// Get ip address
pub fn get_ip_address(matches: &ArgMatches, node_id: String) -> IpAddr {
    let ip_address = match matches.value_of(NODE_IP_ARG_NAME) {
        Some(n) => n.parse().unwrap_or_else(|_| panic!("Invalid IP address")),
        None => crate::util::get_ip_of_node(&node_id),
    };
    ip_address
}

/// Get driver name
pub fn get_driver_name(matches: &ArgMatches) -> String {
    let driver_name = match matches.value_of(DRIVER_NAME_ARG_NAME) {
        Some(d) => d.to_owned(),
        None => crate::util::CSI_PLUGIN_NAME.to_owned(),
    };
    driver_name
}

/// Get mount dir
pub fn get_mount_dir<'a>(matches: &'a ArgMatches) -> &'a str {
    let mount_dir = match matches.value_of(MOUNT_POINT_ARG_NAME) {
        Some(mp) => mp,
        None => panic!("No mount point input"),
    };
    mount_dir
}

/// Get role
pub fn get_run_as(role: Option<&str>) -> RunAsRole {
    match role {
        Some("start_csi_controller") => return RunAsRole::Controller,
        Some("start_node") => return RunAsRole::Node,
        Some("start_scheduler_extender") => return RunAsRole::SchedulerExtender,
        Some("start_async_fuse") => return RunAsRole::AsyncFuse,
        _ => panic!(
            "invalid argument, must be one of start_controller, start_node, start_scheduler_extender, start_async_fuse"
        ),
    }
}

/// Get etcd addresses
pub fn get_etcd_address_vec(matches: &ArgMatches) -> Vec<String> {
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
    etcd_address_vec
}

/// Get scheduler port
pub fn get_scheduler_port(matches: &ArgMatches) -> u16 {
    let scheduler_extender_port = match matches.value_of(SCHEDULER_EXTENDER_PORT_ARG_NAME) {
        Some(p) => match p.parse::<u16>() {
            Ok(port) => port,
            Err(e) => panic!("failed to parse port, the error is: {}", e),
        },
        None => 12345,
    };
    scheduler_extender_port
}

/// Get cache capacity
pub fn get_cache_capacity(matches: &ArgMatches) -> usize {
    let cache_capacity = match matches.value_of(CACHE_CAPACITY_ARG_NAME) {
        Some(cc) => cc.parse::<usize>().unwrap_or_else(|_| {
            panic!(
                "{}",
                format!("cannot parse cache capacity in usize, the input is: {}", cc)
            )
        }),
        None => CACHE_DEFAULT_CAPACITY,
    };
    cache_capacity
}

/// Get volume info
pub fn get_volume_info<'a>(matches: &'a ArgMatches) -> &'a str {
    let volume_info = match matches.value_of(VOLUME_INFO_ARG_NAME) {
        Some(vi) => vi,
        None => panic!("No volume information input"),
    };
    volume_info
}

/// Get server port
pub fn get_server_port<'a>(matches: &'a ArgMatches) -> &'a str {
    let server_port = match matches.value_of(SERVER_PORT_NUM_ARG_NAME) {
        Some(p) => p,
        None => DEFAULT_PORT_NUM,
    };
    server_port
}

/// Get volume type
pub fn get_volume_type(matches: &ArgMatches) -> VolumeType {
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
    volume_type
}

/// Generate the metadata
pub fn gen_metadata<'a>(name: &'a str) -> App<'a, 'a> {
    let arg_map = get_default_arg_map();
    SubCommand::with_name(name)
        .arg(arg_map.get(MOUNT_POINT_ARG_NAME).unwrap().clone())
        .arg(arg_map.get(WORKER_PORT_ARG_NAME).unwrap().clone())
        .arg(arg_map.get(NODE_ID_ARG_NAME).unwrap().clone())
        .arg(arg_map.get(NODE_IP_ARG_NAME).unwrap().clone())
        .arg(arg_map.get(ETCD_ADDRESS_ARG_NAME).unwrap().clone())
}

/// Parse the metadata
pub fn parse_metadata(
    matches: &ArgMatches,
    role_name: Option<&str>,
) -> Result<MetaData, common::error::DatenLordError> {
    let etcd_delegate = EtcdDelegate::new(get_etcd_address_vec(matches)).unwrap();
    let worker_port = get_worker_port(matches);
    let node_id = get_node_id(matches);
    let ip_address = get_ip_address(matches, node_id.clone());
    let mount_dir = get_mount_dir(matches);
    let run_as = get_run_as(role_name);

    let metadata = csi::build_meta_data(
        worker_port,
        node_id.clone(),
        ip_address,
        mount_dir.to_string(),
        run_as,
        etcd_delegate,
    );

    return metadata;
}

/// Parse the args in subcommand
fn parse_args() -> ArgMatches<'static> {
    let arg_map = get_default_arg_map();
    let matches = App::new("datenlord")
        .subcommand(
            gen_metadata("start_csi_controller")
                .arg(arg_map.get(DRIVER_NAME_ARG_NAME).unwrap().clone())
                .arg(arg_map.get(END_POINT_ARG_NAME).unwrap().clone()),
        )
        .subcommand(
            gen_metadata("start_node")
                .arg(arg_map.get(DRIVER_NAME_ARG_NAME).unwrap().clone())
                .arg(arg_map.get(END_POINT_ARG_NAME).unwrap().clone())
                // Now gen async fuse data other args
                .arg(arg_map.get(SERVER_PORT_NUM_ARG_NAME).unwrap().clone())
                .arg(arg_map.get(VOLUME_TYPE_ARG_NAME).unwrap().clone())
                .arg(arg_map.get(CACHE_CAPACITY_ARG_NAME).unwrap().clone())
                .arg(arg_map.get(VOLUME_INFO_ARG_NAME).unwrap().clone()),
        )
        .subcommand(
            gen_metadata("start_scheduler_extender").arg(
                arg_map
                    .get(SCHEDULER_EXTENDER_PORT_ARG_NAME)
                    .unwrap()
                    .clone(),
            ),
        )
        .subcommand(
            SubCommand::with_name("async_fuse")
                .arg(arg_map.get(SERVER_PORT_NUM_ARG_NAME).unwrap().clone())
                .arg(arg_map.get(VOLUME_TYPE_ARG_NAME).unwrap().clone())
                .arg(arg_map.get(CACHE_CAPACITY_ARG_NAME).unwrap().clone())
                .arg(arg_map.get(VOLUME_INFO_ARG_NAME).unwrap().clone())
                .arg(arg_map.get(NODE_ID_ARG_NAME).unwrap().clone())
                .arg(arg_map.get(NODE_IP_ARG_NAME).unwrap().clone())
                .arg(arg_map.get(MOUNT_POINT_ARG_NAME).unwrap().clone())
                .arg(arg_map.get(ETCD_ADDRESS_ARG_NAME).unwrap().clone()),
        )
        .get_matches();
    return matches;
}

#[allow(clippy::too_many_lines)]
fn main() -> anyhow::Result<()> {
    env_logger::init();

    let matches = parse_args();
    if let Some(matches) = matches.subcommand_matches("start_csi_controller") {
        let metadata = parse_metadata(matches, Some("start_csi_controller"))?;
        let md = Arc::new(metadata);
        let async_server = true;

        let end_point = get_end_point(matches);
        let driver_name = get_driver_name(matches);
        let controller_server = csi::build_grpc_controller_server(
            &end_point,
            &driver_name,
            Arc::<MetaData>::clone(&md),
        )?;
        csi::run_grpc_servers(&mut [controller_server], async_server);
    }

    if let Some(matches) = matches.subcommand_matches("start_node") {
        let metadata = parse_metadata(matches, Some("start_node"))?;

        let md = Arc::new(metadata);
        let async_server = true;

        let etcd_delegate = EtcdDelegate::new(get_etcd_address_vec(matches))?;
        let node_id = get_node_id(matches);
        let ip_address = get_ip_address(matches, node_id.clone());
        let mount_dir = get_mount_dir(matches);
        let end_point = get_end_point(matches);
        let driver_name = get_driver_name(matches);

        let worker_server = csi::build_grpc_worker_server(Arc::<MetaData>::clone(&md))?;
        let node_server = csi::build_grpc_node_server(&end_point, &driver_name, md)?;
        let csi_thread = std::thread::spawn(move || {
            csi::run_grpc_servers(&mut [node_server, worker_server], async_server);
        });

        let async_args = AsyncFuseArgs {
            node_id: node_id.clone(),
            ip_address,
            server_port: get_server_port(matches).to_string(),
            volume_type: get_volume_type(matches),
            mount_dir: mount_dir.to_string(),
            cache_capacity: get_cache_capacity(matches),
            volume_info: get_volume_info(matches).to_string(),
        };
        let async_fuse_thread = std::thread::spawn(move || {
            if let Err(e) = async_fuse::start_async_fuse(etcd_delegate.clone(), &async_args) {
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

    if let Some(matches) = matches.subcommand_matches("start_scheduler_extender") {
        let metadata = parse_metadata(matches, Some("start_scheduler_extender"))?;
        let md = Arc::new(metadata);
        let sep = get_scheduler_port(matches);
        let node_id = get_node_id(matches);
        let ip_address = get_ip_address(matches, node_id.clone());

        let scheduler_extender = SchdulerExtender::new(
            Arc::<MetaData>::clone(&md),
            SocketAddr::new(ip_address, sep),
        );
        let scheduler_extender_thread = std::thread::spawn(move || {
            scheduler_extender.start();
        });
        scheduler_extender_thread
            .join()
            .unwrap_or_else(|e| panic!("scheduler extender error: {:?}", e));
    }

    if let Some(matches) = matches.subcommand_matches("start_async_fuse") {
        let etcd_delegate = EtcdDelegate::new(get_etcd_address_vec(matches))?;
        let node_id = get_node_id(matches);
        let ip_address = get_ip_address(matches, node_id.clone());
        let mount_dir = get_mount_dir(matches);
        let async_args = AsyncFuseArgs {
            node_id: node_id.clone(),
            ip_address,
            server_port: get_server_port(matches).to_string(),
            volume_type: get_volume_type(matches),
            mount_dir: mount_dir.to_string(),
            cache_capacity: get_cache_capacity(matches),
            volume_info: get_volume_info(matches).to_string(),
        };

        if let Err(e) = async_fuse::start_async_fuse(etcd_delegate, &async_args) {
            panic!("failed to start async fuse, error is {:?}", e);
        }
    }
    Ok(())
}
