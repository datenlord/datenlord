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
    clippy::cargo
)]
#![allow(
    // Some explicitly allowed Clippy lints, must have clear reason to allow
    clippy::blanket_clippy_restriction_lints, // allow clippy::restriction
    clippy::implicit_return, // actually omitting the return keyword is idiomatic Rust code
    clippy::module_name_repetitions, // repeation of module name in a struct name is not big deal
    clippy::multiple_crate_versions, // multi-version dependency crates is not able to fix
    clippy::panic, // allow debug_assert, panic in production code
    clippy::unreachable,  // Use `unreachable!` instead of `panic!` when impossible cases occurs
    // clippy::panic_in_result_fn,
    clippy::missing_errors_doc, // TODO: add error docs
    clippy::exhaustive_structs,
    clippy::exhaustive_enums,
    clippy::missing_panics_doc, // TODO: add panic docs
    clippy::panic_in_result_fn,
    clippy::single_char_lifetime_names,
    clippy::separated_literal_suffix, // conflict with unseparated_literal_suffix
    clippy::undocumented_unsafe_blocks, // TODO: add safety comment
    clippy::missing_safety_doc, // TODO: add safety comment
    clippy::shadow_unrelated, //it’s a common pattern in Rust code
    clippy::shadow_reuse, //it’s a common pattern in Rust code
    clippy::shadow_same, //it’s a common pattern in Rust code
    clippy::same_name_method, // Skip for protobuf generated code
    clippy::mod_module_files, // TODO: fix code structure to pass this lint
    clippy::std_instead_of_core, // Cause false positive in src/common/error.rs
    clippy::std_instead_of_alloc,
    clippy::pub_use, // TODO: fix this
    clippy::missing_trait_methods, // TODO: fix this
    clippy::arithmetic_side_effects, // TODO: fix this
    clippy::use_debug, // Allow debug print
    clippy::print_stdout, // Allow println!
    clippy::question_mark_used, // Allow ? operator
    clippy::absolute_paths,
    clippy::ref_patterns,
    clippy::single_call_fn,
    clippy::pub_with_shorthand,
    clippy::min_ident_chars,
    clippy::multiple_unsafe_ops_per_block,
    clippy::impl_trait_in_params,
    clippy::missing_assert_message,
    clippy::bind_instead_of_map,
    clippy::map_clone,
    clippy::let_underscore_untyped,
)]

pub mod async_fuse;
mod common;
mod csi;

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use async_fuse::memfs::kv_engine::{KVEngine, KVEngineType};
use clap::{Arg, ArgMatches, Command};
use csi::meta_data::MetaData;
use csi::scheduler_extender::SchedulerExtender;
use csi::util;

use crate::common::etcd_delegate::EtcdDelegate;
use crate::common::logger::init_logger;

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
/// Start csi controller subcommand
const START_CONTROLLER: &str = "start_csi_controller";
/// Start node subcommand
const START_NODE: &str = "start_node";
/// Start scheduler extender subcommand
const START_SCHEDULER_EXTENDER: &str = "start_scheduler_extender";
/// Start async fuse subcommand
const START_ASYNC_FUSE: &str = "start_async_fuse";

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
}

/// Async fuse args type
#[derive(Debug)]
pub struct AsyncFuseArgs {
    /// Node id
    pub node_id: String,
    /// Node ip
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
#[derive(Debug, Clone)]
struct ArgParam<'a> {
    /// Arg name
    name: &'a str,
    /// In short
    short: Option<char>,
    /// In long
    long: Option<&'a str>,
    /// The value name
    value_name: &'a str,
    /// Is take value
    take_value: bool,
    /// Is required
    required: bool,
    /// The help message
    help: &'a str,
}

impl<'a> ArgParam<'a> {
    /// Get the arg name for index
    #[must_use]
    fn get_name(&self) -> String {
        self.name.to_owned()
    }

    /// Generate the arg with short&long or not
    #[must_use]
    fn new_arg(&self) -> Arg<'a> {
        let self_clone = self.clone();
        let mut arg = Arg::new(self_clone.name)
            .value_name(self_clone.value_name)
            .takes_value(self_clone.take_value)
            .required(self_clone.required)
            .help(self_clone.help);

        if let Some(s) = self_clone.short {
            arg = arg.short(s);
        }

        if let Some(l) = self_clone.long {
            arg = arg.long(l);
        }

        arg
    }
}

/// Generate the default arg
#[allow(clippy::too_many_lines)]
// allow for this function as there is no other logic in this function
#[must_use]
fn get_default_arg_map<'a>() -> HashMap<String, Arg<'a>> {
    let vec = vec![
        ArgParam {
            name: MOUNT_POINT_ARG_NAME,
            short: Some('m'),
            long: Some(MOUNT_POINT_ARG_NAME),
            value_name: "MOUNT_DIR",
            take_value: true,
            required: true,
            help: "Set the mount point of FUSE, \
            required argument, no default value",
        },
        ArgParam {
            name: CACHE_CAPACITY_ARG_NAME,
            short: Some('c'),
            long: Some(CACHE_CAPACITY_ARG_NAME),
            value_name: "CACHE_CAPACITY",
            take_value: true,
            required: true,
            help: "Set cache capacity in bytes, \
            required argument, no default value",
        },
        ArgParam {
            name: END_POINT_ARG_NAME,
            short: Some('s'),
            long: Some(END_POINT_ARG_NAME),
            value_name: "SOCKET_FILE",
            take_value: true,
            required: true,
            help: "Set the socket end point of CSI service, \
            required argument, no default value",
        },
        ArgParam {
            name: WORKER_PORT_ARG_NAME,
            short: Some('p'),
            long: Some(WORKER_PORT_ARG_NAME),
            value_name: "PORT",
            take_value: true,
            required: true,
            help: "Set the port of worker service port, \
            no default value",
        },
        ArgParam {
            name: NODE_ID_ARG_NAME,
            short: Some('n'),
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
            short: Some('i'),
            long: Some(NODE_IP_ARG_NAME),
            value_name: "NODE IP",
            take_value: true,
            required: true,
            help: "Set the ip of the node",
        },
        ArgParam {
            name: DRIVER_NAME_ARG_NAME,
            short: Some('d'),
            long: Some(DRIVER_NAME_ARG_NAME),
            value_name: "DRIVER NAME",
            take_value: true,
            required: true,
            help: "Set the CSI driver name, default as io.datenlord.csi.plugin",
        },
        ArgParam {
            name: ETCD_ADDRESS_ARG_NAME,
            short: Some('e'),
            long: Some(ETCD_ADDRESS_ARG_NAME),
            value_name: "ETCD IP:PORT,ETCD IP:PORT",
            take_value: true,
            required: true,
            help: "Set the etcd addresses of format ip:port, \
            if multiple etcd addresses use comma to separate, \
            required argument, no default value",
        },
        ArgParam {
            name: SCHEDULER_EXTENDER_PORT_ARG_NAME,
            short: Some('S'),
            long: Some(SCHEDULER_EXTENDER_PORT_ARG_NAME),
            value_name: "SCHEDULER EXTENDER PORT",
            take_value: true,
            required: true,
            help: "Set the port of the scheduler extender",
        },
        ArgParam {
            name: VOLUME_INFO_ARG_NAME,
            short: Some('v'),
            long: Some(VOLUME_INFO_ARG_NAME),
            value_name: "VOLUME_INFO",
            take_value: true,
            required: true,
            help: "Set volume backend information, \
            required argument, no default value",
        },
        ArgParam {
            name: VOLUME_TYPE_ARG_NAME,
            short: Some('V'),
            long: Some(VOLUME_TYPE_ARG_NAME),
            value_name: "VOLUME_TYPE",
            take_value: true,
            required: true,
            help: "Set volume backend type, \
            required argument",
        },
        ArgParam {
            name: SERVER_PORT_NUM_ARG_NAME,
            short: Some('P'),
            long: Some(SERVER_PORT_NUM_ARG_NAME),
            value_name: "PORT_NUM",
            take_value: true,
            required: true,
            help: "Set service port number, \
                    required argument, default value is 8089",
        },
    ];
    vec.iter().map(|s| (s.get_name(), s.new_arg())).collect()
}

/// Get endpoint
#[must_use]
fn get_end_point(matches: &ArgMatches) -> String {
    let end_point = match matches.get_one::<String>(END_POINT_ARG_NAME) {
        Some(s) => {
            let sock = s.clone();
            assert!(
                sock.starts_with("unix:///"),
                "invalid socket end point: {sock}, should start with unix:///"
            );
            sock
        }
        None => panic!("No valid socket end point"),
    };
    end_point
}

/// get worker port
#[must_use]
fn get_worker_port(matches: &ArgMatches) -> u16 {
    let worker_port = match matches.get_one::<String>(WORKER_PORT_ARG_NAME) {
        Some(p) => match p.parse::<u16>() {
            Ok(port) => port,
            Err(e) => panic!("failed to parse port, the error is: {e}"),
        },
        None => panic!("No valid worker port"),
    };
    worker_port
}

/// Get node id
#[must_use]
fn get_node_id(matches: &ArgMatches) -> String {
    let node_id = match matches.get_one::<String>(NODE_ID_ARG_NAME) {
        Some(n) => n.clone(),
        None => panic!("No input node ID"),
    };
    node_id
}

/// Get ip address
#[must_use]
fn get_ip_address(matches: &ArgMatches, node_id: &str) -> IpAddr {
    let ip_address = match matches.get_one::<String>(NODE_IP_ARG_NAME) {
        Some(n) => n.parse().unwrap_or_else(|_| panic!("Invalid IP address")),
        None => util::get_ip_of_node(node_id),
    };
    ip_address
}

/// Get driver name
#[must_use]
fn get_driver_name(matches: &ArgMatches) -> String {
    let driver_name = match matches.get_one::<String>(DRIVER_NAME_ARG_NAME) {
        Some(d) => d.clone(),
        None => util::CSI_PLUGIN_NAME.to_owned(),
    };
    driver_name
}

/// Get mount dir
#[must_use]
fn get_mount_dir(matches: &ArgMatches) -> &str {
    let Some(mount_dir) = matches.get_one::<String>(MOUNT_POINT_ARG_NAME) else {
        panic!("No mount point input")
    };
    mount_dir
}

/// Get etcd addresses
#[must_use]
fn get_etcd_address_vec(matches: &ArgMatches) -> Vec<String> {
    let etcd_address_vec = match matches.get_one::<String>(ETCD_ADDRESS_ARG_NAME) {
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
#[must_use]
fn get_scheduler_port(matches: &ArgMatches) -> u16 {
    let scheduler_extender_port = match matches.get_one::<String>(SCHEDULER_EXTENDER_PORT_ARG_NAME)
    {
        Some(p) => match p.parse::<u16>() {
            Ok(port) => port,
            Err(e) => panic!("failed to parse port, the error is: {e}"),
        },
        None => 12345,
    };
    scheduler_extender_port
}

/// Get cache capacity
#[must_use]
fn get_cache_capacity(matches: &ArgMatches) -> usize {
    let cache_capacity = match matches.get_one::<String>(CACHE_CAPACITY_ARG_NAME) {
        Some(cc) => cc
            .parse::<usize>()
            .unwrap_or_else(|_| panic!("cannot parse cache capacity in usize, the input is: {cc}")),
        None => CACHE_DEFAULT_CAPACITY,
    };
    cache_capacity
}

/// Get volume info
#[must_use]
fn get_volume_info(matches: &ArgMatches) -> &str {
    let Some(volume_info) = matches.get_one::<String>(VOLUME_INFO_ARG_NAME) else {
        panic!("No volume information input")
    };
    volume_info
}

/// Get server port
#[must_use]
fn get_server_port(matches: &ArgMatches) -> &str {
    let server_port = match matches.get_one::<String>(SERVER_PORT_NUM_ARG_NAME) {
        Some(p) => p,
        None => DEFAULT_PORT_NUM,
    };
    server_port
}

/// Get volume type
#[must_use]
fn get_volume_type(matches: &ArgMatches) -> VolumeType {
    let volume_type = match matches.get_one::<String>(VOLUME_TYPE_ARG_NAME) {
        Some(vt) => {
            if vt == "s3" {
                VolumeType::S3
            } else if vt == "none" {
                VolumeType::None
            } else {
                panic!("Invalid volume type");
            }
        }
        None => VolumeType::S3,
    };
    volume_type
}

/// Generate the metadata
#[must_use]
fn add_metadata_args(app: Command) -> Command {
    let arg_map = get_default_arg_map();
    app.arg(get_map(&arg_map, MOUNT_POINT_ARG_NAME))
        .arg(get_map(&arg_map, WORKER_PORT_ARG_NAME))
        .arg(get_map(&arg_map, NODE_ID_ARG_NAME))
        .arg(get_map(&arg_map, NODE_IP_ARG_NAME))
        .arg(get_map(&arg_map, ETCD_ADDRESS_ARG_NAME))
}

/// Parse the metadata
async fn parse_metadata(
    matches: &ArgMatches,
    role_name: RunAsRole,
) -> Result<MetaData, common::error::DatenLordError> {
    let etcd_delegate = EtcdDelegate::new(get_etcd_address_vec(matches)).await?;
    let worker_port = get_worker_port(matches);
    let node_id = get_node_id(matches);
    let ip_address = get_ip_address(matches, &node_id);
    let mount_dir = get_mount_dir(matches);

    csi::build_meta_data(
        worker_port,
        node_id,
        ip_address,
        mount_dir.to_owned(),
        role_name,
        etcd_delegate,
    )
    .await
}

/// Get command from default map
fn get_map<'a>(map: &HashMap<String, Arg<'a>>, name: &str) -> Arg<'a> {
    if let Some(res) = map.get(name) {
        res.clone()
    } else {
        panic!("Failed to get default argument {name}");
    }
}

/// Parse the args in subcommand
fn parse_args() -> ArgMatches {
    let arg_map = get_default_arg_map();
    let matches = Command::new("datenlord")
        .arg_required_else_help(true)
        .subcommand(
            add_metadata_args(Command::new(START_CONTROLLER))
                .arg(get_map(&arg_map, DRIVER_NAME_ARG_NAME))
                .arg(get_map(&arg_map, END_POINT_ARG_NAME)),
        )
        .subcommand(
            add_metadata_args(Command::new(START_NODE))
                .arg(get_map(&arg_map, DRIVER_NAME_ARG_NAME))
                .arg(get_map(&arg_map, END_POINT_ARG_NAME))
                .arg(get_map(&arg_map, SERVER_PORT_NUM_ARG_NAME))
                .arg(get_map(&arg_map, VOLUME_TYPE_ARG_NAME))
                .arg(get_map(&arg_map, CACHE_CAPACITY_ARG_NAME))
                .arg(get_map(&arg_map, VOLUME_INFO_ARG_NAME)),
        )
        .subcommand(
            add_metadata_args(Command::new(START_SCHEDULER_EXTENDER))
                .arg(get_map(&arg_map, SCHEDULER_EXTENDER_PORT_ARG_NAME)),
        )
        .subcommand(
            Command::new(START_ASYNC_FUSE)
                .arg(get_map(&arg_map, SERVER_PORT_NUM_ARG_NAME))
                .arg(get_map(&arg_map, VOLUME_TYPE_ARG_NAME))
                .arg(get_map(&arg_map, CACHE_CAPACITY_ARG_NAME))
                .arg(get_map(&arg_map, VOLUME_INFO_ARG_NAME))
                .arg(get_map(&arg_map, NODE_ID_ARG_NAME))
                .arg(get_map(&arg_map, NODE_IP_ARG_NAME))
                .arg(get_map(&arg_map, MOUNT_POINT_ARG_NAME))
                .arg(get_map(&arg_map, ETCD_ADDRESS_ARG_NAME)),
        )
        .get_matches();
    matches
}

#[allow(clippy::too_many_lines)]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logger();

    let matches = parse_args();

    // TODO: The pattern_type_mismatch is false positive
    // issue link: `https://github.com/rust-lang/rust-clippy/issues/7946`
    #[allow(clippy::pattern_type_mismatch)]
    match matches.subcommand() {
        Some((START_CONTROLLER, matches)) => {
            let metadata = parse_metadata(matches, RunAsRole::Controller).await?;
            let md = Arc::new(metadata);

            let end_point = get_end_point(matches);
            let driver_name = get_driver_name(matches);
            let controller_server = csi::build_grpc_controller_server(
                &end_point,
                &driver_name,
                Arc::<MetaData>::clone(&md),
            )?;
            csi::run_grpc_servers(&mut [controller_server]).await;
        }
        Some((START_NODE, matches)) => {
            let metadata = parse_metadata(matches, RunAsRole::Node).await?;

            let md = Arc::new(metadata);

            let etcd_delegate = EtcdDelegate::new(get_etcd_address_vec(matches)).await?;
            let kv_engine = Arc::new(KVEngineType::new(get_etcd_address_vec(matches)).await?);
            let node_id = get_node_id(matches);
            let ip_address = get_ip_address(matches, &node_id);
            let mount_dir = get_mount_dir(matches);
            let end_point = get_end_point(matches);
            let driver_name = get_driver_name(matches);

            let worker_server = csi::build_grpc_worker_server(Arc::<MetaData>::clone(&md))?;
            let node_server = csi::build_grpc_node_server(&end_point, &driver_name, md)?;
            let csi_thread = tokio::task::spawn(async move {
                csi::run_grpc_servers(&mut [node_server, worker_server]).await;
            });

            let async_args = AsyncFuseArgs {
                node_id,
                ip_address,
                server_port: get_server_port(matches).to_owned(),
                volume_type: get_volume_type(matches),
                mount_dir: mount_dir.to_owned(),
                cache_capacity: get_cache_capacity(matches),
                volume_info: get_volume_info(matches).to_owned(),
            };
            let async_fuse_thread = tokio::task::spawn(async move {
                if let Err(e) =
                    async_fuse::start_async_fuse(etcd_delegate.clone(), kv_engine, &async_args)
                        .await
                {
                    panic!("failed to start async fuse, error is {e:?}");
                }
            });

            csi_thread
                .await
                .unwrap_or_else(|e| panic!("csi thread error: {e:?}"));
            async_fuse_thread
                .await
                .unwrap_or_else(|e| panic!("csi thread error: {e:?}"));
        }
        Some((START_SCHEDULER_EXTENDER, matches)) => {
            let metadata = parse_metadata(matches, RunAsRole::SchedulerExtender).await?;
            let md = Arc::new(metadata);
            let port = get_scheduler_port(matches);
            let node_id = get_node_id(matches);
            let ip_address = get_ip_address(matches, &node_id);

            let scheduler_extender = SchedulerExtender::new(
                Arc::<MetaData>::clone(&md),
                SocketAddr::new(ip_address, port),
            );
            scheduler_extender.start().await;
        }
        Some((START_ASYNC_FUSE, matches)) => {
            let etcd_delegate = EtcdDelegate::new(get_etcd_address_vec(matches)).await?;
            let kv_engine = Arc::new(KVEngineType::new(get_etcd_address_vec(matches)).await?);
            let node_id = get_node_id(matches);
            let ip_address = get_ip_address(matches, &node_id);
            let mount_dir = get_mount_dir(matches);
            let async_args = AsyncFuseArgs {
                node_id,
                ip_address,
                server_port: get_server_port(matches).to_owned(),
                volume_type: get_volume_type(matches),
                mount_dir: mount_dir.to_owned(),
                cache_capacity: get_cache_capacity(matches),
                volume_info: get_volume_info(matches).to_owned(),
            };

            if let Err(e) =
                async_fuse::start_async_fuse(etcd_delegate, kv_engine, &async_args).await
            {
                panic!("failed to start async fuse, error is {e:?}");
            }
        }
        _ => panic!("Wrong subcommand!"),
    }
    Ok(())
}
