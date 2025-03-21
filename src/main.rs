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
    clippy::absolute_paths,   // Allow use through absolute paths,like `std::env::current_dir`
    clippy::ref_patterns,    // Allow Some(ref x)
    clippy::single_call_fn,  // Allow function is called only once
    clippy::pub_with_shorthand,  // Allow pub(super)
    clippy::min_ident_chars,  // Allow Err(e)
    clippy::multiple_unsafe_ops_per_block, // Mainly caused by `etcd_delegate`, will remove later
    clippy::impl_trait_in_params,  // Allow impl AsRef<Path>, it's common in Rust
    clippy::missing_assert_message, // Allow assert! without message, mainly in test code
    clippy::semicolon_outside_block, // We need to choose between this and `semicolon_inside_block`, we choose outside
    clippy::similar_names, // Allow similar names, due to the existence of uid and gid
)]

pub mod async_fuse;
mod common;
mod csi;
pub mod distribute_kv_cache;
pub mod fs;
pub mod new_storage;
pub mod storage;

use std::net::SocketAddr;
use std::sync::Arc;

use async_fuse::AsyncFuseArgs;
use clap::Parser;
use csi::meta_data::MetaData;
use csi::scheduler_extender::SchedulerExtender;
use datenlord::{config, metrics};
use fs::kv_engine::{KVEngine, KVEngineType};

use crate::common::error::DatenLordResult;
use crate::common::etcd_delegate::EtcdDelegate;
use crate::common::logger::init_logger;
use crate::common::task_manager::{self, TaskName, TASK_MANAGER};
use crate::config::{InnerConfig, NodeRole, StorageParams};

/// Parse config from command line arguments, and return the created `MetaData`
async fn parse_metadata(config: &InnerConfig) -> DatenLordResult<MetaData> {
    let etcd_delegate = EtcdDelegate::new(config.kv_addrs.clone()).await?;
    let worker_port = config.csi_config.worker_port;
    let node_id = config.node_name.clone();
    let ip_address = config.node_ip;
    let mount_dir = config.mount_path.clone();

    csi::build_meta_data(
        worker_port,
        node_id,
        ip_address,
        mount_dir.clone(),
        config.role,
        etcd_delegate,
    )
    .await
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let arg_conf = config::Config::parse();
    let config = InnerConfig::try_from(config::Config::load_from_args(arg_conf)?)?;

    init_logger(config.role.into(), config.log_level);

    match config.role {
        NodeRole::Node => {
            let metadata = parse_metadata(&config).await?;

            let md = Arc::new(metadata);

            let kv_engine = Arc::new(KVEngineType::new(config.kv_addrs.clone()).await?);
            let node_id = config.node_name.clone();
            let ip_address = config.node_ip;
            let mount_dir = config.mount_path.clone();
            let csi_endpoint = config.csi_config.endpoint.clone();
            let driver_name = config.csi_config.driver_name.clone();

            let worker_server = csi::build_grpc_worker_server(Arc::<MetaData>::clone(&md))?;
            let node_server = csi::build_grpc_node_server(&csi_endpoint, &driver_name, md)?;
            TASK_MANAGER
                .spawn(TaskName::Rpc, |token| {
                    csi::run_grpc_servers(token, vec![worker_server, node_server])
                })
                .await?;

            TASK_MANAGER
                .spawn(TaskName::Metrics, metrics::start_metrics_server)
                .await?;

            let async_args = AsyncFuseArgs {
                node_id,
                ip_address,
                server_port: config.server_port,
                mount_dir: mount_dir.clone(),
                storage_config: config.storage,
                enable_distribute_cache: config.distribute_cache_config.is_some(),
            };

            // Start local distribute cache config

            TASK_MANAGER
                .spawn(TaskName::AsyncFuse, |token| async {
                    if let Err(e) = async_fuse::start_async_fuse(kv_engine, async_args, token).await
                    {
                        panic!("failed to start async fuse, error is {e:?}"); // Panic or Error log?
                    }
                })
                .await?;
        }
        NodeRole::Controller => {
            let metadata = parse_metadata(&config).await?;
            let md = Arc::new(metadata);

            let end_point = config.csi_config.endpoint.clone();
            let driver_name = config.csi_config.driver_name.clone();
            let controller_server = csi::build_grpc_controller_server(
                &end_point,
                &driver_name,
                Arc::<MetaData>::clone(&md),
            )?;
            TASK_MANAGER
                .spawn(TaskName::Rpc, |token| {
                    csi::run_grpc_servers(token, vec![controller_server])
                })
                .await?;
        }
        NodeRole::SchedulerExtender => {
            let metadata = parse_metadata(&config).await?;
            let md = Arc::new(metadata);
            let port = config.scheduler_extender_port;
            let ip_address = config.node_ip;

            let scheduler_extender = SchedulerExtender::new(
                Arc::<MetaData>::clone(&md),
                SocketAddr::new(ip_address, port),
            );
            TASK_MANAGER
                .spawn(TaskName::SchedulerExtender, move |token| {
                    scheduler_extender.start(token)
                })
                .await?;
        }
        NodeRole::AsyncFuse => {
            let kv_engine = Arc::new(KVEngineType::new(config.kv_addrs.clone()).await?);
            let node_id = config.node_name.clone();
            let ip_address = config.node_ip;
            let mount_dir = config.mount_path.clone();
            let async_args = AsyncFuseArgs {
                node_id,
                ip_address,
                server_port: config.server_port,
                mount_dir: mount_dir.clone(),
                storage_config: config.storage,
                enable_distribute_cache: config.distribute_cache_config.is_some(),
            };

            TASK_MANAGER
                .spawn(TaskName::Metrics, metrics::start_metrics_server)
                .await?;

            TASK_MANAGER
                .spawn(TaskName::AsyncFuse, |token| async {
                    if let Err(e) = async_fuse::start_async_fuse(kv_engine, async_args, token).await
                    {
                        panic!("failed to start async fuse, error is {e:?}"); // Panic or Error log?
                    }
                })
                .await?;
        }
        NodeRole::Cache => {
            // TODO: separate the distribute cache task to new node role.
            if let Some(distribute_cache_config) = config.distribute_cache_config.clone() {
                let distribute_cache_config_inner =
                    distribute_kv_cache::config::DistributeCacheConfig::new(
                        distribute_cache_config.bind_ip,
                        distribute_cache_config.bind_port,
                    );
                let kv_engine = Arc::new(KVEngineType::new(config.kv_addrs.clone()).await?);
                match config.storage.params {
                    StorageParams::S3(s3config) => {
                        let distribute_cache_manager =
                            // This type is only used in kvcache.
                            distribute_kv_cache::manager::DistributeCacheManager::<u32>::new(
                                kv_engine,
                                &distribute_cache_config_inner,
                                &s3config,
                            );
                        distribute_cache_manager.start().await?;
                    }
                    StorageParams::Fs(_) => {
                        // Currently only support s3
                        unimplemented!()
                    }
                }
            }
        }
        NodeRole::SDK => {
            panic!("SDK role is not supported yet");
        }
    }

    task_manager::wait_for_shutdown(&TASK_MANAGER)?.await;
    Ok(())
}
