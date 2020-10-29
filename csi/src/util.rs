//! Utility functions and const variables

use anyhow::{anyhow, Context};
use futures::prelude::*;
use grpcio::{RpcContext, RpcStatus, RpcStatusCode, UnarySink};
use log::{debug, error, info};
use nix::mount::{self, MntFlags, MsFlags};
use nix::unistd;
use protobuf::RepeatedField;
use serde::de::DeserializeOwned;
use std::ffi::OsStr;
use std::fmt::Debug;
use std::fs;
use std::net::{IpAddr, ToSocketAddrs};
use std::path::Path;
use std::process::Command;
use utilities::Cast;
use walkdir::WalkDir;

use super::csi::{
    CreateSnapshotRequest, CreateSnapshotResponse, CreateVolumeRequest, CreateVolumeResponse,
    Snapshot, Topology, Volume,
};

/// The CSI plugin name
pub const CSI_PLUGIN_NAME: &str = "io.datenlord.csi.plugin";
// TODO: should use DatenLord version instead
/// The CSI plugin version
pub const CSI_PLUGIN_VERSION: &str = "0.1.0";
/// Directory where data for volumes and snapshots are persisted.
/// This can be ephemeral within the container or persisted if
/// backed by a Pod volume.
pub const DATA_DIR: &str = "/tmp/csi-data-dir";
/// Max storage capacity per volume,
/// Default 20GB required by csi-sanity check
pub const MAX_VOLUME_STORAGE_CAPACITY: i64 = 20 * 1024 * 1024 * 1024;
/// Default ephemeral volume storage capacity,
/// Default same as max storage capacity per volume 20GB
pub const EPHEMERAL_VOLUME_STORAGE_CAPACITY: i64 = MAX_VOLUME_STORAGE_CAPACITY;
/// Extension with which snapshot files will be saved.
pub const SNAPSHOT_EXT: &str = ".snap";
/// The key to the topology hashmap
pub const TOPOLOGY_KEY_NODE: &str = "topology.csi.datenlord.io/node";
/// The key of ephemeral in volume context
pub const EPHEMERAL_KEY_CONTEXT: &str = "csi.storage.k8s.io/ephemeral";
/// Default max volume per node, should read from input argument
pub const MAX_VOLUMES_PER_NODE: i32 = 256;
/// The socket file to be binded by worker service
pub const LOCAL_WORKER_SOCKET: &str = "unix:///tmp/worker.sock";
/// The path to bind mount helper command
pub const BIND_MOUNTER: &str = "target/debug/bind_mounter";

/// The runtime role of CSI plugin
#[derive(Clone, Copy, Debug)]
pub enum RunAsRole {
    /// Run both controller and node service
    Both,
    /// Run controller service only
    Controller,
    /// Run node service only
    Node,
}

/// The bind mount mode of a volume
#[derive(Clone, Copy, Debug)]
pub enum BindMountMode {
    /// Volume bind mount to a pod once
    Single,
    /// Volume bind mount to multiple pods
    Multiple,
    /// Volume remount to a pod
    Remount,
}

/// Format `anyhow::Error`
// TODO: refactor this
pub fn format_anyhow_error(error: &anyhow::Error) -> String {
    let err_msg_vec = error
        .chain()
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>();
    let mut err_msg = err_msg_vec.as_slice().join(", caused by: ");
    err_msg.push_str(&format!(", root cause: {}", error.root_cause()));
    err_msg
}

/// Convert `SystemTime` to proto timestamp
pub fn generate_proto_timestamp(
    st: &std::time::SystemTime,
) -> anyhow::Result<protobuf::well_known_types::Timestamp> {
    let d = st
        .duration_since(std::time::UNIX_EPOCH)
        .context("failed to get duration since unix epoch")?;
    let mut ts = protobuf::well_known_types::Timestamp::new();
    ts.set_seconds(d.as_secs().cast());
    ts.set_nanos(d.subsec_nanos().cast());

    Ok(ts)
}

/// Copy a directory recursively
pub fn copy_directory_recursively(
    from: impl AsRef<Path>,
    to: impl AsRef<Path>,
    follow_symlink: bool,
) -> anyhow::Result<usize> {
    let from_path = from.as_ref();
    let to_path = to.as_ref();
    let mut num_copied: usize = 0;
    for entry in WalkDir::new(from_path).follow_links(follow_symlink) {
        let entry = entry?;
        let entry_path = entry.path();
        let stripped_path = entry_path.strip_prefix(from_path)?;
        let target_path = to_path.join(stripped_path);
        if entry_path.is_dir() {
            if !target_path.exists() {
                fs::create_dir(&target_path)?;
            }
        } else if entry_path.is_file() {
            fs::copy(entry_path, &target_path)?;
            let add_res = num_copied.overflowing_add(1);
            debug_assert!(!add_res.1, "num_copied={} add 1 overflowed", num_copied);
            num_copied = add_res.0;
        } else {
            info!("skip non-file and non-dir path: {}", entry_path.display());
        }
    }

    Ok(num_copied)
}

/// Build `CreateVolumeResponse`
pub fn build_create_volume_response(
    req: &CreateVolumeRequest,
    vol_id: &str,
    node_id: &str,
) -> CreateVolumeResponse {
    let mut topology = Topology::new();
    topology
        .mut_segments()
        .insert(TOPOLOGY_KEY_NODE.to_owned(), node_id.to_owned());
    let mut v = Volume::new();
    v.set_volume_id(vol_id.to_owned());
    v.set_capacity_bytes(req.get_capacity_range().get_required_bytes());
    v.set_volume_context(req.get_parameters().clone());
    v.set_content_source(req.get_volume_content_source().clone());
    v.set_accessible_topology(RepeatedField::from_vec(vec![topology]));
    let mut r = CreateVolumeResponse::new();
    r.set_volume(v);
    r
}

/// Build `CreateSnapshotResponse`
pub fn build_create_snapshot_response(
    req: &CreateSnapshotRequest,
    snap_id: &str,
    ts: &std::time::SystemTime,
    size_bytes: i64,
) -> anyhow::Result<CreateSnapshotResponse> {
    let proto_ts = generate_proto_timestamp(ts).context(format!(
        "failed to generate proto timestamp \
            when creating snapshot from volume ID={}",
        req.get_source_volume_id(),
    ))?;
    let mut s = Snapshot::new();
    s.set_snapshot_id(snap_id.to_owned());
    s.set_source_volume_id(req.get_source_volume_id().to_owned());
    s.set_creation_time(proto_ts);
    s.set_size_bytes(size_bytes);
    s.set_ready_to_use(true);
    let mut r = CreateSnapshotResponse::new();
    r.set_snapshot(s);
    Ok(r)
}

/// Send success `gRPC` response
pub fn success<R>(ctx: &RpcContext, sink: UnarySink<R>, r: R) {
    let f = sink
        .success(r)
        .map_err(move |e| error!("failed to send response, the error is: {:?}", e))
        .map(|_| ());
    ctx.spawn(f)
}

/// Send failure `gRPC` response
pub fn fail<R>(
    ctx: &RpcContext,
    sink: UnarySink<R>,
    rsc: RpcStatusCode,
    anyhow_err: &anyhow::Error,
) {
    debug_assert_ne!(
        rsc,
        RpcStatusCode::OK,
        "the input RpcStatusCode should not be OK"
    );
    let details = format_anyhow_error(anyhow_err);
    let rs = RpcStatus::new(rsc, Some(details));
    let f = sink
        .fail(rs)
        .map_err(move |e| error!("failed to send response, the error is: {:?}", e))
        .map(|_| ());
    ctx.spawn(f)
}

/// Decode from bytes
pub fn decode_from_bytes<T: DeserializeOwned>(bytes: &[u8]) -> anyhow::Result<T> {
    // let decoded_value = bincode::deserialize(bytes).map_err(|e| {
    //     anyhow!(
    //         "failed to decode bytes to {}, the error is: {}",
    //         std::any::type_name::<T>(),
    //         e,
    //     )
    // })?;
    let decoded_value = bincode::deserialize(bytes).context(format!(
        "failed to decode bytes to {}",
        std::any::type_name::<T>(),
    ))?;
    Ok(decoded_value)
}

/// Mount target path, if fail try force un-mount again
pub fn mount_volume_bind_path(
    from: impl AsRef<Path>,
    target: impl AsRef<Path>,
    bind_mount_mode: BindMountMode,
    mount_options: &str,
    fs_type: &str,
    read_only: bool,
) -> anyhow::Result<()> {
    let mut mnt_flags = MsFlags::MS_BIND;
    if read_only {
        mnt_flags |= MsFlags::MS_RDONLY;
    }
    let from_path = from.as_ref();
    let target_path = target.as_ref();
    if unistd::geteuid().is_root() {
        if let BindMountMode::Remount = bind_mount_mode {
            mnt_flags |= MsFlags::MS_REMOUNT;
        }
        mount::mount::<Path, Path, OsStr, OsStr>(
            Some(from_path),
            target_path,
            if fs_type.is_empty() {
                None
            } else {
                Some(OsStr::new(fs_type))
            },
            mnt_flags,
            if mount_options.is_empty() {
                None
            } else {
                Some(OsStr::new(&mount_options))
            },
        )
        .context(format!(
            "failed to direct mount {:?} to {:?}",
            from_path, target_path
        ))
    } else {
        let mut mount_cmd = Command::new(BIND_MOUNTER);
        mount_cmd
            .arg("-f")
            .arg(from_path)
            .arg("-t")
            .arg(&target_path);
        if read_only {
            mount_cmd.arg("-r");
        }
        if let BindMountMode::Remount = bind_mount_mode {
            mount_cmd.arg("-m");
        }
        if !fs_type.is_empty() {
            mount_cmd.arg("-s").arg(&fs_type);
        }
        if !mount_options.is_empty() {
            mount_cmd.arg("-o").arg(&mount_options);
        }
        let mount_handle = match mount_cmd.output() {
            Ok(h) => h,
            Err(e) => {
                return Err(anyhow!(format!(
                    "bind_mounter command failed to start, the error is: {}",
                    e
                ),))
            }
        };
        if mount_handle.status.success() {
            Ok(())
        } else {
            let stderr = String::from_utf8_lossy(&mount_handle.stderr);
            debug!("bind_mounter failed to mount, the error is: {}", &stderr);
            Err(anyhow!(
                "bind_mounter failed to mount {:?} to {:?}, the error is: {}",
                from_path,
                target_path,
                stderr,
            ))
        }
    }
}

/// Un-mount target path, if fail try force un-mount again
pub fn umount_volume_bind_path(target_dir: &str) -> anyhow::Result<()> {
    if unistd::geteuid().is_root() {
        let umount_res = mount::umount(Path::new(target_dir));
        if let Err(umount_e) = umount_res {
            let umount_force_res = mount::umount2(Path::new(target_dir), MntFlags::MNT_FORCE);
            if let Err(umount_force_e) = umount_force_res {
                return Err(anyhow!(
                    "failed to un-mount the target path={:?}, \
                            the un-mount error is: {:?} and the force un-mount error is: {}",
                    Path::new(target_dir),
                    umount_e,
                    umount_force_e,
                ));
            }
        }
    } else {
        let umount_handle = Command::new(BIND_MOUNTER)
            .arg("-u")
            .arg(&target_dir)
            .output()
            .context("bind_mounter command failed to start")?;
        if !umount_handle.status.success() {
            let stderr = String::from_utf8_lossy(&umount_handle.stderr);
            debug!("bind_mounter failed to umount, the error is: {}", &stderr);
            return Err(anyhow!(
                "bind_mounter failed to umount {:?}, the error is: {}",
                Path::new(target_dir),
                stderr,
            ));
        }
    }

    // csi-sanity requires plugin to remove the target mount directory
    fs::remove_dir_all(target_dir)
        .context(format!("failed to remove mount target path={}", target_dir))?;

    Ok(())
}

/// Get ip address of node
pub fn get_ip_of_node(node_id: &str) -> IpAddr {
    let hostname = format!("{}:{}", node_id, 0);
    let sockets = hostname.to_socket_addrs();
    let addrs: Vec<_> = sockets
        .unwrap_or_else(|_| panic!("Failed to resolve node ID={}", node_id))
        .collect();
    addrs
        .get(0) // Return the first ip for now.
        .unwrap_or_else(|| panic!("Failed to get ip address when resolve node ID={}", node_id))
        .ip()
}
