//! The implementation for CSI node service

use anyhow::{anyhow, Context};
use grpcio::{RpcContext, RpcStatusCode, UnarySink};
use log::{debug, error, info, warn};
use nix::sys::stat::{self, SFlag};
use protobuf::RepeatedField;
use std::sync::Arc;

use super::csi::{
    NodeExpandVolumeRequest, NodeExpandVolumeResponse, NodeGetCapabilitiesRequest,
    NodeGetCapabilitiesResponse, NodeGetInfoRequest, NodeGetInfoResponse,
    NodeGetVolumeStatsRequest, NodeGetVolumeStatsResponse, NodePublishVolumeRequest,
    NodePublishVolumeResponse, NodeServiceCapability, NodeServiceCapability_RPC_Type,
    NodeStageVolumeRequest, NodeStageVolumeResponse, NodeUnpublishVolumeRequest,
    NodeUnpublishVolumeResponse, NodeUnstageVolumeRequest, NodeUnstageVolumeResponse, Topology,
    VolumeCapability_oneof_access_type,
};
use super::csi_grpc::Node;
use super::meta_data::{DatenLordVolume, MetaData};
use super::util;

/// for `NodeService` implementation
#[derive(Clone)]
pub struct NodeImpl {
    /// Node capabilities
    caps: Vec<NodeServiceCapability>,
    /// Volume meta data for this node
    meta_data: Arc<MetaData>,
}

impl NodeImpl {
    /// Create `NodeImpl`
    pub fn new(meta_data: Arc<MetaData>) -> Self {
        let cap_vec = vec![NodeServiceCapability_RPC_Type::EXPAND_VOLUME];
        let caps = cap_vec
            .into_iter()
            .map(|rpc_type| {
                let mut csc = NodeServiceCapability::new();
                csc.mut_rpc().set_field_type(rpc_type);
                csc
            })
            .collect();
        Self { caps, meta_data }
    }

    /// Validate request with controller capabilities
    fn validate_request_capability(&self, rpc_type: NodeServiceCapability_RPC_Type) -> bool {
        rpc_type == NodeServiceCapability_RPC_Type::UNKNOWN
            || self
                .caps
                .iter()
                .any(|cap| cap.get_rpc().get_field_type() == rpc_type)
    }

    /// Create ephemeral volume
    fn create_ephemeral_volume(&self, vol_id: &str) -> Result<(), (RpcStatusCode, anyhow::Error)> {
        let vol_name = format!("ephemeral-{}", vol_id);
        let vol_res = DatenLordVolume::build_ephemeral_volume(
            vol_id,
            &vol_name,
            self.meta_data.get_node_id(),
            &self.meta_data.get_volume_path(vol_id),
        )
        .context(format!(
            "failed to create ephemeral volume ID={} and name={}",
            vol_id, vol_name,
        ));
        let volume = match vol_res {
            Ok(v) => v,
            Err(e) => {
                return Err((RpcStatusCode::INTERNAL, e));
            }
        };
        info!(
            "ephemeral mode: created volume ID={} and name={}",
            volume.vol_id, volume.vol_name,
        );
        let add_ephemeral_res = self.meta_data.add_volume_meta_data(vol_id, &volume);
        debug_assert!(
            add_ephemeral_res.is_ok(),
            format!(
                "ephemeral volume ID={} and name={} is duplicated",
                vol_id, vol_name,
            )
        );
        Ok(())
    }

    /// Delete ephemeral volume
    /// `tolerant_error` means whether to ignore umount error or not
    fn delete_ephemeral_volume(&self, volume: &DatenLordVolume, tolerant_error: bool) {
        let delete_ephemeral_res = self.meta_data.delete_volume_meta_data(&volume.vol_id);
        if let Err(e) = delete_ephemeral_res {
            if tolerant_error {
                error!(
                    "failed to delete ephemeral volume ID={} and name={}, \
                        the error is: {}",
                    volume.vol_id,
                    volume.vol_name,
                    util::format_anyhow_error(&e),
                );
            } else {
                panic!(
                    "failed to delete ephemeral volume ID={} and name={}, \
                        the error is: {}",
                    volume.vol_id,
                    volume.vol_name,
                    util::format_anyhow_error(&e),
                );
            }
        }
        let delete_dir_res = volume.delete_directory();
        if let Err(e) = delete_dir_res {
            if tolerant_error {
                error!(
                    "failed to delete the directory of ephemerial volume ID={}, \
                        the error is: {}",
                    volume.vol_id,
                    util::format_anyhow_error(&e),
                );
            } else {
                panic!(
                    "failed to delete the directory of ephemerial volume ID={}, \
                        the error is: {}",
                    volume.vol_id,
                    util::format_anyhow_error(&e),
                );
            }
        }
    }

    /// The pre-check helper function for `node_publish_volume`
    fn node_publish_volume_pre_check(
        req: &NodePublishVolumeRequest,
    ) -> Result<(), (RpcStatusCode, anyhow::Error)> {
        if !req.has_volume_capability() {
            return Err((
                RpcStatusCode::INVALID_ARGUMENT,
                anyhow!("volume capability missing in request"),
            ));
        }
        let vol_id = req.get_volume_id();
        if vol_id.is_empty() {
            return Err((
                RpcStatusCode::INVALID_ARGUMENT,
                anyhow!("volume ID missing in request"),
            ));
        }
        let target_dir = req.get_target_path();
        if target_dir.is_empty() {
            return Err((
                RpcStatusCode::INVALID_ARGUMENT,
                anyhow!("target path missing in request"),
            ));
        }
        Ok(())
    }
}

impl Node for NodeImpl {
    fn node_stage_volume(
        &mut self,
        ctx: RpcContext,
        req: NodeStageVolumeRequest,
        sink: UnarySink<NodeStageVolumeResponse>,
    ) {
        debug!("node_stage_volume request: {:?}", req);

        let rpc_type = NodeServiceCapability_RPC_Type::STAGE_UNSTAGE_VOLUME;
        if !self.validate_request_capability(rpc_type) {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                &anyhow!(format!("unsupported capability {:?}", rpc_type)),
            );
        }

        // Check arguments
        let vol_id = req.get_volume_id();
        if vol_id.is_empty() {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                &anyhow!("volume ID missing in request"),
            );
        }
        if req.get_staging_target_path().is_empty() {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                &anyhow!("target path missing in request"),
            );
        }
        if !req.has_volume_capability() {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                &anyhow!("volume Capability missing in request"),
            );
        }

        let r = NodeStageVolumeResponse::new();
        util::success(&ctx, sink, r)
    }

    fn node_unstage_volume(
        &mut self,
        ctx: RpcContext,
        req: NodeUnstageVolumeRequest,
        sink: UnarySink<NodeUnstageVolumeResponse>,
    ) {
        debug!("node_unstage_volume request: {:?}", req);

        let rpc_type = NodeServiceCapability_RPC_Type::STAGE_UNSTAGE_VOLUME;
        if !self.validate_request_capability(rpc_type) {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                &anyhow!(format!("unsupported capability {:?}", rpc_type)),
            );
        }

        // Check arguments
        if req.get_volume_id().is_empty() {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                &anyhow!("volume ID missing in request"),
            );
        }
        if req.get_staging_target_path().is_empty() {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                &anyhow!("target path missing in request"),
            );
        }
        let r = NodeUnstageVolumeResponse::new();
        util::success(&ctx, sink, r)
    }

    fn node_publish_volume(
        &mut self,
        ctx: RpcContext,
        req: NodePublishVolumeRequest,
        sink: UnarySink<NodePublishVolumeResponse>,
    ) {
        debug!("node_publish_volume request: {:?}", req);

        if let Err((rpc_status_code, e)) = Self::node_publish_volume_pre_check(&req) {
            return util::fail(&ctx, sink, rpc_status_code, &e);
        }
        // if !req.has_volume_capability() {
        //     return util::fail(
        //         &ctx,
        //         sink,
        //         RpcStatusCode::INVALID_ARGUMENT,
        //         &anyhow!("volume capability missing in request"),
        //     );
        // }
        // let vol_id = req.get_volume_id();
        // if vol_id.is_empty() {
        //     return util::fail(
        //         &ctx,
        //         sink,
        //         RpcStatusCode::INVALID_ARGUMENT,
        //         &anyhow!("volume ID missing in request"),
        //     );
        // }
        // let target_dir = req.get_target_path();
        // if target_dir.is_empty() {
        //     return util::fail(
        //         &ctx,
        //         sink,
        //         RpcStatusCode::INVALID_ARGUMENT,
        //         &anyhow!("target path missing in request"),
        //     );
        // }

        let read_only = req.get_readonly();
        let volume_context = req.get_volume_context();
        let device_id = match volume_context.get("deviceID") {
            Some(did) => did,
            None => "",
        };

        // Kubernetes 1.15 doesn't have csi.storage.k8s.io/ephemeral
        let context_ephemeral_res = volume_context.get(util::EPHEMERAL_KEY_CONTEXT);
        let ephemeral =
            context_ephemeral_res.map_or(self.meta_data.is_ephemeral(), |context_ephemeral_val| {
                if context_ephemeral_val == "true" {
                    true
                } else if context_ephemeral_val == "false" {
                    false
                } else {
                    self.meta_data.is_ephemeral()
                }
            });

        let vol_id = req.get_volume_id();
        // If ephemeral is true, create volume here to avoid errors if not exists
        if ephemeral && !self.meta_data.find_volume_by_id(vol_id) {
            let create_res = self.create_ephemeral_volume(vol_id);
            if let Err((rpc_status_code, e)) = create_res {
                warn!(
                    "failed to create ephemeral volume ID={}, RpcStatusCode={}, the error is:{}",
                    vol_id,
                    rpc_status_code,
                    util::format_anyhow_error(&e),
                );
                return util::fail(&ctx, sink, RpcStatusCode::INTERNAL, &e);
            }
        }
        let target_dir = req.get_target_path();
        match req.get_volume_capability().access_type {
            None => {
                return util::fail(
                    &ctx,
                    sink,
                    RpcStatusCode::INVALID_ARGUMENT,
                    &anyhow!("access_type missing in request"),
                );
            }
            Some(ref access_type) => {
                if let VolumeCapability_oneof_access_type::mount(ref volume_mount_option) =
                    *access_type
                {
                    let fs_type = volume_mount_option.get_fs_type();
                    let mount_flags = volume_mount_option.get_mount_flags();
                    let mount_options = mount_flags.join(",");
                    info!(
                        "target={}\nfstype={}\ndevice={}\nreadonly={}\n\
                            volume ID={}\nattributes={:?}\nmountflags={}\n",
                        target_dir,
                        fs_type,
                        device_id,
                        read_only,
                        vol_id,
                        volume_context,
                        mount_options,
                    );
                    // Bind mount from target_dir to vol_path
                    let build_res = self.meta_data.bind_mount(
                        target_dir,
                        fs_type,
                        read_only,
                        vol_id,
                        &mount_options,
                        ephemeral,
                    );
                    if let Err((rpc_status_code, e)) = build_res {
                        return util::fail(&ctx, sink, rpc_status_code, &e);
                    }
                } else {
                    // VolumeCapability_oneof_access_type::block(..) not supported
                    return util::fail(
                        &ctx,
                        sink,
                        RpcStatusCode::INVALID_ARGUMENT,
                        &anyhow!(format!("unsupported access type {:?}", access_type)),
                    );
                }
            }
        }

        let r = NodePublishVolumeResponse::new();
        util::success(&ctx, sink, r)
    }

    fn node_unpublish_volume(
        &mut self,
        ctx: RpcContext,
        req: NodeUnpublishVolumeRequest,
        sink: UnarySink<NodeUnpublishVolumeResponse>,
    ) {
        debug!("node_unpublish_volume request: {:?}", req);

        // Check arguments
        let vol_id = req.get_volume_id();
        if vol_id.is_empty() {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                &anyhow!("volume ID missing in request"),
            );
        }
        let target_path = req.get_target_path();
        if target_path.is_empty() {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                &anyhow!("target path missing in request"),
            );
        }

        let volume = match self.meta_data.get_volume_by_id(vol_id) {
            Some(v) => v,
            None => {
                return util::fail(
                    &ctx,
                    sink,
                    RpcStatusCode::NOT_FOUND,
                    &anyhow!(format!("failed to find volume ID={}", vol_id)),
                )
            }
        };

        let r = NodeUnpublishVolumeResponse::new();
        // Do not return error for non-existent path, repeated calls OK for idempotency
        // if unistd::geteuid().is_root() {
        let delete_res = self
            .meta_data
            .delete_volume_one_bind_mount_path(vol_id, target_path);
        let mut pre_mount_path_set = match delete_res {
            Ok(s) => s,
            Err(e) => {
                warn!(
                    "failed to delete mount path={} of volume ID={} from etcd, \
                        the error is: {}",
                    target_path,
                    vol_id,
                    util::format_anyhow_error(&e),
                );
                return util::success(&ctx, sink, r);
            }
        };
        let remove_res = pre_mount_path_set.remove(target_path);
        let tolerant_error = if remove_res {
            debug!("the target path to un-mount found in etcd");
            // Do not tolerant umount error,
            // since the target path is one of the mount paths of this volume
            false
        } else {
            warn!(
                "the target path={} to un-mount not found in etcd",
                target_path
            );
            // Tolerant umount error,
            // since the target path is not one of the mount paths of this volume
            true
        };
        if let Err(e) = util::umount_volume_bind_path(target_path) {
            if tolerant_error {
                // Try to un-mount the path not stored in etcd, if error just log it
                warn!(
                    "failed to un-mount volume ID={} bind path={}, the error is: {}",
                    vol_id,
                    target_path,
                    util::format_anyhow_error(&e),
                );
            } else {
                // Un-mount the path stored in etcd, if error then panic
                panic!(
                    "failed to un-mount volume ID={} bind path={}, the error is: {}",
                    vol_id,
                    target_path,
                    util::format_anyhow_error(&e),
                );
            }
        } else {
            debug!(
                "successfully un-mount voluem ID={} bind path={}",
                vol_id, target_path,
            );
        }
        info!(
            "volume ID={} and name={} with target path={} has been unpublished.",
            vol_id, volume.vol_name, target_path
        );

        // Delete ephemeral volume if no more bind mount
        // Does not return error when delete failure, repeated calls OK for idempotency
        if volume.ephemeral && pre_mount_path_set.is_empty() {
            self.delete_ephemeral_volume(&volume, tolerant_error);
        }
        util::success(&ctx, sink, r)
    }

    fn node_get_volume_stats(
        &mut self,
        ctx: RpcContext,
        req: NodeGetVolumeStatsRequest,
        sink: UnarySink<NodeGetVolumeStatsResponse>,
    ) {
        debug!("node_get_volume_stats request: {:?}", req);

        util::fail(
            &ctx,
            sink,
            RpcStatusCode::UNIMPLEMENTED,
            &anyhow!("unimplemented"),
        )
    }

    // node_expand_volume is only implemented so the driver can be used for e2e testing
    // no actual volume expansion operation
    fn node_expand_volume(
        &mut self,
        ctx: RpcContext,
        req: NodeExpandVolumeRequest,
        sink: UnarySink<NodeExpandVolumeResponse>,
    ) {
        debug!("node_expand_volume request: {:?}", req);

        // Check arguments
        let vol_id = req.get_volume_id();
        if vol_id.is_empty() {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                &anyhow!("volume ID missing in request"),
            );
        }

        let vol_path = req.get_volume_path();
        if vol_path.is_empty() {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                &anyhow!("volume path missing in request"),
            );
        }

        if !self.meta_data.find_volume_by_id(vol_id) {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::NOT_FOUND,
                &anyhow!(format!("failed to find volume ID={}", vol_id)),
            );
        };

        if !req.has_capacity_range() {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                &anyhow!("volume expand capacity missing in request"),
            );
        }

        let stat_res =
            stat::stat(vol_path).context(format!("failed to get file stat of {}", vol_path));
        let file_stat = match stat_res {
            Ok(s) => s,
            Err(e) => {
                return util::fail(&ctx, sink, RpcStatusCode::INVALID_ARGUMENT, &e);
            }
        };

        let sflag = SFlag::from_bits_truncate(file_stat.st_mode);
        if let SFlag::S_IFDIR = sflag {
            // SFlag::S_IFBLK and other type not supported
            // TODO: implement volume expansion here
            debug!("volume access type mount requires volume file type directory");
        } else {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                &anyhow!(format!(
                    "volume ID={} has unsupported file type={:?}",
                    vol_id, sflag
                )),
            );
        }

        let mut r = NodeExpandVolumeResponse::new();
        r.set_capacity_bytes(req.get_capacity_range().get_required_bytes());
        util::success(&ctx, sink, r)
    }

    fn node_get_capabilities(
        &mut self,
        ctx: RpcContext,
        req: NodeGetCapabilitiesRequest,
        sink: UnarySink<NodeGetCapabilitiesResponse>,
    ) {
        debug!("node_get_capabilities request: {:?}", req);

        let mut r = NodeGetCapabilitiesResponse::new();
        r.set_capabilities(RepeatedField::from_vec(self.caps.clone()));
        util::success(&ctx, sink, r)
    }

    fn node_get_info(
        &mut self,
        ctx: RpcContext,
        req: NodeGetInfoRequest,
        sink: UnarySink<NodeGetInfoResponse>,
    ) {
        debug!("node_get_info request: {:?}", req);

        let mut topology = Topology::new();
        topology.mut_segments().insert(
            util::TOPOLOGY_KEY_NODE.to_owned(),
            self.meta_data.get_node_id().to_owned(),
        );

        let mut r = NodeGetInfoResponse::new();
        r.set_node_id(self.meta_data.get_node_id().to_owned());
        r.set_max_volumes_per_node(self.meta_data.get_max_volumes_per_node().into());
        r.set_accessible_topology(topology);

        util::success(&ctx, sink, r)
    }
}
