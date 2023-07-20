//! The implementation for CSI node service

use std::sync::Arc;

use grpcio::{RpcContext, UnarySink};
use log::{debug, error, info, warn};
use nix::sys::stat::{self, SFlag};
use protobuf::RepeatedField;

use super::meta_data::{DatenLordVolume, MetaData};
use super::proto::csi::{
    NodeExpandVolumeRequest, NodeExpandVolumeResponse, NodeGetCapabilitiesRequest,
    NodeGetCapabilitiesResponse, NodeGetInfoRequest, NodeGetInfoResponse,
    NodeGetVolumeStatsRequest, NodeGetVolumeStatsResponse, NodePublishVolumeRequest,
    NodePublishVolumeResponse, NodeServiceCapability, NodeServiceCapability_RPC_Type,
    NodeStageVolumeRequest, NodeStageVolumeResponse, NodeUnpublishVolumeRequest,
    NodeUnpublishVolumeResponse, NodeUnstageVolumeRequest, NodeUnstageVolumeResponse, Topology,
    VolumeCapability_oneof_access_type,
};
use super::proto::csi_grpc::Node;
use super::util;
use crate::common::error::DatenLordError::{ArgumentInvalid, Unimplemented};
use crate::common::error::{Context, DatenLordResult};

/// for `NodeService` implementation
#[derive(Clone)]
pub struct NodeImpl {
    /// Inner data
    inner: Arc<NodeImplInner>,
}

/// Holding `NodeImpl` inner data
struct NodeImplInner {
    /// Node capabilities
    caps: Vec<NodeServiceCapability>,
    /// Volume meta data for this node
    meta_data: Arc<MetaData>,
}

impl NodeImpl {
    /// Create `NodeImpl`
    pub fn new(meta_data: Arc<MetaData>) -> Self {
        Self {
            inner: Arc::new(NodeImplInner::new(meta_data)),
        }
    }
}

impl NodeImplInner {
    /// Create `NodeImpl`
    fn new(meta_data: Arc<MetaData>) -> Self {
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
    async fn create_ephemeral_volume(&self, vol_id: &str) -> DatenLordResult<()> {
        let vol_name = format!("ephemeral-{vol_id}");
        let volume = DatenLordVolume::build_ephemeral_volume(
            vol_id,
            &vol_name,
            self.meta_data.get_node_id(),
            &self.meta_data.get_volume_path(vol_id),
        )
        .with_context(|| {
            format!("failed to create ephemeral volume ID={vol_id} and name={vol_name}",)
        })?;
        info!(
            "ephemeral mode: created volume ID={} and name={}",
            volume.vol_id, volume.vol_name,
        );
        let add_ephemeral_res = self.meta_data.add_volume_meta_data(vol_id, &volume).await;
        debug_assert!(
            add_ephemeral_res.is_ok(),
            "ephemeral volume ID={vol_id} and name={vol_name} is duplicated",
        );
        Ok(())
    }

    /// Delete ephemeral volume
    /// `tolerant_error` means whether to ignore umount error or not
    async fn delete_ephemeral_volume(&self, volume: &DatenLordVolume, tolerant_error: bool) {
        let delete_ephemeral_res = self
            .meta_data
            .delete_volume_meta_data(&volume.vol_id, self.meta_data.get_node_id())
            .await;
        if let Err(e) = delete_ephemeral_res {
            if tolerant_error {
                error!(
                    "failed to delete ephemeral volume ID={} and name={}, \
                        the error is: {}",
                    volume.vol_id, volume.vol_name, e,
                );
            } else {
                panic!(
                    "failed to delete ephemeral volume ID={} and name={}, \
                        the error is: {}",
                    volume.vol_id, volume.vol_name, e,
                );
            }
        }
        let delete_dir_res = volume.delete_directory();
        if let Err(e) = delete_dir_res {
            if tolerant_error {
                error!(
                    "failed to delete the directory of ephemerial volume ID={}, \
                        the error is: {}",
                    volume.vol_id, e,
                );
            } else {
                panic!(
                    "failed to delete the directory of ephemerial volume ID={}, \
                        the error is: {}",
                    volume.vol_id, e,
                );
            }
        }
    }

    /// The pre-check helper function for `node_publish_volume`
    fn node_publish_volume_pre_check(req: &NodePublishVolumeRequest) -> DatenLordResult<()> {
        if !req.has_volume_capability() {
            return Err(ArgumentInvalid {
                context: vec!["volume capability missing in request".to_owned()],
            });
        }
        let vol_id = req.get_volume_id();
        if vol_id.is_empty() {
            return Err(ArgumentInvalid {
                context: vec!["volume ID missing in request".to_owned()],
            });
        }
        let target_dir = req.get_target_path();
        if target_dir.is_empty() {
            return Err(ArgumentInvalid {
                context: vec!["target path missing in request".to_owned()],
            });
        }
        Ok(())
    }
}

impl Node for NodeImpl {
    fn node_stage_volume(
        &mut self,
        _ctx: RpcContext,
        req: NodeStageVolumeRequest,
        sink: UnarySink<NodeStageVolumeResponse>,
    ) {
        debug!("node_stage_volume request: {:?}", req);
        let self_inner = Arc::<NodeImplInner>::clone(&self.inner);

        let task = async move {
            let rpc_type = NodeServiceCapability_RPC_Type::STAGE_UNSTAGE_VOLUME;
            if !self_inner.validate_request_capability(rpc_type) {
                return Err(ArgumentInvalid {
                    context: vec![format!("unsupported capability {rpc_type:?}")],
                });
            }

            // Check arguments
            let vol_id = req.get_volume_id();
            if vol_id.is_empty() {
                return Err(ArgumentInvalid {
                    context: vec!["volume ID missing in request".to_owned()],
                });
            }
            if req.get_staging_target_path().is_empty() {
                return Err(ArgumentInvalid {
                    context: vec!["target path missing in request".to_owned()],
                });
            }
            if !req.has_volume_capability() {
                return Err(ArgumentInvalid {
                    context: vec!["volume capability missing in request".to_owned()],
                });
            }

            let r = NodeStageVolumeResponse::new();
            Ok(r)
        };
        util::spawn_grpc_task(sink, task);
    }

    fn node_unstage_volume(
        &mut self,
        _ctx: RpcContext,
        req: NodeUnstageVolumeRequest,
        sink: UnarySink<NodeUnstageVolumeResponse>,
    ) {
        debug!("node_unstage_volume request: {:?}", req);
        let self_inner = Arc::<NodeImplInner>::clone(&self.inner);

        let task = async move {
            let rpc_type = NodeServiceCapability_RPC_Type::STAGE_UNSTAGE_VOLUME;
            if !self_inner.validate_request_capability(rpc_type) {
                return Err(ArgumentInvalid {
                    context: vec![format!("unsupported capability {rpc_type:?}")],
                });
            }

            // Check arguments
            if req.get_volume_id().is_empty() {
                return Err(ArgumentInvalid {
                    context: vec!["volume ID missing in request".to_owned()],
                });
            }
            if req.get_staging_target_path().is_empty() {
                return Err(ArgumentInvalid {
                    context: vec!["target path missing in request".to_owned()],
                });
            }
            let r = NodeUnstageVolumeResponse::new();
            Ok(r)
        };
        util::spawn_grpc_task(sink, task);
    }

    #[allow(clippy::too_many_lines)]
    fn node_publish_volume(
        &mut self,
        _ctx: RpcContext,
        req: NodePublishVolumeRequest,
        sink: UnarySink<NodePublishVolumeResponse>,
    ) {
        debug!("node_publish_volume request: {:?}", req);
        let self_inner = Arc::<NodeImplInner>::clone(&self.inner);

        let task = async move {
            NodeImplInner::node_publish_volume_pre_check(&req)?;
            let read_only = req.get_readonly();
            let volume_context = req.get_volume_context();
            let device_id = match volume_context.get("deviceID") {
                Some(did) => did,
                None => "",
            };

            // Kubernetes 1.15 doesn't have csi.storage.k8s.io/ephemeral
            let context_ephemeral_res = volume_context.get(util::EPHEMERAL_KEY_CONTEXT);
            let ephemeral = context_ephemeral_res.map_or(
                self_inner.meta_data.is_ephemeral(),
                |context_ephemeral_val| {
                    if context_ephemeral_val == "true" {
                        true
                    } else if context_ephemeral_val == "false" {
                        false
                    } else {
                        self_inner.meta_data.is_ephemeral()
                    }
                },
            );

            let vol_id = req.get_volume_id();
            // If ephemeral is true, create volume here to avoid errors if not exists
            let volume_exist = self_inner.meta_data.find_volume_by_id(vol_id).await?;
            if ephemeral && !volume_exist {
                if let Err(e) = self_inner.create_ephemeral_volume(vol_id).await {
                    warn!(
                        "failed to create ephemeral volume ID={}, the error is:{}",
                        vol_id, e,
                    );
                    return Err(e);
                };
            }

            let mut volume = self_inner.meta_data.get_volume_by_id(vol_id).await?;
            let node_id = self_inner.meta_data.get_node_id();
            if !volume.check_exist_in_accessible_nodes(node_id) {
                return Err(ArgumentInvalid {
                    context: vec![format!(
                        "volume ID={vol_id} is not accessible on node ID={node_id}"
                    )],
                });
            }
            if !volume.check_exist_on_node_id(node_id) {
                volume.node_ids.push(node_id.to_owned());
                self_inner
                    .meta_data
                    .update_volume_meta_data(vol_id, &volume)
                    .await?;
                assert!(
                    volume.vol_path.exists(),
                    "volume path {:?} doesn't exist on node ID={}",
                    volume.vol_path,
                    node_id
                );
                /*
                // TODO: (walkaround) need to list dir before access dir.
                let files = std::fs::read_dir(volume.vol_path)?
                    .map(|res| res.map(|e| e.path()))
                    .collect::<Result<Vec<_>, std::io::Error>>()?;
                debug!("current files under volume: {:?}", files);
                */
            }

            let target_dir = req.get_target_path();
            match req.get_volume_capability().access_type {
                None => {
                    return Err(ArgumentInvalid {
                        context: vec!["access_type missing in request".to_owned()],
                    });
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
                        self_inner
                            .meta_data
                            .bind_mount(
                                target_dir,
                                fs_type,
                                read_only,
                                vol_id,
                                &mount_options,
                                ephemeral,
                            )
                            .await?;
                    } else {
                        // VolumeCapability_oneof_access_type::block(..) not supported
                        return Err(ArgumentInvalid {
                            context: vec![format!("unsupported access type {access_type:?}")],
                        });
                    }
                }
            }
            let r = NodePublishVolumeResponse::new();
            Ok(r)
        };
        util::spawn_grpc_task(sink, task);
    }

    fn node_unpublish_volume(
        &mut self,
        _ctx: RpcContext,
        req: NodeUnpublishVolumeRequest,
        sink: UnarySink<NodeUnpublishVolumeResponse>,
    ) {
        debug!("node_unpublish_volume request: {:?}", req);
        let self_inner = Arc::<NodeImplInner>::clone(&self.inner);

        let task = async move {
            // Check arguments
            let vol_id = req.get_volume_id();
            if vol_id.is_empty() {
                return Err(ArgumentInvalid {
                    context: vec!["volume ID missing in request".to_owned()],
                });
            }
            let target_path = req.get_target_path();
            if target_path.is_empty() {
                return Err(ArgumentInvalid {
                    context: vec!["target path missing in request".to_owned()],
                });
            }

            let volume = self_inner.meta_data.get_volume_by_id(vol_id).await?;

            let r = NodeUnpublishVolumeResponse::new();
            // Do not return error for non-existent path, repeated calls OK for idempotency
            // if unistd::geteuid().is_root() {
            let delete_res = self_inner
                .meta_data
                .delete_volume_one_bind_mount_path(vol_id, target_path)
                .await;
            let mut pre_mount_path_set = match delete_res {
                Ok(s) => s,
                Err(e) => {
                    warn!(
                        "failed to delete mount path={} of volume ID={} from etcd, \
                            the error is: {}",
                        target_path, vol_id, e,
                    );
                    return Ok(r);
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
            let target_path_owned = target_path.to_owned();
            if let Err(e) = tokio::task::spawn_blocking(move || {
                util::umount_volume_bind_path(&target_path_owned)
            })
            .await?
            {
                if tolerant_error {
                    // Try to un-mount the path not stored in etcd, if error just log it
                    warn!(
                        "failed to un-mount volume ID={} bind path={}, the error is: {}",
                        vol_id, target_path, e,
                    );
                } else {
                    // Un-mount the path stored in etcd, if error then panic
                    panic!(
                        "failed to un-mount volume ID={vol_id} bind path={target_path}, the error is: {e}",
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
                self_inner
                    .delete_ephemeral_volume(&volume, tolerant_error)
                    .await;
            }
            Ok(r)
        };
        util::spawn_grpc_task(sink, task);
    }

    fn node_get_volume_stats(
        &mut self,
        _ctx: RpcContext,
        req: NodeGetVolumeStatsRequest,
        sink: UnarySink<NodeGetVolumeStatsResponse>,
    ) {
        debug!("node_get_volume_stats request: {:?}", req);
        util::spawn_grpc_task(sink, async {
            Err(Unimplemented {
                context: vec!["unimplemented".to_owned()],
            })
        });
    }

    // node_expand_volume is only implemented so the driver can be used for e2e
    // testing no actual volume expansion operation
    fn node_expand_volume(
        &mut self,
        _ctx: RpcContext,
        req: NodeExpandVolumeRequest,
        sink: UnarySink<NodeExpandVolumeResponse>,
    ) {
        debug!("node_expand_volume request: {:?}", req);

        let self_inner = Arc::<NodeImplInner>::clone(&self.inner);

        let task = async move {
            // Check arguments
            let vol_id = req.get_volume_id();
            if vol_id.is_empty() {
                return Err(ArgumentInvalid {
                    context: vec!["volume ID missing in request".to_owned()],
                });
            }

            let vol_path = req.get_volume_path();
            if vol_path.is_empty() {
                return Err(ArgumentInvalid {
                    context: vec!["volume path missing in request".to_owned()],
                });
            }

            self_inner
                .meta_data
                .get_volume_by_id(vol_id)
                .await
                .with_context(|| format!("failed to find volume ID={vol_id}"))?;

            if !req.has_capacity_range() {
                return Err(ArgumentInvalid {
                    context: vec!["volume expand capacity missing in request".to_owned()],
                });
            }

            let file_stat = stat::stat(vol_path)
                .with_context(|| format!("failed to get file stat of {vol_path}"))?;

            let sflag = SFlag::from_bits_truncate(file_stat.st_mode);
            if let SFlag::S_IFDIR = sflag {
                // SFlag::S_IFBLK and other type not supported
                // TODO: implement volume expansion here
                debug!("volume access type mount requires volume file type directory");
            } else {
                return Err(ArgumentInvalid {
                    context: vec![format!(
                        "volume ID={vol_id} has unsupported file type={sflag:?}"
                    )],
                });
            }

            let mut r = NodeExpandVolumeResponse::new();
            r.set_capacity_bytes(req.get_capacity_range().get_required_bytes());
            Ok(r)
        };
        util::spawn_grpc_task(sink, task);
    }

    fn node_get_capabilities(
        &mut self,
        _ctx: RpcContext,
        req: NodeGetCapabilitiesRequest,
        sink: UnarySink<NodeGetCapabilitiesResponse>,
    ) {
        debug!("node_get_capabilities request: {:?}", req);

        let mut r = NodeGetCapabilitiesResponse::new();
        r.set_capabilities(RepeatedField::from_vec(self.inner.caps.clone()));
        util::spawn_grpc_task(sink, async move { Ok(r) });
    }

    fn node_get_info(
        &mut self,
        _ctx: RpcContext,
        req: NodeGetInfoRequest,
        sink: UnarySink<NodeGetInfoResponse>,
    ) {
        debug!("node_get_info request: {:?}", req);

        let mut topology = Topology::new();
        topology.mut_segments().insert(
            util::TOPOLOGY_KEY_NODE.to_owned(),
            self.inner.meta_data.get_node_id().to_owned(),
        );

        let mut r = NodeGetInfoResponse::new();
        r.set_node_id(self.inner.meta_data.get_node_id().to_owned());
        r.set_max_volumes_per_node(self.inner.meta_data.get_max_volumes_per_node().into());
        r.set_accessible_topology(topology);

        util::spawn_grpc_task(sink, async move { Ok(r) });
    }
}
