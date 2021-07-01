//! The implementation for CSI controller service

use grpcio::{RpcContext, UnarySink};
use log::{debug, error, info, warn};
use protobuf::RepeatedField;
use std::cmp::Ordering;
use std::sync::Arc;

use super::csi::{
    ControllerExpandVolumeRequest, ControllerExpandVolumeResponse,
    ControllerGetCapabilitiesRequest, ControllerGetCapabilitiesResponse,
    ControllerGetVolumeRequest, ControllerGetVolumeResponse, ControllerPublishVolumeRequest,
    ControllerPublishVolumeResponse, ControllerServiceCapability,
    ControllerServiceCapability_RPC_Type, ControllerUnpublishVolumeRequest,
    ControllerUnpublishVolumeResponse, CreateSnapshotRequest, CreateSnapshotResponse,
    CreateVolumeRequest, CreateVolumeResponse, DeleteSnapshotRequest, DeleteSnapshotResponse,
    DeleteVolumeRequest, DeleteVolumeResponse, GetCapacityRequest, GetCapacityResponse,
    ListSnapshotsRequest, ListSnapshotsResponse, ListSnapshotsResponse_Entry, ListVolumesRequest,
    ListVolumesResponse, ValidateVolumeCapabilitiesRequest, ValidateVolumeCapabilitiesResponse,
    VolumeCapability, VolumeCapability_AccessMode_Mode, VolumeContentSource_oneof_type,
};
use super::csi_grpc::Controller;
use super::meta_data::{DatenLordSnapshot, MetaData, VolumeSource};
use super::util;
use common::error::{
    Context,
    DatenLordError::{
        ArgumentInvalid, ArgumentOutOfRange, SnapshotAlreadyExist, SnapshotNotFound, Unimplemented,
        VolumeAlreadyExist, VolumeNotFound,
    },
    DatenLordResult,
};

/// for `ControllerService` implementation
#[derive(Clone)]
pub struct ControllerImpl {
    /// Inner data
    inner: Arc<ControllerImplInner>,
}

/// Holding `ControllerImpl` inner data
struct ControllerImplInner {
    /// Controller capabilities
    caps: Vec<ControllerServiceCapability>,
    /// Volume meta data for controller
    meta_data: Arc<MetaData>,
}

impl ControllerImpl {
    /// Create `ControllerImpl`
    pub fn new(meta_data: Arc<MetaData>) -> Self {
        Self {
            inner: Arc::new(ControllerImplInner::new(meta_data)),
        }
    }
}

impl ControllerImplInner {
    /// Create `ControllerImplInner`
    fn new(meta_data: Arc<MetaData>) -> Self {
        let cap_vec = if meta_data.is_ephemeral() {
            Vec::new()
        } else {
            vec![
                ControllerServiceCapability_RPC_Type::CREATE_DELETE_VOLUME,
                ControllerServiceCapability_RPC_Type::CREATE_DELETE_SNAPSHOT,
                ControllerServiceCapability_RPC_Type::CLONE_VOLUME,
                ControllerServiceCapability_RPC_Type::LIST_VOLUMES,
                ControllerServiceCapability_RPC_Type::LIST_SNAPSHOTS,
                ControllerServiceCapability_RPC_Type::EXPAND_VOLUME,
            ]
        };
        let caps = cap_vec
            .into_iter()
            .map(|rpc_type| {
                let mut csc = ControllerServiceCapability::new();
                csc.mut_rpc().set_field_type(rpc_type);
                csc
            })
            .collect();

        Self { caps, meta_data }
    }

    /// Validate request with controller capabilities
    fn validate_request_capability(&self, rpc_type: ControllerServiceCapability_RPC_Type) -> bool {
        rpc_type == ControllerServiceCapability_RPC_Type::UNKNOWN
            || self
                .caps
                .iter()
                .any(|cap| cap.get_rpc().get_field_type() == rpc_type)
    }

    /// Check for already existing volume name, and if found
    /// check for the requested capacity and already allocated capacity
    async fn find_existing_volume(
        &self,
        req: &CreateVolumeRequest,
    ) -> DatenLordResult<CreateVolumeResponse> {
        let vol_name = req.get_name();
        let ex_vol = self.meta_data.get_volume_by_name(vol_name).await?;
        // It means the volume with the same name already exists
        // need to check if the size of existing volume is the same as in new
        // request
        let volume_size = req.get_capacity_range().get_required_bytes();
        if ex_vol.get_size() != volume_size {
            return Err(VolumeAlreadyExist {
                volume_id: vol_name.to_string(),
                context: vec![format!(
                    "volume with the same name={} already exist but with different size",
                    vol_name,
                )],
            });
        }

        if req.has_volume_content_source() {
            let ex_vol_content_source = if let Some(ref vcs) = ex_vol.content_source {
                vcs
            } else {
                return Err(VolumeAlreadyExist {
                    volume_id: ex_vol.vol_id.clone(),
                    context: vec![format!(
                        "existing volume ID={} doesn't have content source",
                        ex_vol.vol_id,
                    )],
                });
            };

            let volume_source = req.get_volume_content_source();
            if let Some(ref volume_source_type) = volume_source.field_type {
                match *volume_source_type {
                    VolumeContentSource_oneof_type::snapshot(ref snapshot_source) => {
                        let parent_snap_id = snapshot_source.get_snapshot_id();
                        if let VolumeSource::Snapshot(ref psid) = *ex_vol_content_source {
                            if psid != parent_snap_id {
                                return Err(VolumeAlreadyExist {
                                    volume_id: ex_vol.vol_id.clone(),
                                    context: vec![format!(
                                        "existing volume ID={} has parent snapshot ID={}, \
                                                but VolumeContentSource_SnapshotSource has \
                                                parent snapshot ID={}",
                                        ex_vol.vol_id, psid, parent_snap_id,
                                    )],
                                });
                            }
                        } else {
                            return Err(VolumeAlreadyExist {
                                volume_id: ex_vol.vol_id.clone(),
                                context: vec![format!(
                                    "existing volume ID={} doesn't have parent snapshot ID",
                                    ex_vol.vol_id
                                )],
                            });
                        }
                    }
                    VolumeContentSource_oneof_type::volume(ref volume_source) => {
                        let parent_vol_id = volume_source.get_volume_id();
                        if let VolumeSource::Volume(ref pvid) = *ex_vol_content_source {
                            if pvid != parent_vol_id {
                                return Err(VolumeAlreadyExist {
                                    volume_id: ex_vol.vol_id.clone(),
                                    context: vec![format!(
                                        "existing volume ID={} has parent volume ID={}, \
                                                but VolumeContentSource_VolumeSource has \
                                                parent volume ID={}",
                                        ex_vol.vol_id, pvid, parent_vol_id,
                                    )],
                                });
                            }
                        } else {
                            return Err(VolumeAlreadyExist {
                                volume_id: ex_vol.vol_id.clone(),
                                context: vec![format!(
                                    "existing volume ID={} doesn't have parent volume ID",
                                    ex_vol.vol_id,
                                )],
                            });
                        }
                    }
                }
            }
        }
        // Return existing volume
        // TODO: make sure that volume still exists?
        let resp = util::build_create_volume_response(req, &ex_vol.vol_id, ex_vol.accessible_nodes);
        Ok(resp)
    }

    /// Check for already existing snapshot name, and if found check for the
    /// requested source volume ID matches snapshot that has been created
    async fn find_existing_snapshot(
        &self,
        req: &CreateSnapshotRequest,
    ) -> DatenLordResult<CreateSnapshotResponse> {
        let ex_snap = self.meta_data.get_snapshot_by_name(req.get_name()).await?;
        // The snapshot with the same name already exists need to check
        // if the source volume ID of existing snapshot is the same as in new request
        let snap_name = req.get_name();
        let src_vol_id = req.get_source_volume_id();
        let node_id = self.meta_data.get_node_id();

        if ex_snap.vol_id == src_vol_id {
            let build_resp_res = util::build_create_snapshot_response(
                req,
                &ex_snap.snap_id,
                &ex_snap.creation_time,
                ex_snap.size_bytes,
            )
            .with_context(|| {
                format!(
                    "failed to build CreateSnapshotResponse at controller ID={}",
                    node_id,
                )
            });
            match build_resp_res {
                Ok(resp) => {
                    info!(
                        "find existing snapshot ID={} and name={}",
                        ex_snap.snap_id, snap_name,
                    );
                    Ok(resp)
                }
                Err(e) => Err(e),
            }
        } else {
            Err(SnapshotAlreadyExist {
                snapshot_id: snap_name.to_string(),
                context: vec![format!(
                    "snapshot with the same name={} exists on node ID={} \
                            but of different source volume ID",
                    snap_name, node_id,
                )],
            })
        }
    }

    /// Build list snapshot response
    fn add_snapshot_to_list_response(
        snap: &DatenLordSnapshot,
    ) -> DatenLordResult<ListSnapshotsResponse> {
        let mut entry = ListSnapshotsResponse_Entry::new();
        entry.mut_snapshot().set_size_bytes(snap.size_bytes);
        entry.mut_snapshot().set_snapshot_id(snap.snap_id.clone());
        entry
            .mut_snapshot()
            .set_source_volume_id(snap.vol_id.clone());
        entry.mut_snapshot().set_creation_time(
            util::generate_proto_timestamp(&snap.creation_time)
                .add_context("failed to convert to proto timestamp")?,
        );
        entry.mut_snapshot().set_ready_to_use(snap.ready_to_use);

        let mut resp = ListSnapshotsResponse::new();
        resp.set_entries(RepeatedField::from_vec(vec![entry]));
        Ok(resp)
    }

    /// Call worker create volume
    async fn worker_create_volume(
        &self,
        req: &CreateVolumeRequest,
    ) -> DatenLordResult<CreateVolumeResponse> {
        let worker_node = match self.meta_data.select_node(req).await {
            Ok(n) => n,
            Err(e) => {
                error!("failed to select a node, the error is: {}", e,);
                return Err(e);
            }
        };
        let client = MetaData::build_worker_client(&worker_node);
        let create_res = client.worker_create_volume_async(req)?;

        create_res.await.map_err(|e| e.into())
    }

    /// The pre-check helper function for `create_volume`
    fn create_volume_pre_check(&self, req: &CreateVolumeRequest) -> DatenLordResult<()> {
        let rpc_type = ControllerServiceCapability_RPC_Type::CREATE_DELETE_VOLUME;
        if !self.validate_request_capability(rpc_type) {
            return Err(ArgumentInvalid {
                context: vec![format!("unsupported capability {:?}", rpc_type)],
            });
        }

        let vol_name = req.get_name();
        if vol_name.is_empty() {
            return Err(ArgumentInvalid {
                context: vec!["name missing in request".to_string()],
            });
        }

        let req_caps = req.get_volume_capabilities();
        if req_caps.is_empty() {
            return Err(ArgumentInvalid {
                context: vec!["volume capabilities missing in request".to_string()],
            });
        }

        let access_type_block = req_caps.iter().any(VolumeCapability::has_block);
        let access_mode_multi_writer = req_caps.iter().any(|vc| {
            let vol_access_mode = vc.get_access_mode().get_mode();
            vol_access_mode == VolumeCapability_AccessMode_Mode::MULTI_NODE_READER_ONLY
                || vol_access_mode == VolumeCapability_AccessMode_Mode::MULTI_NODE_MULTI_WRITER
                || vol_access_mode == VolumeCapability_AccessMode_Mode::MULTI_NODE_SINGLE_WRITER
        });
        if access_type_block {
            return Err(ArgumentInvalid {
                context: vec!["access type block not supported".to_string()],
            });
        }
        if access_mode_multi_writer {
            return Err(ArgumentInvalid {
                context: vec!["access mode MULTI_NODE_SINGLE_WRITER and MULTI_NODE_MULTI_WRITER not supported".to_string()],
            });
        }

        let volume_size = req.get_capacity_range().get_required_bytes();
        if volume_size > util::MAX_VOLUME_STORAGE_CAPACITY {
            return Err(ArgumentOutOfRange {
                context: vec![format!(
                    "requested size {} exceeds maximum allowed {}",
                    volume_size,
                    util::MAX_VOLUME_STORAGE_CAPACITY,
                )],
            });
        }

        Ok(())
    }
}

impl Controller for ControllerImpl {
    fn create_volume(
        &mut self,
        _ctx: RpcContext,
        req: CreateVolumeRequest,
        sink: UnarySink<CreateVolumeResponse>,
    ) {
        debug!("create_volume request: {:?}", req);
        let self_inner = Arc::<ControllerImplInner>::clone(&self.inner);

        let task = async move {
            self_inner.create_volume_pre_check(&req)?;

            let vol_name = req.get_name();
            match self_inner.find_existing_volume(&req).await {
                Ok(resp) => {
                    debug!(
                        "found existing volume ID={} and name={}",
                        resp.get_volume().get_volume_id(),
                        vol_name,
                    );
                    return Ok(resp);
                }
                Err(e) => {
                    if let VolumeNotFound { .. } = e {
                        debug!("no volume name={} found", vol_name);
                    } else {
                        debug!(
                            "failed to find existing volume name={}, the error is: {}",
                            vol_name, e,
                        );
                        return Err(e);
                    }
                }
            }

            self_inner.worker_create_volume(&req).await.map_err(|e| {
                debug!("failed to create volume, the error is: {}", e);
                e
            })
        };

        util::spawn_grpc_task(sink, task);
    }

    fn delete_volume(
        &mut self,
        _ctx: RpcContext,
        req: DeleteVolumeRequest,
        sink: UnarySink<DeleteVolumeResponse>,
    ) {
        debug!("delete_volume request: {:?}", req);
        let self_inner = Arc::<ControllerImplInner>::clone(&self.inner);

        let task = async move {
            // Check arguments
            let vol_id = req.get_volume_id();
            if vol_id.is_empty() {
                return Err(ArgumentInvalid {
                    context: vec!["volume ID missing in request".to_string()],
                });
            }

            let rpc_type = ControllerServiceCapability_RPC_Type::CREATE_DELETE_VOLUME;
            if !self_inner.validate_request_capability(rpc_type) {
                return Err(ArgumentInvalid {
                    context: vec![format!("unsupported capability {:?}", rpc_type)],
                });
            }

            // Do not return gRPC error when delete failed for idempotency
            match self_inner.meta_data.get_volume_by_id(vol_id).await {
                Ok(vol) => {
                    // TODO don't fail on first error.
                    for node_id in vol.node_ids {
                        let node = self_inner.meta_data.get_node_by_id(&node_id).await?;
                        let client = MetaData::build_worker_client(&node);
                        let worker_delete_res = client
                            .worker_delete_volume_async(&req)?
                            .await
                            .with_context(|| {
                                format!(
                                    "failed to delete volume ID={} on node ID={}",
                                    vol_id, node_id,
                                )
                            });
                        match worker_delete_res {
                            Ok(_) => info!("successfully deleted volume ID={}", vol_id),
                            Err(e) => {
                                // Return error here?
                                // Should we return this error, old logic will ignore this
                                warn!(
                                    "failed to delete volume ID={} on node ID={}, the error is: {}",
                                    vol_id, node_id, e,
                                );
                                return Err(e);
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "failed to find volume ID={} to delete on controller ID={}",
                        vol_id,
                        self_inner.meta_data.get_node_id(),
                    );
                    if let VolumeNotFound { .. } = e {
                    } else {
                        // Should we return this error, old logic will ignore this
                        return Err(e);
                    }
                }
            }

            let r = DeleteVolumeResponse::new();
            Ok(r)
        };
        util::spawn_grpc_task(sink, task);
    }

    fn controller_publish_volume(
        &mut self,
        _ctx: RpcContext,
        req: ControllerPublishVolumeRequest,
        sink: UnarySink<ControllerPublishVolumeResponse>,
    ) {
        debug!("controller_publish_volume request: {:?}", req);

        util::spawn_grpc_task(sink, async {
            Err(Unimplemented {
                context: vec!["unimplemented".to_string()],
            })
        });
    }

    fn controller_unpublish_volume(
        &mut self,
        _ctx: RpcContext,
        req: ControllerUnpublishVolumeRequest,
        sink: UnarySink<ControllerUnpublishVolumeResponse>,
    ) {
        debug!("controller_unpublish_volume request: {:?}", req);

        util::spawn_grpc_task(sink, async {
            Err(Unimplemented {
                context: vec!["unimplemented".to_string()],
            })
        });
    }

    fn validate_volume_capabilities(
        &mut self,
        _ctx: RpcContext,
        mut req: ValidateVolumeCapabilitiesRequest,
        sink: UnarySink<ValidateVolumeCapabilitiesResponse>,
    ) {
        debug!("validate_volume_capabilities request: {:?}", req);
        let self_inner = Arc::<ControllerImplInner>::clone(&self.inner);

        let task = async move {
            let vol_id = req.get_volume_id();
            // Check arguments
            if vol_id.is_empty() {
                return Err(ArgumentInvalid {
                    context: vec!["volume ID cannot be empty".to_string()],
                });
            }
            let vol_caps = req.get_volume_capabilities();
            if vol_caps.is_empty() {
                return Err(ArgumentInvalid {
                    context: vec![format!(
                        "volume ID={} has no volume capabilities in reqeust",
                        vol_id
                    )],
                });
            }

            self_inner.meta_data.get_volume_by_id(vol_id).await?;

            for cap in vol_caps {
                if !cap.has_mount() && !cap.has_block() {
                    return Err(ArgumentInvalid {
                        context: vec![
                            "cannot have neither mount nor block access type undefined".to_string()
                        ],
                    });
                }
                if cap.has_block() {
                    return Err(ArgumentInvalid {
                        context: vec!["access type block is not supported".to_string()],
                    });
                }
                // TODO: a real driver would check the capabilities of the given volume with
                // the set of requested capabilities.
            }

            let mut r = ValidateVolumeCapabilitiesResponse::new();
            r.mut_confirmed()
                .set_volume_context(req.take_volume_context());
            r.mut_confirmed()
                .set_volume_capabilities(req.take_volume_capabilities());
            r.mut_confirmed().set_parameters(req.take_parameters());

            Ok(r)
        };
        util::spawn_grpc_task(sink, task);
    }

    fn list_volumes(
        &mut self,
        _ctx: RpcContext,
        req: ListVolumesRequest,
        sink: UnarySink<ListVolumesResponse>,
    ) {
        debug!("list_volumes request: {:?}", req);
        let self_inner = Arc::<ControllerImplInner>::clone(&self.inner);

        let task = async move {
            let rpc_type = ControllerServiceCapability_RPC_Type::LIST_VOLUMES;
            if !self_inner.validate_request_capability(rpc_type) {
                return Err(ArgumentInvalid {
                    context: vec![format!("unsupported capability {:?}", rpc_type)],
                });
            }

            let max_entries = req.get_max_entries();
            let starting_token = req.get_starting_token();
            let list_res = self_inner
                .meta_data
                .list_volumes(starting_token, max_entries)
                .await;
            let (vol_vec, next_pos) = match list_res {
                Ok((vol_vec, next_pos)) => (vol_vec, next_pos),
                Err(e) => {
                    warn!(
                        "failed to list volumes from starting position={} and \
                            max entries={}, the error is: {}",
                        starting_token, max_entries, e,
                    );
                    return Err(e);
                }
            };
            let list_size = vol_vec.len();
            let mut r = ListVolumesResponse::new();
            r.set_entries(RepeatedField::from_vec(vol_vec));
            r.set_next_token(next_pos.to_string());
            info!("list volumes size: {}", list_size);
            Ok(r)
        };
        util::spawn_grpc_task(sink, task);
    }

    fn get_capacity(
        &mut self,
        _ctx: RpcContext,
        req: GetCapacityRequest,
        sink: UnarySink<GetCapacityResponse>,
    ) {
        debug!("get_capacity request: {:?}", req);

        util::spawn_grpc_task(sink, async {
            Err(Unimplemented {
                context: vec!["unimplemented".to_string()],
            })
        });
    }

    fn controller_get_capabilities(
        &mut self,
        _ctx: RpcContext,
        req: ControllerGetCapabilitiesRequest,
        sink: UnarySink<ControllerGetCapabilitiesResponse>,
    ) {
        debug!("controller_get_capabilities request: {:?}", req);

        let mut r = ControllerGetCapabilitiesResponse::new();
        r.set_capabilities(RepeatedField::from_vec(self.inner.caps.clone()));
        util::spawn_grpc_task(sink, async { Ok(r) });
    }

    fn create_snapshot(
        &mut self,
        _ctx: RpcContext,
        req: CreateSnapshotRequest,
        sink: UnarySink<CreateSnapshotResponse>,
    ) {
        debug!("create_snapshot request: {:?}", req);
        let self_inner = Arc::<ControllerImplInner>::clone(&self.inner);

        let task = async move {
            let rpc_type = ControllerServiceCapability_RPC_Type::CREATE_DELETE_SNAPSHOT;
            if !self_inner.validate_request_capability(rpc_type) {
                return Err(ArgumentInvalid {
                    context: vec![format!("unsupported capability {:?}", rpc_type)],
                });
            }

            let snap_name = req.get_name();
            if snap_name.is_empty() {
                return Err(ArgumentInvalid {
                    context: vec!["name missing in request".to_string()],
                });
            }
            // Check source volume exists
            let src_vol_id = req.get_source_volume_id();
            if src_vol_id.is_empty() {
                return Err(ArgumentInvalid {
                    context: vec!["source volume ID missing in request".to_string()],
                });
            }

            match self_inner.find_existing_snapshot(&req).await {
                Ok(resp) => {
                    info!(
                        "find existing snapshot ID={} and name={}",
                        resp.get_snapshot().get_snapshot_id(),
                        snap_name,
                    );
                    return Ok(resp);
                }
                Err(e) => {
                    if let SnapshotNotFound { .. } = e {
                        debug!("no snapshot name={} found", snap_name);
                    } else {
                        debug!(
                            "failed to find existing snapshot name={}, the error is: {}",
                            snap_name, e,
                        );
                        return Err(e);
                    }
                }
            }

            let src_vol = self_inner.meta_data.get_volume_by_id(src_vol_id).await?;
            let primary_node_id = src_vol.get_primary_node_id();
            match self_inner.meta_data.get_node_by_id(primary_node_id).await {
                Ok(node) => {
                    let client = MetaData::build_worker_client(&node);
                    client
                        .worker_create_snapshot_async(&req)?
                        .await
                        .with_context(|| {
                            format!(
                                "failed to create snapshot name={} on node ID={}",
                                snap_name, primary_node_id,
                            )
                        })
                }
                Err(e) => {
                    warn!("failed to find node ID={} from etcd", primary_node_id);
                    Err(e)
                }
            }
        };
        util::spawn_grpc_task(sink, task);
    }

    fn delete_snapshot(
        &mut self,
        _ctx: RpcContext,
        req: DeleteSnapshotRequest,
        sink: UnarySink<DeleteSnapshotResponse>,
    ) {
        debug!("delete_snapshot request: {:?}", req);
        let self_inner = Arc::<ControllerImplInner>::clone(&self.inner);

        let task = async move {
            // Check arguments
            let snap_id = req.get_snapshot_id();
            if snap_id.is_empty() {
                return Err(ArgumentInvalid {
                    context: vec!["snapshot ID missing in request".to_string()],
                });
            }

            let rpc_type = ControllerServiceCapability_RPC_Type::CREATE_DELETE_SNAPSHOT;
            if !self_inner.validate_request_capability(rpc_type) {
                return Err(ArgumentInvalid {
                    context: vec![format!("unsupported capability {:?}", rpc_type)],
                });
            }

            // Do not return gRPC error when delete failed for idempotency
            match self_inner.meta_data.get_snapshot_by_id(snap_id).await {
                Ok(snap) => match self_inner.meta_data.get_node_by_id(&snap.node_id).await {
                    Ok(node) => {
                        let client = MetaData::build_worker_client(&node);
                        let worker_delete_res = client
                            .worker_delete_snapshot_async(&req)?
                            .await
                            .with_context(|| {
                                format!(
                                    "failed to delete snapshot ID={} on node ID={}",
                                    snap_id, snap.node_id,
                                )
                            });
                        match worker_delete_res {
                            Ok(_r) => info!("successfully deleted sanpshot ID={}", snap_id),
                            Err(e) => {
                                error!(
                                    "failed to delete snapshot ID={} on node ID={}, the error is: {}",
                                    snap_id,
                                    snap.node_id,
                                    e,
                                );
                            }
                        }
                    }
                    Err(e) => {
                        warn!(
                            "failed to find node ID={} to get work port, the error is: {}",
                            snap.node_id, e
                        );
                    }
                },
                Err(e) => {
                    warn!(
                        "failed to find snapshot ID={} to delete, the error is: {}",
                        snap_id, e
                    );
                }
            }

            let r = DeleteSnapshotResponse::new();
            Ok(r)
        };
        util::spawn_grpc_task(sink, task);
    }

    fn list_snapshots(
        &mut self,
        _ctx: RpcContext,
        req: ListSnapshotsRequest,
        sink: UnarySink<ListSnapshotsResponse>,
    ) {
        debug!("list_snapshots request: {:?}", req);
        let self_inner = Arc::<ControllerImplInner>::clone(&self.inner);

        let task = async move {
            let rpc_type = ControllerServiceCapability_RPC_Type::LIST_SNAPSHOTS;
            if !self_inner.validate_request_capability(rpc_type) {
                return Err(ArgumentInvalid {
                    context: vec![format!("unsupported capability {:?}", rpc_type)],
                });
            }

            // case 1: snapshot ID is not empty, return snapshots that match the snapshot id.
            let snap_id = req.get_snapshot_id();
            if !snap_id.is_empty() {
                match self_inner.meta_data.get_snapshot_by_id(snap_id).await {
                    Ok(snap) => {
                        let r = ControllerImplInner::add_snapshot_to_list_response(&snap)
                            .add_context("failed to generate ListSnapshotsResponse")?;
                        return Ok(r);
                    }
                    Err(e) => {
                        warn!(
                            "failed to list snapshot ID={}, the error is: {}",
                            snap_id, e
                        );
                        if let SnapshotNotFound { .. } = e {
                            return Ok(ListSnapshotsResponse::new());
                        } else {
                            return Err(e);
                        }
                    }
                }
            }

            // case 2: source volume ID is not empty, return snapshots that match the source volume id.
            let src_volume_id = req.get_source_volume_id();
            if !src_volume_id.is_empty() {
                match self_inner
                    .meta_data
                    .get_snapshot_by_src_volume_id(src_volume_id)
                    .await
                {
                    Ok(snap) => {
                        let r = ControllerImplInner::add_snapshot_to_list_response(&snap)
                            .add_context("failed to generate ListSnapshotsResponse")?;
                        return Ok(r);
                    }
                    Err(e) => {
                        warn!(
                            "failed to list snapshot with source volume ID={}, the error is: {}",
                            src_volume_id, e,
                        );
                        if let SnapshotNotFound { .. } = e {
                            return Ok(ListSnapshotsResponse::new());
                        } else {
                            return Err(e);
                        }
                    }
                }
            }

            // case 3: no parameter is set, so return all the snapshots
            let max_entries = req.get_max_entries();
            let starting_token = req.get_starting_token();
            let list_res = self_inner
                .meta_data
                .list_snapshots(starting_token, max_entries)
                .await;
            let (snap_vec, next_pos) = match list_res {
                Ok((snap_vec, next_pos)) => (snap_vec, next_pos),
                Err(e) => {
                    warn!(
                        "failed to list snapshots from starting position={}, \
                            max entries={}, the error is: {}",
                        starting_token, max_entries, e,
                    );
                    return Err(e);
                }
            };

            let mut r = ListSnapshotsResponse::new();
            r.set_entries(RepeatedField::from_vec(snap_vec));
            r.set_next_token(next_pos.to_string());
            Ok(r)
        };
        util::spawn_grpc_task(sink, task);
    }

    fn controller_expand_volume(
        &mut self,
        _ctx: RpcContext,
        req: ControllerExpandVolumeRequest,
        sink: UnarySink<ControllerExpandVolumeResponse>,
    ) {
        debug!("controller_expand_volume request: {:?}", req);
        let self_inner = Arc::<ControllerImplInner>::clone(&self.inner);

        let task = async move {
            let vol_id = req.get_volume_id();
            if vol_id.is_empty() {
                return Err(ArgumentInvalid {
                    context: vec!["volume ID is not provided".to_string()],
                });
            }

            if !req.has_capacity_range() {
                return Err(ArgumentInvalid {
                    context: vec!["capacity range not provided".to_string()],
                });
            }
            let cap_range = req.get_capacity_range();
            let capacity = cap_range.get_required_bytes();
            if capacity > util::MAX_VOLUME_STORAGE_CAPACITY {
                return Err(ArgumentOutOfRange {
                    context: vec![format!(
                        "requested size {} exceeds maximum allowed {}",
                        capacity,
                        util::MAX_VOLUME_STORAGE_CAPACITY,
                    )],
                });
            }

            let mut ex_vol = self_inner.meta_data.get_volume_by_id(vol_id).await?;

            match ex_vol.get_size().cmp(&capacity) {
                Ordering::Less => {
                    let expand_res = self_inner.meta_data.expand(&mut ex_vol, capacity).await;
                    if let Err(e) = expand_res {
                        panic!("failed to expand volume ID={}, the error is: {}", vol_id, e,);
                    }
                }
                Ordering::Greater => {
                    return Err(ArgumentInvalid {
                        context: vec![format!(
                        "capacity={} to expand in request is smaller than the size={} of volume ID={}",
                        capacity, ex_vol.get_size(), vol_id,
                    )]});
                }
                Ordering::Equal => {
                    debug!("capacity equals to volume size, no need to expand");
                }
            }

            let mut r = ControllerExpandVolumeResponse::new();
            r.set_capacity_bytes(capacity);
            r.set_node_expansion_required(true);
            Ok(r)
        };
        util::spawn_grpc_task(sink, task);
    }

    fn controller_get_volume(
        &mut self,
        _ctx: RpcContext,
        req: ControllerGetVolumeRequest,
        sink: UnarySink<ControllerGetVolumeResponse>,
    ) {
        debug!("controller_get_volume request: {:?}", req);

        util::spawn_grpc_task(sink, async {
            Err(Unimplemented {
                context: vec!["unimplemented".to_string()],
            })
        });
    }
}
