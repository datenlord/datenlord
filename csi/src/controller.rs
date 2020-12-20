//! The implementation for CSI controller service

use anyhow::{anyhow, Context};
use grpcio::{RpcContext, RpcStatusCode, UnarySink};
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

/// for `ControllerService` implementation
#[derive(Clone)]
pub struct ControllerImpl {
    /// Inner data
    inner: Arc<ControllerImplInner>,
}

/// Holding `ControllerImpl` inner data
pub struct ControllerImplInner {
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
    pub fn new(meta_data: Arc<MetaData>) -> Self {
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
    ) -> Result<Option<CreateVolumeResponse>, (RpcStatusCode, anyhow::Error)> {
        let vol_name = req.get_name();
        let get_vol_res = self.meta_data.get_volume_by_name(vol_name).await;
        if let Some(ex_vol) = get_vol_res {
            // It means the volume with the same name already exists
            // need to check if the size of existing volume is the same as in new
            // request
            let volume_size = req.get_capacity_range().get_required_bytes();
            if ex_vol.get_size() != volume_size {
                return Err((
                    RpcStatusCode::ALREADY_EXISTS,
                    anyhow!(format!(
                        "volume with the same name={} already exist but with different size",
                        vol_name,
                    )),
                ));
            }

            if req.has_volume_content_source() {
                let ex_vol_content_source = if let Some(ref vcs) = ex_vol.content_source {
                    vcs
                } else {
                    return Err((
                        RpcStatusCode::ALREADY_EXISTS,
                        anyhow!(format!(
                            "existing volume ID={} doesn't have content source",
                            ex_vol.vol_id,
                        )),
                    ));
                };

                let volume_source = req.get_volume_content_source();
                if let Some(ref volume_source_type) = volume_source.field_type {
                    match *volume_source_type {
                        VolumeContentSource_oneof_type::snapshot(ref snapshot_source) => {
                            let parent_snap_id = snapshot_source.get_snapshot_id();
                            if let VolumeSource::Snapshot(ref psid) = *ex_vol_content_source {
                                if psid != parent_snap_id {
                                    return Err((
                                        RpcStatusCode::ALREADY_EXISTS,
                                        anyhow!(format!(
                                            "existing volume ID={} has parent snapshot ID={}, \
                                            but VolumeContentSource_SnapshotSource has \
                                            parent snapshot ID={}",
                                            ex_vol.vol_id, psid, parent_snap_id,
                                        )),
                                    ));
                                }
                            } else {
                                return Err((
                                    RpcStatusCode::ALREADY_EXISTS,
                                    anyhow!(format!(
                                        "existing volume ID={} doesn't have parent snapshot ID",
                                        ex_vol.vol_id
                                    )),
                                ));
                            }
                        }
                        VolumeContentSource_oneof_type::volume(ref volume_source) => {
                            let parent_vol_id = volume_source.get_volume_id();
                            if let VolumeSource::Volume(ref pvid) = *ex_vol_content_source {
                                if pvid != parent_vol_id {
                                    return Err((
                                        RpcStatusCode::ALREADY_EXISTS,
                                        anyhow!(format!(
                                            "existing volume ID={} has parent volume ID={}, \
                                            but VolumeContentSource_VolumeSource has \
                                            parent volume ID={}",
                                            ex_vol.vol_id, pvid, parent_vol_id,
                                        )),
                                    ));
                                }
                            } else {
                                return Err((
                                    RpcStatusCode::ALREADY_EXISTS,
                                    anyhow!(format!(
                                        "existing volume ID={} doesn't have parent volume ID",
                                        ex_vol.vol_id,
                                    )),
                                ));
                            }
                        }
                    }
                }
            }
            // Return existing volume
            // TODO: make sure that volume still exists?
            let resp = util::build_create_volume_response(req, &ex_vol.vol_id, &ex_vol.node_id);
            Ok(Some(resp))
        } else {
            debug!("no volume with name={} exists", vol_name);
            Ok(None)
        }
    }

    /// Check for already existing snapshot name, and if found check for the
    /// requested source volume ID matches snapshot that has been created
    async fn find_existing_snapshot(
        &self,
        req: &CreateSnapshotRequest,
    ) -> Result<Option<CreateSnapshotResponse>, (RpcStatusCode, anyhow::Error)> {
        if let Some(ex_snap) = self.meta_data.get_snapshot_by_name(req.get_name()).await {
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
                .context(format!(
                    "failed to build CreateSnapshotResponse at controller ID={}",
                    node_id,
                ));
                match build_resp_res {
                    Ok(resp) => {
                        info!(
                            "find existing snapshot ID={} and name={}",
                            ex_snap.snap_id, snap_name,
                        );
                        Ok(Some(resp))
                    }
                    Err(e) => Err((RpcStatusCode::INTERNAL, e)),
                }
            } else {
                Err((
                    RpcStatusCode::ALREADY_EXISTS,
                    anyhow!(format!(
                        "snapshot with the same name={} exists on node ID={} \
                            but of different source volume ID",
                        snap_name, node_id,
                    )),
                ))
            }
        } else {
            debug!("no snapshot with name={} exists", req.get_name());
            Ok(None)
        }
    }

    /// Build list snapshot response
    fn add_snapshot_to_list_response(
        snap: &DatenLordSnapshot,
    ) -> anyhow::Result<ListSnapshotsResponse> {
        let mut entry = ListSnapshotsResponse_Entry::new();
        entry.mut_snapshot().set_size_bytes(snap.size_bytes);
        entry.mut_snapshot().set_snapshot_id(snap.snap_id.clone());
        entry
            .mut_snapshot()
            .set_source_volume_id(snap.vol_id.clone());
        entry.mut_snapshot().set_creation_time(
            util::generate_proto_timestamp(&snap.creation_time)
                .context("failed to convert to proto timestamp")?,
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
    ) -> Result<CreateVolumeResponse, (RpcStatusCode, anyhow::Error)> {
        let worker_node = match self.meta_data.select_node(req).await {
            Ok(n) => n,
            Err(e) => {
                error!(
                    "failed to select a node, the error is: {}",
                    util::format_anyhow_error(&e),
                );
                return Err((RpcStatusCode::NOT_FOUND, e));
            }
        };
        let client = MetaData::build_worker_client(&worker_node);
        let create_res = client.worker_create_volume(req);
        match create_res {
            Ok(resp) => Ok(resp),
            Err(e) => {
                match e {
                    grpcio::Error::RpcFailure(ref s) => Err((
                        s.status,
                        anyhow::Error::new(e),
                        // if let Some(m) = s.details {
                        //     m
                        // } else {
                        //     format!(
                        //         "failed to create volume by worker at {}",
                        //         worker_node.node_id,
                        //     )
                        // },
                    )),
                    e @ grpcio::Error::Codec(..)
                    | e @ grpcio::Error::CallFailure(..)
                    | e @ grpcio::Error::RpcFinished(..)
                    | e @ grpcio::Error::RemoteStopped
                    | e @ grpcio::Error::ShutdownFailed
                    | e @ grpcio::Error::BindFail(..)
                    | e @ grpcio::Error::QueueShutdown
                    | e @ grpcio::Error::GoogleAuthenticationFailed
                    | e @ grpcio::Error::InvalidMetadata(..) => Err((
                        RpcStatusCode::INTERNAL,
                        anyhow::Error::new(e),
                        // format!("failed to create volume, the error is: {}", e),
                    )),
                }
            }
        }
    }

    /// The pre-check helper function for `create_volume`
    fn create_volume_pre_check(
        &self,
        req: &CreateVolumeRequest,
    ) -> Result<(), (RpcStatusCode, anyhow::Error)> {
        let rpc_type = ControllerServiceCapability_RPC_Type::CREATE_DELETE_VOLUME;
        if !self.validate_request_capability(rpc_type) {
            return Err((
                RpcStatusCode::INVALID_ARGUMENT,
                anyhow!(format!("unsupported capability {:?}", rpc_type)),
            ));
        }

        let vol_name = req.get_name();
        if vol_name.is_empty() {
            return Err((
                RpcStatusCode::INVALID_ARGUMENT,
                anyhow!("name missing in request"),
            ));
        }

        let req_caps = req.get_volume_capabilities();
        if req_caps.is_empty() {
            return Err((
                RpcStatusCode::INVALID_ARGUMENT,
                anyhow!("volume capabilities missing in request"),
            ));
        }

        let access_type_block = req_caps.iter().any(VolumeCapability::has_block);
        let access_mode_multi_writer = req_caps.iter().any(|vc| {
            let vol_access_mode = vc.get_access_mode().get_mode();
            vol_access_mode == VolumeCapability_AccessMode_Mode::MULTI_NODE_READER_ONLY
                || vol_access_mode == VolumeCapability_AccessMode_Mode::MULTI_NODE_MULTI_WRITER
                || vol_access_mode == VolumeCapability_AccessMode_Mode::MULTI_NODE_SINGLE_WRITER
        });
        if access_type_block {
            return Err((
                RpcStatusCode::INVALID_ARGUMENT,
                anyhow!("access type block not supported"),
            ));
        }
        if access_mode_multi_writer {
            return Err((
                RpcStatusCode::INVALID_ARGUMENT,
                anyhow!(
                    "access mode MULTI_NODE_SINGLE_WRITER and \
                        MULTI_NODE_MULTI_WRITER not supported"
                ),
            ));
        }

        let volume_size = req.get_capacity_range().get_required_bytes();
        if volume_size > util::MAX_VOLUME_STORAGE_CAPACITY {
            return Err((
                RpcStatusCode::OUT_OF_RANGE,
                anyhow!(format!(
                    "requested size {} exceeds maximum allowed {}",
                    volume_size,
                    util::MAX_VOLUME_STORAGE_CAPACITY,
                )),
            ));
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

        let task =
            async move {
                self_inner.create_volume_pre_check(&req)?;
                // let rpc_type = ControllerServiceCapability_RPC_Type::CREATE_DELETE_VOLUME;
                // if !self.validate_request_capability(rpc_type) {
                //     return util::fail(
                //         &ctx,
                //         sink,
                //         RpcStatusCode::INVALID_ARGUMENT,
                //         anyhow!(format!("unsupported capability {:?}", rpc_type)),
                //     );
                // }

                // let vol_name = req.get_name();
                // if vol_name.is_empty() {
                //     return util::fail(
                //         &ctx,
                //         sink,
                //         RpcStatusCode::INVALID_ARGUMENT,
                //         anyhow!("name missing in request"),
                //     );
                // }

                // let req_caps = req.get_volume_capabilities();
                // if req_caps.is_empty() {
                //     return util::fail(
                //         &ctx,
                //         sink,
                //         RpcStatusCode::INVALID_ARGUMENT,
                //         anyhow!("volume capabilities missing in request"),
                //     );
                // }

                // let access_type_block = req_caps.iter().any(VolumeCapability::has_block);
                // let access_mode_multi_writer = req_caps.iter().any(|vc| {
                //     vc.get_access_mode().get_mode()
                //         == VolumeCapability_AccessMode_Mode::MULTI_NODE_MULTI_WRITER
                //         || vc.get_access_mode().get_mode()
                //             == VolumeCapability_AccessMode_Mode::MULTI_NODE_SINGLE_WRITER
                // });
                // if access_type_block {
                //     return util::fail(
                //         &ctx,
                //         sink,
                //         RpcStatusCode::INVALID_ARGUMENT,
                //         anyhow!("access type block not supported"),
                //     );
                // }
                // if access_mode_multi_writer {
                //     return util::fail(
                //         &ctx,
                //         sink,
                //         RpcStatusCode::INVALID_ARGUMENT,
                //         anyhow!(
                //             "access mode MULTI_NODE_SINGLE_WRITER and \
                //                 MULTI_NODE_MULTI_WRITER not supported"
                //         ),
                //     );
                // }

                // let volume_size = req.get_capacity_range().get_required_bytes();
                // if volume_size > util::MAX_VOLUME_STORAGE_CAPACITY {
                //     return util::fail(
                //         &ctx,
                //         sink,
                //         RpcStatusCode::OUT_OF_RANGE,
                //         anyhow!(format!(
                //             "requested size {} exceeds maximum allowed {}",
                //             volume_size,
                //             util::MAX_VOLUME_STORAGE_CAPACITY,
                //         )),
                //     );
                // }
                let vol_name = req.get_name();
                let resp_opt = self_inner.find_existing_volume(&req).await.map_err(
                    |(rpc_status_code, e)| {
                        debug!(
                            "failed to find existing volume name={}, the error is: {}",
                            vol_name,
                            util::format_anyhow_error(&e),
                        );
                        (rpc_status_code, e)
                    },
                )?;

                if let Some(resp) = resp_opt {
                    debug!(
                        "found existing volume ID={} and name={}",
                        resp.get_volume().get_volume_id(),
                        vol_name,
                    );
                    return Ok(resp);
                } else {
                    debug!("no volume name={} found", vol_name);
                }

                self_inner
                    .worker_create_volume(&req)
                    .await
                    .map_err(|(rpc_status_code, e)| {
                        debug_assert_ne!(
                            rpc_status_code,
                            RpcStatusCode::OK,
                            "the RpcStatusCode should not be OK when error",
                        );
                        debug!(
                            "failed to create volume, the error is: {}",
                            util::format_anyhow_error(&e)
                        );
                        (rpc_status_code, e)
                    })
            };
        smol::spawn(async move {
            let result = task.await;
            match result {
                Err((rpc_status_code, e)) => util::async_fail(sink, rpc_status_code, &e).await,
                Ok(resp) => util::async_success(sink, resp).await,
            }
        })
        .detach();
    }

    fn delete_volume(
        &mut self,
        ctx: RpcContext,
        req: DeleteVolumeRequest,
        sink: UnarySink<DeleteVolumeResponse>,
    ) {
        debug!("delete_volume request: {:?}", req);

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

        let rpc_type = ControllerServiceCapability_RPC_Type::CREATE_DELETE_VOLUME;
        if !self.inner.validate_request_capability(rpc_type) {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                &anyhow!(format!("unsupported capability {:?}", rpc_type)),
            );
        }

        // Do not return gRPC error when delete failed for idempotency
        let vol_res = smol::block_on(async { self.inner.meta_data.get_volume_by_id(vol_id).await });
        if let Some(vol) = vol_res {
            let node_res =
                smol::block_on(async { self.inner.meta_data.get_node_by_id(&vol.node_id).await });
            if let Some(node) = node_res {
                let client = MetaData::build_worker_client(&node);
                let worker_delete_res = client.worker_delete_volume(&req).context(format!(
                    "failed to delete volume ID={} on node ID={}",
                    vol_id, vol.node_id,
                ));
                match worker_delete_res {
                    Ok(_) => info!("successfully deleted volume ID={}", vol_id),
                    Err(e) => {
                        warn!(
                            "failed to delete volume ID={} on node ID={}, the error is: {}",
                            vol_id,
                            vol.node_id,
                            util::format_anyhow_error(&e),
                        );
                    }
                }
            } else {
                warn!("failed to find node ID={} to get work port", vol.node_id);
            }
        } else {
            warn!(
                "failed to find volume ID={} to delete on controller ID={}",
                vol_id,
                self.inner.meta_data.get_node_id(),
            );
        }
        let r = DeleteVolumeResponse::new();
        util::success(&ctx, sink, r)
    }

    fn controller_publish_volume(
        &mut self,
        ctx: RpcContext,
        req: ControllerPublishVolumeRequest,
        sink: UnarySink<ControllerPublishVolumeResponse>,
    ) {
        debug!("controller_publish_volume request: {:?}", req);

        util::fail(
            &ctx,
            sink,
            RpcStatusCode::UNIMPLEMENTED,
            &anyhow!("unimplemented"),
        )
    }

    fn controller_unpublish_volume(
        &mut self,
        ctx: RpcContext,
        req: ControllerUnpublishVolumeRequest,
        sink: UnarySink<ControllerUnpublishVolumeResponse>,
    ) {
        debug!("controller_unpublish_volume request: {:?}", req);

        util::fail(
            &ctx,
            sink,
            RpcStatusCode::UNIMPLEMENTED,
            &anyhow!("unimplemented"),
        )
    }

    fn validate_volume_capabilities(
        &mut self,
        ctx: RpcContext,
        mut req: ValidateVolumeCapabilitiesRequest,
        sink: UnarySink<ValidateVolumeCapabilitiesResponse>,
    ) {
        debug!("validate_volume_capabilities request: {:?}", req);

        let vol_id = req.get_volume_id();
        // Check arguments
        if vol_id.is_empty() {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                &anyhow!("volume ID cannot be empty"),
            );
        }
        let vol_caps = req.get_volume_capabilities();
        if vol_caps.is_empty() {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                &anyhow!(format!(
                    "volume ID={} has no volume capabilities in reqeust",
                    vol_id,
                )),
            );
        }

        let vol_res = smol::block_on(async { self.inner.meta_data.get_volume_by_id(vol_id).await });
        if vol_res.is_none() {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::NOT_FOUND,
                &anyhow!(format!("failed to find volume ID={}", vol_id)),
            );
        }

        for cap in vol_caps {
            if !cap.has_mount() && !cap.has_block() {
                return util::fail(
                    &ctx,
                    sink,
                    RpcStatusCode::INVALID_ARGUMENT,
                    &anyhow!("cannot have neither mount nor block access type undefined"),
                );
            }
            if cap.has_block() {
                return util::fail(
                    &ctx,
                    sink,
                    RpcStatusCode::INVALID_ARGUMENT,
                    &anyhow!("access type block is not supported"),
                );
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

        util::success(&ctx, sink, r)
    }

    fn list_volumes(
        &mut self,
        ctx: RpcContext,
        req: ListVolumesRequest,
        sink: UnarySink<ListVolumesResponse>,
    ) {
        debug!("list_volumes request: {:?}", req);

        let rpc_type = ControllerServiceCapability_RPC_Type::LIST_VOLUMES;
        if !self.inner.validate_request_capability(rpc_type) {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                &anyhow!(format!("unsupported capability {:?}", rpc_type)),
            );
        }

        let max_entries = req.get_max_entries();
        let starting_token = req.get_starting_token();
        let list_res = self
            .inner
            .meta_data
            .list_volumes(starting_token, max_entries);
        let (vol_vec, next_pos) = match list_res {
            Ok((vol_vec, next_pos)) => (vol_vec, next_pos),
            Err((rpc_status_code, e)) => {
                debug_assert_ne!(
                    rpc_status_code,
                    RpcStatusCode::OK,
                    "the RpcStatusCode should not be OK when error",
                );
                warn!(
                    "failed to list volumes from starting position={} and \
                        max entries={}, the error is: {}",
                    starting_token,
                    max_entries,
                    util::format_anyhow_error(&e),
                );
                return util::fail(&ctx, sink, rpc_status_code, &e);
            }
        };
        let list_size = vol_vec.len();
        let mut r = ListVolumesResponse::new();
        r.set_entries(RepeatedField::from_vec(vol_vec));
        r.set_next_token(next_pos.to_string());
        info!("list volumes size: {}", list_size);
        util::success(&ctx, sink, r)
    }

    fn get_capacity(
        &mut self,
        ctx: RpcContext,
        req: GetCapacityRequest,
        sink: UnarySink<GetCapacityResponse>,
    ) {
        debug!("get_capacity request: {:?}", req);

        util::fail(
            &ctx,
            sink,
            RpcStatusCode::UNIMPLEMENTED,
            &anyhow!("unimplemented"),
        )
    }

    fn controller_get_capabilities(
        &mut self,
        ctx: RpcContext,
        req: ControllerGetCapabilitiesRequest,
        sink: UnarySink<ControllerGetCapabilitiesResponse>,
    ) {
        debug!("controller_get_capabilities request: {:?}", req);

        let mut r = ControllerGetCapabilitiesResponse::new();
        r.set_capabilities(RepeatedField::from_vec(self.inner.caps.clone()));
        util::success(&ctx, sink, r)
    }

    fn create_snapshot(
        &mut self,
        ctx: RpcContext,
        req: CreateSnapshotRequest,
        sink: UnarySink<CreateSnapshotResponse>,
    ) {
        debug!("create_snapshot request: {:?}", req);

        let rpc_type = ControllerServiceCapability_RPC_Type::CREATE_DELETE_SNAPSHOT;
        if !self.inner.validate_request_capability(rpc_type) {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                &anyhow!(format!("unsupported capability {:?}", rpc_type)),
            );
        }

        let snap_name = req.get_name();
        if snap_name.is_empty() {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                &anyhow!("name missing in request"),
            );
        }
        // Check source volume exists
        let src_vol_id = req.get_source_volume_id();
        if src_vol_id.is_empty() {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                &anyhow!("source volume ID missing in request"),
            );
        }

        let find_res = smol::block_on(async { self.inner.find_existing_snapshot(&req).await });
        match find_res {
            Ok(resp_opt) => {
                if let Some(resp) = resp_opt {
                    info!(
                        "find existing snapshot ID={} and name={}",
                        resp.get_snapshot().get_snapshot_id(),
                        snap_name,
                    );
                    return util::success(&ctx, sink, resp);
                } else {
                    debug!("no snapshot name={} found", snap_name);
                }
            }
            Err((rpc_status_code, e)) => {
                debug!(
                    "failed to find existing snapshot name={}, the error is: {}",
                    snap_name,
                    util::format_anyhow_error(&e),
                );
                return util::fail(&ctx, sink, rpc_status_code, &e);
            }
        }

        match smol::block_on(async { self.inner.meta_data.get_volume_by_id(src_vol_id).await }) {
            Some(src_vol) => {
                let node_res = smol::block_on(async {
                    self.inner.meta_data.get_node_by_id(&src_vol.node_id).await
                });
                if let Some(node) = node_res {
                    let client = MetaData::build_worker_client(&node);
                    let create_res = client.worker_create_snapshot(&req).context(format!(
                        "failed to create snapshot name={} on node ID={}",
                        snap_name, src_vol.node_id,
                    ));
                    match create_res {
                        Ok(r) => util::success(&ctx, sink, r),
                        Err(e) => util::fail(&ctx, sink, RpcStatusCode::INTERNAL, &e),
                    }
                } else {
                    warn!("failed to find node ID={} from etcd", src_vol.node_id);
                }
            }
            None => util::fail(
                &ctx,
                sink,
                RpcStatusCode::INTERNAL,
                &anyhow!(format!("failed to find source volume ID={}", src_vol_id)),
            ),
        }
    }

    fn delete_snapshot(
        &mut self,
        ctx: RpcContext,
        req: DeleteSnapshotRequest,
        sink: UnarySink<DeleteSnapshotResponse>,
    ) {
        debug!("delete_snapshot request: {:?}", req);

        // Check arguments
        let snap_id = req.get_snapshot_id();
        if snap_id.is_empty() {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                &anyhow!("snapshot ID missing in request"),
            );
        }

        let rpc_type = ControllerServiceCapability_RPC_Type::CREATE_DELETE_SNAPSHOT;
        if !self.inner.validate_request_capability(rpc_type) {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                &anyhow!(format!("unsupported capability {:?}", rpc_type)),
            );
        }

        // Do not return gRPC error when delete failed for idempotency
        let snap_res =
            smol::block_on(async { self.inner.meta_data.get_snapshot_by_id(snap_id).await });
        if let Some(snap) = snap_res {
            let node_res =
                smol::block_on(async { self.inner.meta_data.get_node_by_id(&snap.node_id).await });
            if let Some(node) = node_res {
                let client = MetaData::build_worker_client(&node);
                let worker_delete_res = client.worker_delete_snapshot(&req).context(format!(
                    "failed to delete snapshot ID={} on node ID={}",
                    snap_id, snap.node_id,
                ));
                match worker_delete_res {
                    Ok(_r) => info!("successfully deleted sanpshot ID={}", snap_id),
                    Err(e) => {
                        error!(
                            "failed to delete snapshot ID={} on node ID={}, the error is: {}",
                            snap_id,
                            snap.node_id,
                            util::format_anyhow_error(&e),
                        );
                    }
                }
            } else {
                warn!("failed to find node ID={} to get work port", snap.node_id);
            }
        } else {
            warn!("failed to find snapshot ID={} to delete", snap_id);
        }

        let r = DeleteSnapshotResponse::new();
        util::success(&ctx, sink, r)
    }

    fn list_snapshots(
        &mut self,
        ctx: RpcContext,
        req: ListSnapshotsRequest,
        sink: UnarySink<ListSnapshotsResponse>,
    ) {
        debug!("list_snapshots request: {:?}", req);

        let rpc_type = ControllerServiceCapability_RPC_Type::LIST_SNAPSHOTS;
        if !self.inner.validate_request_capability(rpc_type) {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                &anyhow!(format!("unsupported capability {:?}", rpc_type)),
            );
        }

        // case 1: snapshot ID is not empty, return snapshots that match the snapshot id.
        let snap_id = req.get_snapshot_id();
        if !snap_id.is_empty() {
            let snap_res =
                smol::block_on(async { self.inner.meta_data.get_snapshot_by_id(snap_id).await });
            if let Some(snap) = snap_res {
                let add_res = ControllerImplInner::add_snapshot_to_list_response(&snap)
                    .context("failed to generate ListSnapshotsResponse");
                let r = match add_res {
                    Ok(resp) => resp,
                    Err(e) => {
                        return util::fail(&ctx, sink, RpcStatusCode::INTERNAL, &e);
                    }
                };

                return util::success(&ctx, sink, r);
            } else {
                warn!("failed to list snapshot ID={}", snap_id);
                let r = ListSnapshotsResponse::new();
                return util::success(&ctx, sink, r);
            }
        }

        // case 2: source volume ID is not empty, return snapshots that match the source volume id.
        let src_volume_id = req.get_source_volume_id();
        if !src_volume_id.is_empty() {
            let snap_res = smol::block_on(async {
                self.inner
                    .meta_data
                    .get_snapshot_by_src_volume_id(src_volume_id)
                    .await
            });
            if let Some(snap) = snap_res {
                let add_res = ControllerImplInner::add_snapshot_to_list_response(&snap)
                    .context("failed to generate ListSnapshotsResponse");
                let r = match add_res {
                    Ok(resp) => resp,
                    Err(e) => {
                        return util::fail(&ctx, sink, RpcStatusCode::INTERNAL, &e);
                    }
                };

                return util::success(&ctx, sink, r);
            } else {
                warn!(
                    "failed to list snapshot with source volume ID={}",
                    src_volume_id,
                );
                let r = ListSnapshotsResponse::new();
                return util::success(&ctx, sink, r);
            }
        }

        // case 3: no parameter is set, so return all the snapshots
        let max_entries = req.get_max_entries();
        let starting_token = req.get_starting_token();
        let list_res = self
            .inner
            .meta_data
            .list_snapshots(starting_token, max_entries);
        let (snap_vec, next_pos) = match list_res {
            Ok((snap_vec, next_pos)) => (snap_vec, next_pos),
            Err((rpc_status_code, e)) => {
                debug_assert_ne!(
                    rpc_status_code,
                    RpcStatusCode::OK,
                    "the RpcStatusCode should not be OK when error",
                );
                warn!(
                    "failed to list snapshots from starting position={}, \
                        max entries={}, the error is: {}",
                    starting_token,
                    max_entries,
                    util::format_anyhow_error(&e),
                );
                return util::fail(&ctx, sink, rpc_status_code, &e);
            }
        };

        let mut r = ListSnapshotsResponse::new();
        r.set_entries(RepeatedField::from_vec(snap_vec));
        r.set_next_token(next_pos.to_string());
        util::success(&ctx, sink, r)
    }

    fn controller_expand_volume(
        &mut self,
        ctx: RpcContext,
        req: ControllerExpandVolumeRequest,
        sink: UnarySink<ControllerExpandVolumeResponse>,
    ) {
        debug!("controller_expand_volume request: {:?}", req);

        let vol_id = req.get_volume_id();
        if vol_id.is_empty() {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                &anyhow!("volume ID not provided"),
            );
        }

        if !req.has_capacity_range() {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                &anyhow!("capacity range not provided"),
            );
        }
        let cap_range = req.get_capacity_range();
        let capacity = cap_range.get_required_bytes();
        if capacity > util::MAX_VOLUME_STORAGE_CAPACITY {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::OUT_OF_RANGE,
                &anyhow!(format!(
                    "requested capacity {} exceeds maximum allowed {}",
                    capacity,
                    util::MAX_VOLUME_STORAGE_CAPACITY,
                )),
            );
        }

        let vol_res = smol::block_on(async { self.inner.meta_data.get_volume_by_id(vol_id).await });
        let mut ex_vol = if let Some(v) = vol_res {
            v
        } else {
            return util::fail(
                &ctx,
                sink,
                // Assume not found error
                RpcStatusCode::NOT_FOUND,
                &anyhow!(format!("failed to find volume ID={}", vol_id)),
            );
        };

        match ex_vol.get_size().cmp(&capacity) {
            Ordering::Less => {
                let expand_res = self.inner.meta_data.expand(&mut ex_vol, capacity);
                if let Err(e) = expand_res {
                    panic!(
                        "failed to expand volume ID={}, the error is: {}",
                        vol_id,
                        util::format_anyhow_error(&e),
                    );
                }
            }
            Ordering::Greater => {
                return util::fail(&ctx, sink, RpcStatusCode::INVALID_ARGUMENT, &anyhow!(format!(
                        "capacity={} to expand in request is smaller than the size={} of volume ID={}",
                        capacity, ex_vol.get_size(), vol_id,
                    )));
            }
            Ordering::Equal => {
                debug!("capacity equals to volume size, no need to expand");
            }
        }

        let mut r = ControllerExpandVolumeResponse::new();
        r.set_capacity_bytes(capacity);
        r.set_node_expansion_required(true);
        util::success(&ctx, sink, r)
    }

    fn controller_get_volume(
        &mut self,
        ctx: RpcContext,
        req: ControllerGetVolumeRequest,
        sink: UnarySink<ControllerGetVolumeResponse>,
    ) {
        debug!("controller_get_volume request: {:?}", req);

        util::fail(
            &ctx,
            sink,
            RpcStatusCode::UNIMPLEMENTED,
            &anyhow!("unimplemented"),
        )
    }
}
