//! The implementation for `DatenLord` worker service

use anyhow::Context;
use grpcio::{RpcContext, RpcStatusCode, UnarySink};
use log::{debug, info};
use std::sync::Arc;
use uuid::Uuid;

use super::csi::{
    CreateSnapshotRequest, CreateSnapshotResponse, CreateVolumeRequest, CreateVolumeResponse,
    DeleteSnapshotRequest, DeleteSnapshotResponse, DeleteVolumeRequest, DeleteVolumeResponse,
};
use super::datenlord_worker_grpc::Worker;
use super::meta_data::{DatenLordVolume, MetaData, VolumeSource};
use super::util;

/// for `DatenLord` worker implementation
#[derive(Clone)]
pub struct WorkerImpl {
    /// Inner data
    inner: Arc<WorkerImplInner>,
}

/// Holding `WorkerImpl` inner data
struct WorkerImplInner {
    /// Volume meta data for this worker
    meta_data: Arc<MetaData>,
}

impl WorkerImpl {
    /// Create `WorkImpl`
    pub fn new(meta_data: Arc<MetaData>) -> Self {
        Self {
            inner: Arc::new(WorkerImplInner::new(meta_data)),
        }
    }
}

impl WorkerImplInner {
    /// Create `WorkerImpl`
    pub fn new(meta_data: Arc<MetaData>) -> Self {
        Self { meta_data }
    }
    /// Build volume from either snapshot or another volume
    async fn build_volume_from_source(
        &self,
        vol_id: &str,
        vol_name: &str,
        vol_size: i64,
        content_source: &VolumeSource,
    ) -> Result<(), (RpcStatusCode, anyhow::Error)> {
        match *content_source {
            VolumeSource::Snapshot(ref source_snapshot_id) => {
                self.meta_data
                    .copy_volume_from_snapshot(vol_size, source_snapshot_id, &vol_id.to_string()).await
                    .map_err(|(rsc, anyhow_err)| {
                        (
                            rsc,
                            anyhow_err.context(format!(
                                "failed to populate volume ID={} and name={} from source snapshot ID={}",
                                vol_id, vol_name, source_snapshot_id,
                            )),
                        )
                    })?;
                info!(
                    "successfully populated volume ID={} and name={} \
                        from source snapshot ID={} on node ID={}",
                    vol_id,
                    vol_name,
                    source_snapshot_id,
                    self.meta_data.get_node_id(),
                );
                Ok(())
            }
            VolumeSource::Volume(ref source_volume_id) => {
                self.meta_data
                    .copy_volume_from_volume(vol_size, source_volume_id, &vol_id.to_string())
                    .await
                    .map_err(|(rsc, anyhow_err)| {
                        (
                            rsc,
                            anyhow_err.context(format!(
                                "failed to populate volume ID={} and name={} \
                                    from source volume ID={} on node ID={}",
                                vol_id,
                                vol_name,
                                source_volume_id,
                                self.meta_data.get_node_id(),
                            )),
                        )
                    })?;
                info!(
                    "successfully populated volume ID={} and name={} \
                        from source volume ID={} on node ID={}",
                    vol_id,
                    vol_name,
                    source_volume_id,
                    self.meta_data.get_node_id(),
                );
                Ok(())
            }
        }
    }
}

impl Worker for WorkerImpl {
    fn worker_create_volume(
        &mut self,
        _ctx: RpcContext,
        req: CreateVolumeRequest,
        sink: UnarySink<CreateVolumeResponse>,
    ) {
        debug!("worker create_volume request: {:?}", req);
        let self_inner = Arc::<WorkerImplInner>::clone(&self.inner);

        let task = async move {
            let vol_id = Uuid::new_v4();
            let vol_id_str = vol_id.to_string();
            let vol_name = req.get_name();
            let vol_size = req.get_capacity_range().get_required_bytes();

            let volume = DatenLordVolume::build_from_create_volume_req(
                &req,
                &vol_id_str,
                self_inner.meta_data.get_node_id(),
                &self_inner.meta_data.get_volume_path(&vol_id_str),
            )
            .context(format!(
                "failed to create volume ID={} and name={} on node ID={}",
                vol_id,
                vol_name,
                self_inner.meta_data.get_node_id(),
            ))
            .map_err(|err| (RpcStatusCode::INTERNAL, err))?;

            if let Some(ref content_source) = volume.content_source {
                self_inner
                    .build_volume_from_source(&vol_id_str, vol_name, vol_size, content_source)
                    .await
                    .map_err(|(rsc, anyhow_err)| {
                        let err = anyhow_err.context("failed to create volume from source");
                        debug!(
                            "failed to create volume from source, the error is: {}",
                            util::format_anyhow_error(&err),
                        );
                        (rsc, err)
                    })?;
            }

            info!(
                "created volume ID={} and name={} on node ID={:?}",
                volume.vol_id,
                vol_name,
                self_inner.meta_data.get_node_id(),
            );
            let add_res = self_inner
                .meta_data
                .add_volume_meta_data(&volume.vol_id, &volume)
                .await;
            debug_assert!(
                add_res.is_ok(),
                "volume with the same ID={} exists on node ID={}, impossible case",
                vol_id,
                self_inner.meta_data.get_node_id(),
            );

            let r = util::build_create_volume_response(
                &req,
                &vol_id.to_string(),
                self_inner.meta_data.get_node_id(),
            );
            Ok(r)
        };
        util::spawn_grpc_task(sink, task);
    }

    fn worker_delete_volume(
        &mut self,
        _ctx: RpcContext,
        req: DeleteVolumeRequest,
        sink: UnarySink<DeleteVolumeResponse>,
    ) {
        debug!("worker delete_volume request: {:?}", req);
        let self_inner = Arc::<WorkerImplInner>::clone(&self.inner);

        let task = async move {
            let vol_id = req.get_volume_id();
            let delete_res = self_inner
                .meta_data
                .delete_volume_meta_data(vol_id)
                .await
                .context(format!(
                    "failed to find the volume ID={} to delete on node ID={}",
                    vol_id,
                    self_inner.meta_data.get_node_id(),
                ));
            match delete_res {
                Ok(volume) => {
                    let del_res = volume.delete_directory();
                    if let Err(e) = del_res {
                        panic!(
                        "failed to delete volume ID={} directory on node ID={}, the error is: {}",
                        vol_id,
                        self_inner.meta_data.get_node_id(),
                        util::format_anyhow_error(&e),
                    );
                    }
                    debug!(
                        "successfully delete volume ID={} on node ID={}",
                        vol_id,
                        self_inner.meta_data.get_node_id(),
                    );
                    let r = DeleteVolumeResponse::new();
                    Ok(r)
                }
                Err(e) => Err((RpcStatusCode::NOT_FOUND, e)),
            }
        };
        util::spawn_grpc_task(sink, task);
    }

    fn worker_create_snapshot(
        &mut self,
        _ctx: RpcContext,
        req: CreateSnapshotRequest,
        sink: UnarySink<CreateSnapshotResponse>,
    ) {
        debug!("worker create_snapshot request: {:?}", req);
        let self_inner = Arc::<WorkerImplInner>::clone(&self.inner);

        let task = async move {
            let snap_id = Uuid::new_v4();
            let snap_id_str = snap_id.to_string();
            let snap_name = req.get_name();
            let src_volume_id = req.get_source_volume_id();
            let node_id = self_inner.meta_data.get_node_id();

            let build_snap_res = self_inner
                .meta_data
                .build_snapshot_from_volume(src_volume_id, &snap_id_str, snap_name)
                .await
                .context(format!(
                    "failed to create snapshot ID={} on node ID={}",
                    snap_id_str, node_id,
                ));
            match build_snap_res {
                Ok(snapshot) => {
                    let build_resp_res = util::build_create_snapshot_response(
                        &req,
                        &snap_id_str,
                        &snapshot.creation_time,
                        snapshot.size_bytes,
                    )
                    .context(format!(
                        "failed to build CreateSnapshotResponse on node ID={}",
                        node_id,
                    ));
                    match build_resp_res {
                        Ok(r) => {
                            let add_res = self_inner
                                .meta_data
                                .add_snapshot_meta_data(&snap_id_str, &snapshot)
                                .await;
                            debug_assert!(
                            add_res.is_ok(),
                            "snapshot with the same ID={} exists on node ID={}, impossible case",
                            snap_id,
                            node_id,
                        );
                            info!(
                                "create snapshot ID={} and name={} on node ID={}",
                                snap_id,
                                req.get_name(),
                                node_id,
                            );
                            Ok(r)
                        }
                        Err(e) => Err((RpcStatusCode::INTERNAL, e)),
                    }
                }
                Err(e) => Err((RpcStatusCode::INTERNAL, e)),
            }
        };
        util::spawn_grpc_task(sink, task);
    }

    fn worker_delete_snapshot(
        &mut self,
        _ctx: RpcContext,
        req: DeleteSnapshotRequest,
        sink: UnarySink<DeleteSnapshotResponse>,
    ) {
        debug!("worker delete_snapshot request: {:?}", req);
        let self_inner = Arc::<WorkerImplInner>::clone(&self.inner);

        let task = async move {
            let snap_id = req.get_snapshot_id();
            let delete_res = self_inner
                .meta_data
                .delete_snapshot_meta_data(snap_id)
                .await
                .context(format!(
                    "failed to find the snapshot ID={} to delete on node ID={}",
                    snap_id,
                    self_inner.meta_data.get_node_id(),
                ));
            match delete_res {
                Ok(snapshot) => {
                    let del_res = snapshot.delete_file();
                    if let Err(e) = del_res {
                        panic!(
                            "failed to delete snapshot ID={} file on node ID={}, the error is: {}",
                            snap_id,
                            self_inner.meta_data.get_node_id(),
                            util::format_anyhow_error(&e),
                        );
                    }
                    debug!(
                        "successfully delete snapshot ID={} on node ID={}",
                        snap_id,
                        self_inner.meta_data.get_node_id(),
                    );
                    let r = DeleteSnapshotResponse::new();
                    Ok(r)
                }
                Err(e) => Err((RpcStatusCode::NOT_FOUND, e)),
            }
        };
        util::spawn_grpc_task(sink, task);
    }
}
