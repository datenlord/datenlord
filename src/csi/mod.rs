//! K8S CSI `gRPC` service
mod controller;
mod identity;
pub mod meta_data;
mod node;
/// Proto definition
mod proto;
pub mod scheduler_extender;
pub mod util;
mod worker;

use std::net::IpAddr;
use std::sync::Arc;

use controller::ControllerImpl;
use grpcio::{Environment, Server};
use identity::IdentityImpl;
use meta_data::{DatenLordNode, MetaData};
use node::NodeImpl;
use tracing::info;
use worker::WorkerImpl;

use crate::common::error::{Context, DatenLordResult};
use crate::common::etcd_delegate::EtcdDelegate;
use crate::RunAsRole;

/// Build meta data
pub async fn build_meta_data(
    worker_port: u16,
    node_id: String,
    ip_address: IpAddr,
    data_dir: String,
    run_as: RunAsRole,
    etcd_delegate: EtcdDelegate,
) -> DatenLordResult<MetaData> {
    let ephemeral = false; // TODO: read from command line argument
    let node = DatenLordNode::new(
        node_id,
        ip_address,
        worker_port,
        util::MAX_VOLUME_STORAGE_CAPACITY,
        util::MAX_VOLUMES_PER_NODE,
    );
    MetaData::new(data_dir, ephemeral, run_as, etcd_delegate, node).await
}

/// Build worker service
pub fn build_grpc_worker_server(meta_data: Arc<MetaData>) -> DatenLordResult<Server> {
    remove_socket_file(util::LOCAL_WORKER_SOCKET);

    let node = meta_data.get_node();
    let ip_address: &str = &node.ip_address.to_string();

    let (worker_bind_address, worker_bind_port) = if node.worker_port == 0 {
        // In this case, worker server won't be public,
        // bind worker service at a socket file and port as 0
        (util::LOCAL_WORKER_SOCKET, 0) // Non-public worker service
    } else {
        (ip_address, node.worker_port) // Public worker service
    };

    let worker_service = proto::datenlord_worker_grpc::create_worker(WorkerImpl::new(meta_data));
    // TODO: increase concurrent queue size
    let worker_server = grpcio::ServerBuilder::new(Arc::new(Environment::new(1)))
        .register_service(worker_service)
        .bind(worker_bind_address, worker_bind_port)
        // .channel_args(ch_builder.build_args())
        .build()
        .add_context("failed to build DatenLord worker server")?;

    Ok(worker_server)
}

/// Build node service
pub fn build_grpc_node_server(
    end_point: &str,
    driver_name: &str,
    meta_data: Arc<MetaData>,
) -> DatenLordResult<Server> {
    remove_socket_file(end_point);

    let identity_service = proto::csi_grpc::create_identity(IdentityImpl::new(
        driver_name.to_owned(),
        util::CSI_PLUGIN_VERSION.to_owned(),
    ));
    let node_service = proto::csi_grpc::create_node(NodeImpl::new(meta_data));
    // TODO: increase concurrent queue size
    let node_server = grpcio::ServerBuilder::new(Arc::new(Environment::new(1)))
        .register_service(identity_service)
        .register_service(node_service)
        .bind(end_point, 0)
        // .channel_args(ch_builder.build_args())
        .build()
        .add_context("failed to build DatenLord worker server")?;

    Ok(node_server)
}

/// Build controller service
pub fn build_grpc_controller_server(
    end_point: &str,
    driver_name: &str,
    meta_data: Arc<MetaData>,
) -> DatenLordResult<Server> {
    remove_socket_file(end_point);

    let identity_service = proto::csi_grpc::create_identity(IdentityImpl::new(
        driver_name.to_owned(),
        util::CSI_PLUGIN_VERSION.to_owned(),
    ));
    let controller_service = proto::csi_grpc::create_controller(ControllerImpl::new(meta_data));

    // let (mem_size, overflow) = 1024_usize.overflowing_mul(1024);
    // debug_assert!(!overflow, "computing memory size overflowed");
    // let quota =
    // ResourceQuota::new(Some("DatenLordWokerQuota")).resize_memory(mem_size);
    // let ch_builder =
    // ChannelBuilder::new(Arc::<Environment>::clone(&env)).
    // set_resource_quota(quota); TODO: increase concurrent queue size
    let controller_server = grpcio::ServerBuilder::new(Arc::new(Environment::new(1)))
        .register_service(identity_service)
        .register_service(controller_service)
        .bind(end_point, 0) // Port is not need when bind to socket file
        // .channel_args(ch_builder.build_args())
        .build()
        .add_context("failed to build CSI gRPC server")?;

    Ok(controller_server)
}

/// Remove existing socket file before run CSI `gRPC` server
fn remove_socket_file(sock: &str) {
    if std::path::Path::new(sock).exists() {
        if let Err(e) = std::fs::remove_file(sock) {
            panic!("failed to remove existing socket file {sock}, the error is: {e}",);
        }
    }
}

/// Helper function to run server
fn run_single_server_helper(srv: &mut Server) {
    srv.start();
    for (host, port) in srv.bind_addrs() {
        info!("gRPC server listening on {}:{}", host, port);
    }
}

/// Run `gRPC` servers
pub async fn run_grpc_servers(servers: &mut [Server]) {
    /// The future to run `gRPC` servers
    async fn run_servers(servers: &mut [Server]) {
        for server in servers.iter_mut() {
            run_single_server_helper(server);
        }
        let f = futures::future::pending::<()>();
        f.await;
    }
    run_servers(servers).await;
}

#[cfg(test)]
mod test {
    use std::fs::{self, File};
    use std::io::prelude::*;
    use std::net::Ipv4Addr;
    use std::path::{Path, PathBuf};
    use std::sync::Once;

    use clippy_utilities::{Cast, OverflowArithmetic};
    use grpcio::{ChannelBuilder, EnvBuilder};
    use proto::csi::{
        ControllerExpandVolumeRequest, ControllerExpandVolumeResponse, CreateSnapshotRequest,
        CreateSnapshotResponse, CreateVolumeRequest, CreateVolumeResponse, DeleteSnapshotRequest,
        DeleteSnapshotResponse, DeleteVolumeRequest, DeleteVolumeResponse,
        GetPluginCapabilitiesRequest, GetPluginInfoRequest, ListSnapshotsRequest,
        ListSnapshotsResponse, ListVolumesRequest, ListVolumesResponse, NodeExpandVolumeRequest,
        NodeGetCapabilitiesRequest, NodeGetInfoRequest, NodePublishVolumeRequest,
        NodeServiceCapability_RPC_Type, NodeUnpublishVolumeRequest, PluginCapability_Service_Type,
        ProbeRequest, VolumeCapability, VolumeCapability_AccessMode_Mode,
        VolumeCapability_MountVolume,
    };
    use proto::csi_grpc::{ControllerClient, IdentityClient, NodeClient};
    // use mock_etcd::MockEtcdServer;
    use protobuf::RepeatedField;
    use tracing::debug;

    use super::{util, *};
    use crate::common::error::Context;
    use crate::common::logger::{init_logger, NodeType};

    const NODE_PUBLISH_VOLUME_TARGET_PATH: &str = "/tmp/target_volume_path";
    const NODE_PUBLISH_VOLUME_TARGET_PATH_1: &str = "/tmp/target_volume_path_1";
    const NODE_PUBLISH_VOLUME_TARGET_PATH_2: &str = "/tmp/target_volume_path_2";
    const NODE_PUBLISH_VOLUME_ID: &str = "46ebd0ee-0e6d-43c9-b90d-ccc35a913f3e";
    const DEFAULT_NODE_NAME: &str = "localhost";
    const DEFAULT_NODE_IP: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
    const DEFAULT_ETCD_ENDPOINT_FOR_TEST: &str = "127.0.0.1:2379";
    const ETCD_ENV_VAR_KEY: &str = "ETCD_END_POINT";
    /// The csi server socket file to communicate with K8S CSI sidecars
    const CONTROLLER_END_POINT: &str = "unix:///tmp/csi.sock";
    /// The node server socket file to communicate with K8S CSI sidecars
    const NODE_END_POINT: &str = "unix:///tmp/node.sock";
    // const WORKER_PORT_ENV_VAR_KEY: &str = "WORKER_PORT";
    /// Directory where data for volumes and snapshots are persisted.
    /// This can be ephemeral within the container or persisted if
    /// backed by a Pod volume.
    const DATA_DIR: &str = "/tmp/csi-data-dir";
    static GRPC_SERVER: Once = Once::new();

    #[ignore = "maybe conflict with other tests"]
    #[allow(clippy::let_underscore_must_use)]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_all() -> DatenLordResult<()> {
        init_logger(NodeType::Node);
        // TODO: run test case in parallel
        // Because they all depend on etcd, so cannot run in parallel now
        // let mut etcd_server = MockEtcdServer::new();
        // etcd_server.start();
        info!("test meta data");
        test_meta_data()
            .await
            .add_context("test meta data failed")?;
        info!("test identity server");
        test_identity_server().add_context("test identity server failed")?;
        info!("test controller server");
        test_controller_server().add_context("test controller server failed")?;
        info!("test node server");
        test_node_server().add_context("test node server failed")?;
        Ok(())
    }

    fn get_etcd_address_vec() -> Vec<String> {
        match std::env::var(ETCD_ENV_VAR_KEY) {
            Ok(val) => {
                debug!("{}={}", ETCD_ENV_VAR_KEY, val);
                vec![val]
            }
            Err(_) => vec![DEFAULT_ETCD_ENDPOINT_FOR_TEST.to_owned()],
        }
    }

    async fn clear_test_data(etcd_delegate: &EtcdDelegate) -> DatenLordResult<()> {
        let dir_path = Path::new(DATA_DIR);
        if dir_path.exists() {
            fs::remove_dir_all(dir_path)?;
        }
        let node_volume_publish_path = Path::new(NODE_PUBLISH_VOLUME_TARGET_PATH);
        if node_volume_publish_path.exists() {
            let umount_res = util::umount_volume_bind_path(NODE_PUBLISH_VOLUME_TARGET_PATH);
            debug!(
                "un-mount {} result: {:?}",
                NODE_PUBLISH_VOLUME_TARGET_PATH, umount_res
            );
            fs::remove_dir_all(NODE_PUBLISH_VOLUME_TARGET_PATH)?;
        }

        etcd_delegate.delete_all().await?;
        Ok(())
    }

    async fn build_test_meta_data() -> DatenLordResult<MetaData> {
        let etcd_address_vec = get_etcd_address_vec();
        let etcd_delegate = EtcdDelegate::new(etcd_address_vec).await?;
        clear_test_data(&etcd_delegate).await?;

        let worker_port = 50051;
        let node_id = DEFAULT_NODE_NAME;
        let ip_address = DEFAULT_NODE_IP;
        let data_dir = DATA_DIR;
        let ephemeral = false;
        let node = DatenLordNode::new(
            node_id.to_owned(),
            ip_address,
            worker_port,
            util::MAX_VOLUME_STORAGE_CAPACITY,
            util::MAX_VOLUMES_PER_NODE,
        );
        // Register Node metadata to etcd, but return Controller metadata for test
        let _metadata = MetaData::new(
            data_dir.to_owned(),
            ephemeral,
            RunAsRole::Node,
            etcd_delegate.clone(),
            node.clone(),
        )
        .await?;
        MetaData::new(
            data_dir.to_owned(),
            ephemeral,
            RunAsRole::Controller,
            etcd_delegate,
            node,
        )
        .await
    }

    async fn test_meta_data() -> DatenLordResult<()> {
        let meta_data = build_test_meta_data().await?;
        let vol_id = "the-fake-ephemeral-volume-id-for-meta-data-test";
        let mut volume = meta_data::DatenLordVolume::build_ephemeral_volume(
            vol_id,
            "ephemeral-volume", // vol_name
            DEFAULT_NODE_NAME,
            meta_data.get_volume_path(NODE_PUBLISH_VOLUME_ID).as_path(), // vol_path
        )?;
        let add_vol_res = meta_data.add_volume_meta_data(vol_id, &volume).await;
        assert!(
            add_vol_res.is_ok(),
            "failed to add new volume meta data to etcd"
        );
        let get_vol_res = meta_data
            .get_volume_by_name(&volume.vol_name)
            .await
            .with_context(|| format!("failed to find volume by name={}", volume.vol_name,))?;
        assert_eq!(
            get_vol_res.vol_name, volume.vol_name,
            "volume name not match"
        );

        let new_size_bytes = 2.overflow_mul(util::EPHEMERAL_VOLUME_STORAGE_CAPACITY);
        let exp_vol_res = meta_data.expand(&mut volume, new_size_bytes).await?;
        assert_eq!(
            exp_vol_res,
            util::EPHEMERAL_VOLUME_STORAGE_CAPACITY,
            "the old size before expand not match"
        );

        let expanded_vol = meta_data
            .get_volume_by_id(vol_id)
            .await
            .with_context(|| format!("failed to find volume ID={vol_id}"))?;
        assert_eq!(
            expanded_vol.size_bytes, new_size_bytes,
            "the expanded volume size not match"
        );

        let request = CreateVolumeRequest::new();
        let selected_node = meta_data.select_node(&request).await?;
        assert_eq!(
            selected_node.node_id, DEFAULT_NODE_NAME,
            "selected node ID not match"
        );

        let snap_id = "the-fake-snapshot-id-for-meta-data-test";
        let snapshot = meta_data::DatenLordSnapshot::new(
            "test-snapshot-name".to_owned(), // snap_name,
            snap_id.to_owned(),              // snap_id,
            vol_id.to_owned(),
            meta_data.get_node_id().to_owned(),
            meta_data.get_snapshot_path(snap_id),
            std::time::SystemTime::now(),
            0, // size_bytes,
        );
        let add_snap_res = meta_data.add_snapshot_meta_data(snap_id, &snapshot).await;
        assert!(
            add_snap_res.is_ok(),
            "failed to add new snapshot meta data to etcd"
        );
        let get_snap_by_name_res = meta_data
            .get_snapshot_by_name(&snapshot.snap_name)
            .await
            .with_context(|| format!("failed to find snapshot by name={}", snapshot.snap_name,))?;
        assert_eq!(
            get_snap_by_name_res.snap_name, snapshot.snap_name,
            "snapshot name not match"
        );

        let get_snap_by_src_vol_id_res = meta_data
            .get_snapshot_by_src_volume_id(&snapshot.vol_id)
            .await
            .with_context(|| {
                format!(
                    "failed to find snapshot by source volume ID={}",
                    snapshot.vol_id,
                )
            })?;
        assert_eq!(
            get_snap_by_src_vol_id_res.vol_id, snapshot.vol_id,
            "snapshot source volume ID not match"
        );

        let del_snap_res = meta_data.delete_snapshot_meta_data(snap_id).await?;
        assert_eq!(
            del_snap_res
                .unwrap_or_else(|| panic!("Cannot get snap id {snap_id} from etcd"))
                .snap_id,
            snap_id,
            "deleted snapshot ID not match"
        );
        let del_vol_res = meta_data
            .delete_volume_meta_data(vol_id, meta_data.get_node_id())
            .await?;
        assert_eq!(del_vol_res.vol_id, vol_id, "deleted volume ID not match");
        Ok(())
    }

    fn get_volume_path(vol_id: &str) -> PathBuf {
        Path::new(DATA_DIR).join(vol_id)
    }

    #[allow(clippy::unnecessary_wraps)]
    #[allow(clippy::unwrap_used)]
    fn run_test_server() -> DatenLordResult<()> {
        let controller_end_point = CONTROLLER_END_POINT.to_owned();
        let node_end_point = NODE_END_POINT.to_owned();
        let worker_port = 0;
        let node_id = DEFAULT_NODE_NAME.to_owned();
        let ip_address = DEFAULT_NODE_IP;
        let driver_name = util::CSI_PLUGIN_NAME.to_owned();
        let data_dir = DATA_DIR.to_owned();
        let etcd_address_vec = get_etcd_address_vec();

        GRPC_SERVER.call_once(move || {
            futures::executor::block_on(async {
                let etcd_delegate = EtcdDelegate::new(etcd_address_vec)
                    .await
                    .unwrap_or_else(|e| {
                        panic!("failed to create new EtcdDelegate for error {e}");
                    });
                let clear_res = clear_test_data(&etcd_delegate).await;
                assert!(
                    clear_res.is_ok(),
                    "failed to clear test data, the error is: {}",
                    clear_res.unwrap_err(),
                );
                let controller_meta_data = match build_meta_data(
                    worker_port,
                    node_id.clone(),
                    ip_address,
                    data_dir.clone(),
                    RunAsRole::Controller,
                    etcd_delegate.clone(),
                )
                .await
                {
                    Ok(md) => md,
                    Err(e) => panic!("failed to build meta data, the error is : {e}",),
                };
                let controller_md = Arc::new(controller_meta_data);
                let controller_server = match build_grpc_controller_server(
                    &controller_end_point,
                    &driver_name,
                    Arc::<MetaData>::clone(&controller_md),
                ) {
                    Ok(server) => server,
                    Err(e) => panic!("failed to build CSI server, the error is : {e}",),
                };
                let node_meta_data = match build_meta_data(
                    worker_port,
                    node_id.clone(),
                    ip_address,
                    data_dir.clone(),
                    RunAsRole::Node,
                    etcd_delegate.clone(),
                )
                .await
                {
                    Ok(md) => md,
                    Err(e) => panic!("failed to build meta data, the error is : {e}",),
                };
                let node_worker_md = Arc::new(node_meta_data);
                let node_server = match build_grpc_node_server(
                    &node_end_point,
                    &driver_name,
                    Arc::<MetaData>::clone(&node_worker_md),
                ) {
                    Ok(server) => server,
                    Err(e) => panic!("failed to build Node server, the error is : {e}",),
                };
                let worker_server = match build_grpc_worker_server(node_worker_md) {
                    Ok(server) => server,
                    Err(e) => panic!("failed to build Worker server, the error is : {e}",),
                };

                // Keep running the task in the background
                let _controller_thread = tokio::spawn(async move {
                    run_grpc_servers(&mut [controller_server]).await;
                });

                // Keep running the task in the background
                let _node_thread = tokio::spawn(async move {
                    run_grpc_servers(&mut [node_server, worker_server]).await;
                });
            });
        });
        Ok(())
    }

    fn build_identity_client() -> DatenLordResult<IdentityClient> {
        run_test_server()?;
        let env = Arc::new(EnvBuilder::new().build());
        let ch = ChannelBuilder::new(env).connect(CONTROLLER_END_POINT);
        Ok(IdentityClient::new(ch))
    }

    fn test_identity_server() -> DatenLordResult<()> {
        let client = build_identity_client()?;

        // Test get info
        let info_resp = client
            .get_plugin_info(&GetPluginInfoRequest::new())
            .add_context("failed to get GetPluginInfoResponse")?;
        assert_eq!(
            info_resp.name,
            util::CSI_PLUGIN_NAME,
            "GetPluginInfoResponse has incorrect name",
        );
        assert_eq!(
            info_resp.vendor_version,
            util::CSI_PLUGIN_VERSION,
            "GetPluginInfoResponse has incorrect version",
        );

        // Test get capabilities
        let cap_resp = client
            .get_plugin_capabilities(&GetPluginCapabilitiesRequest::new())
            .add_context("failed to get GetPluginCapabilitiesResponse")?;
        let caps = cap_resp.get_capabilities();
        let cap_vec = caps
            .iter()
            .map(|cap| cap.get_service().get_field_type())
            .collect::<Vec<_>>();
        assert_eq!(
            cap_vec,
            vec![
                PluginCapability_Service_Type::CONTROLLER_SERVICE,
                PluginCapability_Service_Type::VOLUME_ACCESSIBILITY_CONSTRAINTS,
            ],
            "get_plugin_capabilities returned capabilities not as expected"
        );

        // Test probe
        let prob_resp = client
            .probe(&ProbeRequest::new())
            .add_context("failed to get ProbeResponse")?;
        debug_assert!(
            prob_resp.get_ready().value,
            "ProbeResponse showed server not ready",
        );

        Ok(())
    }

    fn build_controller_client() -> DatenLordResult<ControllerClient> {
        run_test_server()?;
        let env = Arc::new(EnvBuilder::new().build());
        let ch = ChannelBuilder::new(env).connect(CONTROLLER_END_POINT);
        Ok(ControllerClient::new(ch))
    }

    fn create_volume(
        client: &ControllerClient,
        req: &CreateVolumeRequest,
    ) -> DatenLordResult<CreateVolumeResponse> {
        let resp = client.create_volume(req)?;
        Ok(resp)
    }

    fn delete_volume(
        client: &ControllerClient,
        req: &DeleteVolumeRequest,
    ) -> DatenLordResult<DeleteVolumeResponse> {
        let resp = client.delete_volume(req)?;
        Ok(resp)
    }

    fn create_snapshot(
        client: &ControllerClient,
        req: &CreateSnapshotRequest,
    ) -> DatenLordResult<CreateSnapshotResponse> {
        let resp = client.create_snapshot(req)?;
        Ok(resp)
    }

    fn list_volumes(
        client: &ControllerClient,
        req: &ListVolumesRequest,
    ) -> DatenLordResult<ListVolumesResponse> {
        let resp = client.list_volumes(req)?;
        Ok(resp)
    }

    fn delete_snapshot(
        client: &ControllerClient,
        req: &DeleteSnapshotRequest,
    ) -> DatenLordResult<DeleteSnapshotResponse> {
        let resp = client.delete_snapshot(req)?;
        Ok(resp)
    }

    fn list_snapshots(
        client: &ControllerClient,
        req: &ListSnapshotsRequest,
    ) -> DatenLordResult<ListSnapshotsResponse> {
        let resp = client.list_snapshots(req)?;
        Ok(resp)
    }

    fn controller_expand_volume(
        client: &ControllerClient,
        req: &ControllerExpandVolumeRequest,
    ) -> DatenLordResult<ControllerExpandVolumeResponse> {
        let resp = client.controller_expand_volume(req)?;
        Ok(resp)
    }

    fn write_file_in_volume(
        vol_id: &str,
        vol_file_name: &str,
        vol_file_content: &str,
    ) -> DatenLordResult<()> {
        // Write some date to volume directory
        let vol_path = get_volume_path(vol_id);
        let mut vol_data_file = File::create(vol_path.join(vol_file_name))?;
        vol_data_file.write_all(vol_file_content.as_bytes())?;
        Ok(())
    }

    fn verify_volume_file_content(
        vol_id: &str,
        vol_file_name: &str,
        expected_content: &str,
    ) -> DatenLordResult<()> {
        let vol_file_path = get_volume_path(vol_id).join(vol_file_name);
        let buffer = fs::read_to_string(&vol_file_path).with_context(|| {
            format!("failed to read the file name={vol_file_path:?} of volume ID={vol_id}")
        })?;
        assert!(
            !buffer.is_empty(),
            "failed to read content from duplicated volume file"
        );
        assert_eq!(
            buffer, expected_content,
            "verify volume content failed, read content: {buffer}, expected content: {expected_content}",
        );
        Ok(())
    }

    fn test_controller_server() -> DatenLordResult<()> {
        let controller_client = build_controller_client()?;

        test_controller_create_volume_from_volume(&controller_client)?;
        test_controller_create_volume_from_snapshot(&controller_client)?;
        test_controller_create_delete_idempotency(&controller_client)?;
        test_controller_expand_volume(&controller_client)?;
        test_controller_create_and_list_snapshot(&controller_client)?;
        test_list_volumes(&controller_client)?;
        Ok(())
    }

    fn test_list_volumes(client: &ControllerClient) -> DatenLordResult<()> {
        let vol_names = (1_i32..5_i32)
            .map(|idx| format!("tmp_volume_name_{idx}"))
            .collect::<Vec<_>>();

        // Test create new volume
        let mut vc = VolumeCapability::new();
        vc.set_mount(VolumeCapability_MountVolume::new());
        vc.mut_access_mode()
            .set_mode(VolumeCapability_AccessMode_Mode::SINGLE_NODE_WRITER);

        let mut creat_vol_req = CreateVolumeRequest::new();
        creat_vol_req.set_volume_capabilities(RepeatedField::from_vec(vec![vc]));

        let mut volumes = Vec::new();
        for vol_name in &vol_names {
            creat_vol_req.set_name((*vol_name).clone());
            let creat_resp = create_volume(client, &creat_vol_req)
                .add_context("failed to get CreateVolumeResponse")?;
            let vol = creat_resp.get_volume();
            volumes.push(vol.get_volume_id().to_owned());
        }

        // List all volumes
        let mut list_vol_req = ListVolumesRequest::new();
        let list_vol_resp1 = list_volumes(client, &list_vol_req)
            .add_context("failed to get ListVolumesResponse of all volumes")?;

        let list_vols1 = list_vol_resp1.get_entries();
        let all_vol_vec = list_vols1
            .iter()
            .map(|vol| vol.get_volume().get_volume_id())
            .collect::<Vec<_>>();
        let mut vol_vec1 = all_vol_vec.clone();
        vol_vec1.sort_unstable();
        let mut expect_vol_vec1 = volumes.clone();
        expect_vol_vec1.sort();
        assert_eq!(vol_vec1, expect_vol_vec1, "list volume result not match");
        assert_eq!(
            list_vol_resp1.get_next_token(),
            expect_vol_vec1.len().to_string(),
            "next volume index not match",
        );

        // List volume from starting position as 1 and max entries as 1
        let starting_pos: usize = 1;
        let max_entries = 2_i32;
        list_vol_req.set_starting_token(starting_pos.to_string());
        list_vol_req.set_max_entries(max_entries);
        let list_vol_resp2 = list_volumes(client, &list_vol_req)
            .add_context("failed to get ListVolumesResponse of two volumes")?;

        let list_vols2 = list_vol_resp2.get_entries();
        let mut vol_vec2 = list_vols2
            .iter()
            .map(|vol| vol.get_volume().get_volume_id())
            .collect::<Vec<_>>();
        vol_vec2.sort_unstable();
        let end_pos = starting_pos.overflow_add(max_entries.cast::<usize>());
        let mut expect_vol_vec2 = all_vol_vec
            .get(starting_pos..end_pos)
            .map_or(Vec::new(), std::borrow::ToOwned::to_owned);
        expect_vol_vec2.sort_unstable();
        assert_eq!(vol_vec2, expect_vol_vec2, "list volume result not match");
        let next_starting_pos = starting_pos.overflow_add(max_entries.cast::<usize>());
        assert_eq!(
            list_vol_resp2.get_next_token(),
            next_starting_pos.to_string(),
            "next volume index not match",
        );

        // Delete remaining volumes
        let mut del_vol_req = DeleteVolumeRequest::new();
        for vol_id in volumes {
            del_vol_req.set_volume_id(vol_id.clone());
            let _del_vol_resp3 = delete_volume(client, &del_vol_req).with_context(|| {
                format!("failed to get DeleteVolumeResponse when delete volume ID={vol_id}",)
            })?;
        }
        Ok(())
    }

    fn test_controller_create_volume_from_volume(client: &ControllerClient) -> DatenLordResult<()> {
        let vol_name = "tmp_volume";
        let dup_vol_name = "dup_volume";
        let vol_file_name = "volume.dat";
        let vol_file_content = "TMP_VOLUME_DATA";

        // Test create new volume
        let mut vc = VolumeCapability::new();
        vc.set_mount(VolumeCapability_MountVolume::new());
        vc.mut_access_mode()
            .set_mode(VolumeCapability_AccessMode_Mode::SINGLE_NODE_WRITER);

        let mut creat_vol_req = CreateVolumeRequest::new();
        creat_vol_req.set_name(vol_name.to_owned());
        creat_vol_req.set_volume_capabilities(RepeatedField::from_vec(vec![vc]));

        let creat_resp1 = create_volume(client, &creat_vol_req)
            .add_context("failed to get CreateVolumeResponse when create first volume")?;
        let volume1 = creat_resp1.get_volume();

        // Write some date to volume directory
        write_file_in_volume(volume1.get_volume_id(), vol_file_name, vol_file_content)?;

        // Test create volume from existing volume
        creat_vol_req
            .mut_volume_content_source()
            .mut_volume()
            .set_volume_id(volume1.get_volume_id().to_owned());
        creat_vol_req.set_name(dup_vol_name.to_owned());

        let creat_resp2 = create_volume(client, &creat_vol_req)
            .add_context("failed to get CreateVolumeResponse when create second volume")?;
        let volume2 = creat_resp2.get_volume();

        assert!(
            volume2.has_content_source(),
            "duplicated volume should have content source",
        );
        assert_eq!(
            volume2.get_content_source().get_volume().get_volume_id(),
            volume1.get_volume_id(),
            "the parent volume ID of duplicated volume not match the source volume ID",
        );

        // Verify volume data of duplicated volume
        verify_volume_file_content(volume2.get_volume_id(), vol_file_name, vol_file_content)
            .with_context(|| {
                format!(
                    "failed to verify the content of file name={} of volume ID={}",
                    vol_file_name,
                    volume2.get_volume_id(),
                )
            })?;

        // List to verify two volumes
        let list_vol_req = ListVolumesRequest::new();
        let list_vol_resp1 = list_volumes(client, &list_vol_req)
            .add_context("failed to get ListVolumesResponse of two volumes")?;

        let vols1 = list_vol_resp1.get_entries();
        let mut vol_vec1 = vols1
            .iter()
            .map(|vol| vol.get_volume().get_volume_id())
            .collect::<Vec<_>>();
        vol_vec1.sort_unstable();
        let mut expect_vol_vec1 = vec![volume1.get_volume_id(), volume2.get_volume_id()];
        expect_vol_vec1.sort_unstable();
        assert_eq!(vol_vec1, expect_vol_vec1, "list volume result not match",);
        assert_eq!(
            list_vol_resp1.get_next_token(),
            "2",
            "next volume index not match",
        );

        // Delete second volume
        let mut del_vol_req = DeleteVolumeRequest::new();
        del_vol_req.set_volume_id(volume2.get_volume_id().to_owned());
        let _del_resp1 = delete_volume(client, &del_vol_req)
            .add_context("failed to get DeleteVolumeResponse when delete second volume")?;

        // List the first volume only
        let list_vol_req = ListVolumesRequest::new();
        let list_vol_resp1 = list_volumes(client, &list_vol_req)
            .add_context("failed to get ListVolumesResponse of first volume")?;

        let vols2 = list_vol_resp1.get_entries();
        let vol_vec2 = vols2
            .iter()
            .map(|vol| vol.get_volume().get_volume_id())
            .collect::<Vec<_>>();
        let expect_vol_vec2 = vec![volume1.get_volume_id()];
        assert_eq!(vol_vec2, expect_vol_vec2, "list volume result not match",);
        assert_eq!(
            list_vol_resp1.get_next_token(),
            "1",
            "next volume index not match",
        );

        // Delete first volume
        del_vol_req.set_volume_id(volume1.get_volume_id().to_owned());
        let _del_vol_resp3 = delete_volume(client, &del_vol_req)
            .add_context("failed to get DeleteVolumeResponse when delete first volume")?;

        Ok(())
    }

    fn test_controller_create_volume_from_snapshot(
        client: &ControllerClient,
    ) -> DatenLordResult<()> {
        let vol_name = "tmp_volume";
        let dup_vol_name = "dup_volume";
        let vol_file_name = "volume.dat";
        let vol_file_content = "TMP_VOLUME_DATA";
        let snap_name = "tmp_snapshot";

        // Test create new volume
        let mut vc = VolumeCapability::new();
        vc.set_mount(VolumeCapability_MountVolume::new());
        vc.mut_access_mode()
            .set_mode(VolumeCapability_AccessMode_Mode::SINGLE_NODE_WRITER);

        let mut creat_vol_req = CreateVolumeRequest::new();
        creat_vol_req.set_name(vol_name.to_owned());
        creat_vol_req.set_volume_capabilities(RepeatedField::from_vec(vec![vc]));

        let creat_resp1 = create_volume(client, &creat_vol_req)
            .add_context("failed to get CreateVolumeResponse when create source volume")?;
        let volume1 = creat_resp1.get_volume();

        // Write some date to volume directory
        write_file_in_volume(volume1.get_volume_id(), vol_file_name, vol_file_content)?;

        // Test create snapshot
        let mut creat_snap_req = CreateSnapshotRequest::new();
        creat_snap_req.set_source_volume_id(volume1.get_volume_id().to_owned());
        creat_snap_req.set_name(snap_name.to_owned());

        let creat_snap_resp = create_snapshot(client, &creat_snap_req)
            .add_context("failed to get CreateSnapshotResponse")?;

        let snapshot = creat_snap_resp.get_snapshot();
        assert!(
            snapshot.get_ready_to_use(),
            "snapshot should be ready to use",
        );
        assert_eq!(
            snapshot.get_source_volume_id(),
            volume1.get_volume_id(),
            "snapshot source volume ID not match",
        );

        // Test create volume from snapshot
        creat_vol_req
            .mut_volume_content_source()
            .mut_snapshot()
            .set_snapshot_id(snapshot.get_snapshot_id().to_owned());
        creat_vol_req.set_name(dup_vol_name.to_owned());

        let creat_resp3 = create_volume(client, &creat_vol_req)
            .add_context("failed to get CreateVolumeResponse when create volume from snapshot")?;
        let volume3 = creat_resp3.get_volume();

        assert!(
            volume3.has_content_source(),
            "duplicated volume should have content source",
        );
        assert_eq!(
            volume3
                .get_content_source()
                .get_snapshot()
                .get_snapshot_id(),
            snapshot.get_snapshot_id(),
            "the parent volume ID of duplicated volume not match the source snapshot ID",
        );

        // Verify volume data of duplicated volume
        verify_volume_file_content(volume3.get_volume_id(), vol_file_name, vol_file_content)
            .with_context(|| {
                format!(
                    "failed to verify the content of file name={} of volume ID={}",
                    vol_file_name,
                    volume3.get_volume_id(),
                )
            })?;

        // Delete snapshot
        let mut del_snap_req = DeleteSnapshotRequest::new();
        del_snap_req.set_snapshot_id(snapshot.get_snapshot_id().to_owned());

        let _del_snap_resp1 = delete_snapshot(client, &del_snap_req)
            .add_context("failed to get DeleteSnapshotResponse")?;

        // Delete duplicated volume
        let mut del_vol_req = DeleteVolumeRequest::new();
        del_vol_req.set_volume_id(volume3.get_volume_id().to_owned());
        let _del_vol_resp2 = delete_volume(client, &del_vol_req)
            .add_context("failed to get DeleteVolumeResponse when delete twice")?;

        // Delete source volume
        del_vol_req.set_volume_id(volume1.get_volume_id().to_owned());
        let _del_vol_resp3 = delete_volume(client, &del_vol_req)
            .add_context("failed to get DeleteVolumeResponse")?;

        Ok(())
    }

    fn test_controller_create_delete_idempotency(client: &ControllerClient) -> DatenLordResult<()> {
        let vol_name = "test_volume";
        let snap_name = "test_snapshot";

        // Test create new volume
        let mut vc = VolumeCapability::new();
        vc.set_mount(VolumeCapability_MountVolume::new());
        vc.mut_access_mode()
            .set_mode(VolumeCapability_AccessMode_Mode::SINGLE_NODE_WRITER);
        let mut creat_vol_req = CreateVolumeRequest::new();
        creat_vol_req.set_name(vol_name.to_owned());
        creat_vol_req.set_volume_capabilities(RepeatedField::from_vec(vec![vc]));

        let creat_resp1 = create_volume(client, &creat_vol_req)
            .add_context("failed to get CreateVolumeResponse")?;
        let volume = creat_resp1.get_volume();

        // Idempotency test for create volume
        let creat_resp2 = create_volume(client, &creat_vol_req)
            .add_context("failed to get CreateVolumeResponse when create twice")?;
        let same_volume = creat_resp2.get_volume();
        assert_eq!(
            volume.get_volume_id(),
            same_volume.get_volume_id(),
            "volume ID should match",
        );

        // Test create snapshot
        let mut creat_snap_req = CreateSnapshotRequest::new();
        creat_snap_req.set_source_volume_id(volume.get_volume_id().to_owned());
        creat_snap_req.set_name(snap_name.to_owned());

        let creat_snap_resp1 = create_snapshot(client, &creat_snap_req)
            .add_context("failed to get CreateSnapshotResponse")?;

        let snapshot1 = creat_snap_resp1.get_snapshot();
        assert!(
            snapshot1.get_ready_to_use(),
            "snapshot should be ready to use",
        );
        assert_eq!(
            snapshot1.get_source_volume_id(),
            volume.get_volume_id(),
            "snapshot source volume ID not match",
        );

        // Idempotency test for create snapshot
        let creat_snap_resp2 = create_snapshot(client, &creat_snap_req)
            .add_context("failed to get CreateSnapshotResponse when create twice")?;

        let snapshot2 = creat_snap_resp2.get_snapshot();
        assert!(
            snapshot2.get_ready_to_use(),
            "snapshot should be ready to use",
        );
        assert_eq!(
            snapshot2.get_source_volume_id(),
            volume.get_volume_id(),
            "snapshot source volume ID not match",
        );
        assert_eq!(
            snapshot1.get_snapshot_id(),
            snapshot2.get_snapshot_id(),
            "snapshot ID not match",
        );

        // Test delete snapshot
        let mut del_snap_req = DeleteSnapshotRequest::new();
        del_snap_req.set_snapshot_id(snapshot1.get_snapshot_id().to_owned());

        let _del_snap_resp1 = delete_snapshot(client, &del_snap_req)
            .add_context("failed to get DeleteSnapshotResponse")?;

        // Idempotency test for delete snapshot
        let _del_snap_resp2 = delete_snapshot(client, &del_snap_req)
            .add_context("failed to get DeleteSnapshotResponse when delete twice")?;

        // Test delete volume
        let mut del_vol_req = DeleteVolumeRequest::new();
        del_vol_req.set_volume_id(volume.get_volume_id().to_owned());
        let _del_resp1 = delete_volume(client, &del_vol_req)
            .add_context("failed to get DeleteVolumeResponse")?;

        // Idempotency test for delete volume
        let _del_resp2 = delete_volume(client, &del_vol_req)
            .add_context("failed to get DeleteVolumeResponse when delete twice")?;

        Ok(())
    }

    fn test_controller_expand_volume(client: &ControllerClient) -> DatenLordResult<()> {
        let vol_name = "test_volume";

        // Test create new volume
        let mut vc = VolumeCapability::new();
        vc.set_mount(VolumeCapability_MountVolume::new());
        vc.mut_access_mode()
            .set_mode(VolumeCapability_AccessMode_Mode::SINGLE_NODE_WRITER);
        let mut creat_vol_req = CreateVolumeRequest::new();
        creat_vol_req.set_name(vol_name.to_owned());
        creat_vol_req.set_volume_capabilities(RepeatedField::from_vec(vec![vc]));

        let creat_resp1 = create_volume(client, &creat_vol_req)
            .add_context("failed to get CreateVolumeResponse")?;
        let volume = creat_resp1.get_volume();

        // Test expand volume
        let mut exp_req = ControllerExpandVolumeRequest::new();
        exp_req.set_volume_id(volume.get_volume_id().to_owned());
        exp_req
            .mut_capacity_range()
            .set_required_bytes(util::MAX_VOLUME_STORAGE_CAPACITY);
        let exp_resp = controller_expand_volume(client, &exp_req)
            .add_context("failed to get ControllerExpandVolumeResponse")?;
        assert_eq!(
            exp_resp.get_capacity_bytes(),
            util::MAX_VOLUME_STORAGE_CAPACITY,
            "volume capacity not match after expend",
        );
        assert!(
            exp_resp.get_node_expansion_required(),
            "CO should call node expand volume after controller expand volume",
        );

        // Test delete volume
        let mut del_vol_req = DeleteVolumeRequest::new();
        del_vol_req.set_volume_id(volume.get_volume_id().to_owned());
        let _del_resp1 = delete_volume(client, &del_vol_req)
            .add_context("failed to get DeleteVolumeResponse")?;

        Ok(())
    }

    fn test_controller_create_and_list_snapshot(client: &ControllerClient) -> DatenLordResult<()> {
        let vol_name = "test_volume";
        let snap_name = "test_snapshot";

        // Test create new volume
        let mut vc = VolumeCapability::new();
        vc.set_mount(VolumeCapability_MountVolume::new());
        vc.mut_access_mode()
            .set_mode(VolumeCapability_AccessMode_Mode::SINGLE_NODE_WRITER);
        let mut creat_vol_req = CreateVolumeRequest::new();
        creat_vol_req.set_name(vol_name.to_owned());
        creat_vol_req.set_volume_capabilities(RepeatedField::from_vec(vec![vc]));

        let creat_resp1 = create_volume(client, &creat_vol_req)
            .add_context("failed to get CreateVolumeResponse")?;
        let volume = creat_resp1.get_volume();

        // Test create snapshot
        let mut creat_snap_req = CreateSnapshotRequest::new();
        creat_snap_req.set_source_volume_id(volume.get_volume_id().to_owned());
        creat_snap_req.set_name(snap_name.to_owned());

        let creat_snap_resp1 = create_snapshot(client, &creat_snap_req)
            .add_context("failed to get CreateSnapshotResponse")?;

        let snapshot1 = creat_snap_resp1.get_snapshot();
        assert!(
            snapshot1.get_ready_to_use(),
            "snapshot should be ready to use",
        );
        assert_eq!(
            snapshot1.get_source_volume_id(),
            volume.get_volume_id(),
            "snapshot source volume ID not match",
        );

        // Test for create snapshot failure with name match but src volume ID not match
        creat_snap_req.set_source_volume_id("some_illegle_volume_id".to_owned());
        let creat_snap_resp3 = create_snapshot(client, &creat_snap_req);
        assert!(
            creat_snap_resp3.is_err(),
            "create snapshot should fail when name match but src volume ID not match",
        );

        // Test list snapshot
        let mut list_snap_req = ListSnapshotsRequest::new();
        let list_snap_resp1 = list_snapshots(client, &list_snap_req)
            .add_context("failed to get ListSnapshotsResponse")?;

        let snaps1 = list_snap_resp1.get_entries();
        let snap_vec1 = snaps1
            .iter()
            .map(|snap| snap.get_snapshot().get_snapshot_id())
            .collect::<Vec<_>>();
        assert_eq!(
            snap_vec1,
            vec![snapshot1.get_snapshot_id()],
            "list snapshot result not match",
        );
        assert_eq!(
            list_snap_resp1.get_next_token(),
            "1",
            "next snapshot index not match",
        );

        // Test list snapshot by src volume ID
        list_snap_req.set_source_volume_id(volume.get_volume_id().to_owned());
        let list_snap_resp2 = list_snapshots(client, &list_snap_req)
            .add_context("failed to get ListSnapshotsResponse")?;

        let snaps2 = list_snap_resp2.get_entries();
        let snap_vec2 = snaps2
            .iter()
            .map(|snap| snap.get_snapshot().get_snapshot_id())
            .collect::<Vec<_>>();
        assert_eq!(
            snap_vec2,
            vec![snapshot1.get_snapshot_id()],
            "list snapshot result not match",
        );

        // Test list snapshot by snapshot ID
        list_snap_req.clear_source_volume_id();
        list_snap_req.set_snapshot_id(snapshot1.get_snapshot_id().to_owned());
        let list_snap_resp3 = list_snapshots(client, &list_snap_req)
            .add_context("failed to get ListSnapshotsResponse")?;

        let snaps3 = list_snap_resp3.get_entries();
        let snap_vec3 = snaps3
            .iter()
            .map(|snap| snap.get_snapshot().get_snapshot_id())
            .collect::<Vec<_>>();
        assert_eq!(
            snap_vec3,
            vec![snapshot1.get_snapshot_id()],
            "list snapshot result not match",
        );

        // Test delete snapshot
        let mut del_snap_req = DeleteSnapshotRequest::new();
        del_snap_req.set_snapshot_id(snapshot1.get_snapshot_id().to_owned());

        let _del_snap_resp1 = delete_snapshot(client, &del_snap_req)
            .add_context("failed to get DeleteSnapshotResponse")?;

        // Test delete volume
        let mut del_vol_req = DeleteVolumeRequest::new();
        del_vol_req.set_volume_id(volume.get_volume_id().to_owned());
        let _del_resp1 = delete_volume(client, &del_vol_req)
            .add_context("failed to get DeleteVolumeResponse")?;

        Ok(())
    }

    fn build_node_client() -> DatenLordResult<NodeClient> {
        run_test_server()?;
        let env = Arc::new(EnvBuilder::new().build());
        let ch = ChannelBuilder::new(env).connect(NODE_END_POINT);
        Ok(NodeClient::new(ch))
    }

    fn test_node_server() -> DatenLordResult<()> {
        let node_client = build_node_client()?;

        info!("test node server publish unpublish");
        test_node_server_publish_unpublish(&node_client)
            .add_context("failed to test node publish unpublish")?;
        info!("test node server remount publish");
        test_node_server_remount_publish(&node_client)
            .add_context("failed to test node remount")?;
        info!("test node server multi publish");
        test_node_server_multiple_publish(&node_client)
            .add_context("failed to test node multi-mount")?;
        Ok(())
    }

    fn test_node_server_publish_unpublish(client: &NodeClient) -> DatenLordResult<()> {
        // Test node get capabilities
        let cap_req = NodeGetCapabilitiesRequest::new();
        let cap_resp = client
            .node_get_capabilities(&cap_req)
            .add_context("failed to get NodeGetCapabilitiesResponse")?;
        let caps = cap_resp.get_capabilities();
        let cap_vec = caps
            .iter()
            .map(|cap| cap.get_rpc().get_field_type())
            .collect::<Vec<_>>();
        assert_eq!(
            cap_vec,
            vec![NodeServiceCapability_RPC_Type::EXPAND_VOLUME,],
            "node_get_capabilities returns unexpected capabilities",
        );

        // Test node get info
        let info_req = NodeGetInfoRequest::new();
        let info_resp = client
            .node_get_info(&info_req)
            .add_context("failed to get NodeGetInfoResponse")?;
        assert_eq!(
            info_resp.get_node_id(),
            DEFAULT_NODE_NAME,
            "node name not match",
        );
        assert_eq!(
            info_resp.get_max_volumes_per_node(),
            util::MAX_VOLUMES_PER_NODE.cast::<i64>(),
            "max volumes per node not match",
        );
        let topology = info_resp.get_accessible_topology();
        assert_eq!(
            topology.get_segments().get(util::TOPOLOGY_KEY_NODE),
            Some(&DEFAULT_NODE_NAME.to_owned()), // Expect &String not &str
            "topology not match",
        );

        // Test publish volume
        let target_path = NODE_PUBLISH_VOLUME_TARGET_PATH;
        let vol_id = NODE_PUBLISH_VOLUME_ID;
        let mut mount_option = VolumeCapability_MountVolume::new();
        mount_option.set_fs_type("fuse".to_owned());
        mount_option.set_mount_flags(protobuf::RepeatedField::from_vec(vec![
            "nosuid".to_owned(),
            "nodev".to_owned(),
        ]));
        let mut vc = VolumeCapability::new();
        vc.set_mount(mount_option);
        vc.mut_access_mode()
            .set_mode(VolumeCapability_AccessMode_Mode::SINGLE_NODE_WRITER);
        let mut pub_req = NodePublishVolumeRequest::new();
        pub_req.set_volume_id(vol_id.to_owned());
        pub_req.set_volume_capability(vc);
        pub_req.set_target_path(target_path.to_owned());
        pub_req.set_readonly(false);
        pub_req
            .mut_volume_context()
            .insert(util::EPHEMERAL_KEY_CONTEXT.to_owned(), "true".to_owned());

        let _pub_resp1 = client
            .node_publish_volume(&pub_req)
            .add_context("failed to get NodePublishVolumeResponse")?;

        // Test expand volume
        let mut exp_req = NodeExpandVolumeRequest::new();
        exp_req.set_volume_id(vol_id.to_owned());
        exp_req.set_volume_path(target_path.to_owned());
        exp_req
            .mut_capacity_range()
            .set_required_bytes(util::MAX_VOLUME_STORAGE_CAPACITY);
        let exp_resp = client
            .node_expand_volume(&exp_req)
            .add_context("failed to get NodeExpandVolumeResponse")?;
        assert_eq!(
            exp_resp.get_capacity_bytes(),
            util::MAX_VOLUME_STORAGE_CAPACITY,
            "volume capacity not match after expend",
        );

        // Idempotency test for publish volume
        let _pub_resp2 = client
            .node_publish_volume(&pub_req)
            .add_context("failed to get NodePublishVolumeResponse")?;

        // Test unpublish volume
        let mut unpub_req = NodeUnpublishVolumeRequest::new();
        unpub_req.set_volume_id(vol_id.to_owned());
        unpub_req.set_target_path(target_path.to_owned());

        let _unpub_resp1 = client
            .node_unpublish_volume(&unpub_req)
            .add_context("failed to get NodeUnpublishVolumeResponse")?;

        Ok(())
    }

    fn test_node_server_remount_publish(client: &NodeClient) -> DatenLordResult<()> {
        // First publish volume
        let target_path = NODE_PUBLISH_VOLUME_TARGET_PATH;
        let vol_id = NODE_PUBLISH_VOLUME_ID;
        let mut mount_option = VolumeCapability_MountVolume::new();
        mount_option.set_fs_type("fuse".to_owned());
        mount_option.set_mount_flags(protobuf::RepeatedField::from_vec(vec![
            "nosuid".to_owned(),
            "nodev".to_owned(),
        ]));
        let mut vc = VolumeCapability::new();
        vc.set_mount(mount_option);
        vc.mut_access_mode()
            .set_mode(VolumeCapability_AccessMode_Mode::SINGLE_NODE_WRITER);
        let mut pub_req = NodePublishVolumeRequest::new();
        pub_req.set_volume_id(vol_id.to_owned());
        pub_req.set_volume_capability(vc);
        pub_req.set_target_path(target_path.to_owned());
        pub_req.set_readonly(false);
        pub_req
            .mut_volume_context()
            .insert(util::EPHEMERAL_KEY_CONTEXT.to_owned(), "true".to_owned());

        let _pub_resp1 = client
            .node_publish_volume(&pub_req)
            .add_context("failed to get first NodePublishVolumeResponse when test remount")?;

        // Second publish volume
        pub_req.set_readonly(true);

        let _pub_resp2 = client
            .node_publish_volume(&pub_req)
            .add_context("failed to get second NodePublishVolumeResponse when test remount")?;

        // Test unpublish volume
        let mut unpub_req = NodeUnpublishVolumeRequest::new();
        unpub_req.set_volume_id(vol_id.to_owned());
        unpub_req.set_target_path(target_path.to_owned());

        let _unpub_resp = client
            .node_unpublish_volume(&unpub_req)
            .add_context("failed to get NodeUnpublishVolumeResponse")?;

        // Verify second unpublish volume result should fail
        let failed_unpub_resp1 = client.node_unpublish_volume(&unpub_req);
        assert!(failed_unpub_resp1.is_err(), "unpublish again should fail");

        Ok(())
    }

    fn test_node_server_multiple_publish(client: &NodeClient) -> DatenLordResult<()> {
        // First publish volume
        let target_path1 = NODE_PUBLISH_VOLUME_TARGET_PATH_1;
        let vol_id = NODE_PUBLISH_VOLUME_ID;
        let mut mount_option = VolumeCapability_MountVolume::new();
        mount_option.set_fs_type("fuse".to_owned());
        mount_option.set_mount_flags(protobuf::RepeatedField::from_vec(vec![
            "nosuid".to_owned(),
            "nodev".to_owned(),
        ]));
        let mut vc = VolumeCapability::new();
        vc.set_mount(mount_option);
        vc.mut_access_mode()
            .set_mode(VolumeCapability_AccessMode_Mode::SINGLE_NODE_WRITER);
        let mut pub_req = NodePublishVolumeRequest::new();
        pub_req.set_volume_id(vol_id.to_owned());
        pub_req.set_volume_capability(vc);
        pub_req.set_target_path(target_path1.to_owned());
        pub_req.set_readonly(false);
        pub_req
            .mut_volume_context()
            .insert(util::EPHEMERAL_KEY_CONTEXT.to_owned(), "true".to_owned());

        let _pub_resp1 = client
            .node_publish_volume(&pub_req)
            .add_context("failed to get first NodePublishVolumeResponse")?;

        // Second publish volume
        let target_path2 = NODE_PUBLISH_VOLUME_TARGET_PATH_2;
        pub_req.set_target_path(target_path2.to_owned());
        let _pub_resp2 = client
            .node_publish_volume(&pub_req)
            .add_context("failed to get second NodePublishVolumeResponse")?;

        // First unpublish volume
        let mut unpub_req = NodeUnpublishVolumeRequest::new();
        unpub_req.set_volume_id(vol_id.to_owned());
        unpub_req.set_target_path(target_path1.to_owned());

        let _unpub_resp1 = client
            .node_unpublish_volume(&unpub_req)
            .add_context("failed to get first NodeUnpublishVolumeResponse")?;

        // Second unpublish volume
        unpub_req.set_target_path(target_path2.to_owned());
        let _unpub_resp2 = client
            .node_unpublish_volume(&unpub_req)
            .add_context("failed to get first NodeUnpublishVolumeResponse")?;
        Ok(())
    }
}
