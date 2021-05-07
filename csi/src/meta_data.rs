//! The utilities of meta data management

use grpcio::{ChannelBuilder, Environment};
use log::{debug, error, info, warn};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::convert::From;
use std::fmt::Debug;
use std::fs::{self, File};
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use utilities::Cast;

use super::csi::{
    CreateVolumeRequest, ListSnapshotsResponse_Entry, ListVolumesResponse_Entry,
    VolumeCapability_AccessMode_Mode, VolumeContentSource, VolumeContentSource_SnapshotSource,
    VolumeContentSource_VolumeSource, VolumeContentSource_oneof_type,
};
use super::datenlord_worker_grpc::WorkerClient;
use super::util::{self, BindMountMode, RunAsRole};
use common::error::{
    Context,
    DatenLordError::{
        ArgumentInvalid, NodeNotFound, SnapshotNotFound, SnapshotNotReady, StartingTokenInvalid,
        VolumeNotFound,
    },
    DatenLordResult,
};
use common::etcd_delegate::EtcdDelegate;

/// `DatenLord` node
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DatenLordNode {
    /// Node ID
    pub node_id: String,
    /// The port of worker service
    pub worker_port: u16,
    /// Node available space for storage
    pub max_available_space_bytes: i64,
    /// Max volumes per node
    pub max_volumes_per_node: i32,
    /// Node IP
    pub ip_address: IpAddr,
}

impl DatenLordNode {
    /// Create `DatenLordNode`
    pub const fn new(
        node_id: String,
        ip_address: IpAddr,
        worker_port: u16,
        max_available_space_bytes: i64,
        max_volumes_per_node: i32,
    ) -> Self {
        Self {
            node_id,
            worker_port,
            max_available_space_bytes,
            max_volumes_per_node,
            ip_address,
        }
    }
}

/// The data structure to store volume and snapshot meta data
pub struct MetaData {
    /// Volume and snapshot data directory
    data_dir: String,
    /// The plugin will save data persistently or not
    ephemeral: bool,
    /// The run as role, either controller or node
    run_as: RunAsRole,
    /// The list of etcd address and port
    etcd_delegate: EtcdDelegate,
    /// The meta data about this node
    node: DatenLordNode,
    // /// All volumes by ID
    // volume_meta_data: RwLock<HashMap<String, Arc<DatenLordVolume>>>,
    // /// All snapshots by ID
    // snapshot_meta_data: RwLock<HashMap<String, Arc<DatenLordSnapshot>>>,
}

/// The etcd key prefix to controller ID
const CONTROLLER_PREFIX: &str = "controller";
/// The etcd key prefix to node ID
const NODE_PREFIX: &str = "node";
/// The etcd key prefix to scheduler extender
const SCHEDULER_EXTENDER_PREFIX: &str = "scheduler_extender";
/// The etcd key prefix to node ID and snapshot ID
const NODE_SNAPSHOT_PREFIX: &str = "node_snapshot_id";
/// The etcd key prefix to node ID and volume ID
const NODE_VOLUME_PREFIX: &str = "node_volume_id";
/// The etcd key prefix to snapshot ID
const SNAPSHOT_ID_PREFIX: &str = "snapshot_id";
/// The etcd key prefix to snapshot name
const SNAPSHOT_NAME_PREFIX: &str = "snapshot_name";
/// The etcd key prefix to snapshot ID and source volume ID
const SNAPSHOT_SOURCE_ID_PREFIX: &str = "snapshot_source_id";
/// The etcd key prefix to volume ID
const VOLUME_ID_PREFIX: &str = "volume_id";
/// The etcd key prefix to volume name
const VOLUME_NAME_PREFIX: &str = "volume_name";
/// The etcd key prefix to volume bind mount path
const VOLUME_BIND_MOUNT_PATH_PREFIX: &str = "volume_bind_mount_path";
/// The separator used to split multiple volume bind mount paths
const VOLUME_BIND_MOUNT_PATH_SEPARATOR: &str = "\n";

impl MetaData {
    /// Create `MetaData`
    pub fn new(
        data_dir: String,
        ephemeral: bool,
        run_as: RunAsRole,
        etcd_delegate: EtcdDelegate,
        node: DatenLordNode,
    ) -> DatenLordResult<Self> {
        smol::block_on(async move {
            let md = Self {
                data_dir,
                ephemeral,
                run_as,
                etcd_delegate,
                node,
            };
            match md.run_as {
                RunAsRole::Controller => md.register_to_etcd(CONTROLLER_PREFIX).await?,
                RunAsRole::Node => md.register_to_etcd(NODE_PREFIX).await?,
                RunAsRole::SchedulerExtender => {
                    md.register_to_etcd(SCHEDULER_EXTENDER_PREFIX).await?
                }
            }

            Ok(md)
        })
    }

    /// Register this worker to etcd
    async fn register_to_etcd(&self, prefix: &str) -> DatenLordResult<()> {
        let key = format!("{}/{}", prefix, self.get_node_id());
        debug!("register node ID={} to etcd", key);
        self.etcd_delegate
            .write_or_update_kv(&key, &self.node)
            .await
            .with_context(|| {
                format!(
                    "failed to registration in etcd, the node ID={}",
                    self.get_node_id(),
                )
            })
    }

    /// Build `gRPC` client to `DatenLord` worker
    pub fn build_worker_client(node: &DatenLordNode) -> WorkerClient {
        // TODO: increase concurrent queue size
        let env = Arc::new(Environment::new(1));
        let work_address = if node.worker_port == 0 {
            util::LOCAL_WORKER_SOCKET.to_string()
        } else {
            format!("{}:{}", node.ip_address, node.worker_port)
        };
        let ch = ChannelBuilder::new(env).connect(&work_address);
        let client = WorkerClient::new(ch);
        debug!("build worker client to {}", work_address);
        client
    }

    /// Select a random node
    async fn select_random_node(&self) -> DatenLordResult<DatenLordNode> {
        // List key-value pairs with prefix
        let node_list: Vec<DatenLordNode> = self
            .etcd_delegate
            .get_list(&format!("{}/", NODE_PREFIX))
            .await?;
        debug_assert!(
            !node_list.is_empty(),
            "failed to retrieve node list from etcd"
        );
        let mut rng = rand::thread_rng();

        let idx = rng.gen_range(0..node_list.len());
        Ok(node_list
            .get(idx)
            .unwrap_or_else(|| {
                panic!(
                    "failed to get the {}-th node from returned node list, list={:?}",
                    idx, node_list,
                )
            })
            .clone())
    }

    /// Select a node to create volume or snapshot
    pub async fn select_node(&self, req: &CreateVolumeRequest) -> DatenLordResult<DatenLordNode> {
        let node_id = if req.has_volume_content_source() {
            let volume_src = req.get_volume_content_source();
            if volume_src.has_snapshot() {
                let snapshot_id = &volume_src.get_snapshot().snapshot_id;
                self.get_snapshot_by_id(snapshot_id)
                    .await
                    .map(|snapshot| Some(snapshot.node_id))?
            } else if volume_src.has_volume() {
                let volume_id = &volume_src.get_volume().volume_id;
                self.get_volume_by_id(volume_id)
                    .await
                    .map(|volume| Some(volume.node_id))?
            } else {
                None
            }
        } else {
            None
        };
        match node_id {
            None => {
                debug!("request doesn't have node id, select random node");
                self.select_random_node().await
            }
            Some(node_id) => {
                debug!("select node ID={} from request", node_id);
                if req.has_accessibility_requirements() {
                    let match_requisite = req
                        .get_accessibility_requirements()
                        .get_requisite()
                        .iter()
                        .any(|t| t.get_segments().iter().any(|(_k, v)| v == &node_id));
                    let match_preferred = req
                        .get_accessibility_requirements()
                        .get_preferred()
                        .iter()
                        .any(|t| t.get_segments().iter().any(|(_k, v)| v == &node_id));
                    if match_requisite || match_preferred {
                        self.get_node_by_id(&node_id)
                            .await
                            .or_else(|_| panic!("failed to get node ID={}", node_id))
                    } else {
                        panic!(
                            "select node ID={} is not in request required topology and preferred topology",
                            node_id,
                        );
                    }
                } else {
                    self.get_node_by_id(&node_id)
                        .await
                        .or_else(|_| panic!("failed to get node ID={}", node_id))
                }
            }
        }
    }

    /// Get volume absolute path by ID
    pub fn get_volume_path(&self, vol_id: &str) -> PathBuf {
        Path::new(&self.data_dir).join(vol_id)
    }

    /// Get snapshot path by ID
    pub fn get_snapshot_path(&self, snap_id: &str) -> PathBuf {
        let mut str_snap_id = snap_id.to_owned();
        str_snap_id.push_str(util::SNAPSHOT_EXT);
        Path::new(&self.data_dir).join(str_snap_id)
    }

    /// Get the node ID
    pub fn get_node_id(&self) -> &str {
        &self.node.node_id
    }

    /// Get node
    pub const fn get_node(&self) -> &DatenLordNode {
        &self.node
    }

    /// Is volume data ephemeral or not
    pub const fn is_ephemeral(&self) -> bool {
        self.ephemeral
    }

    /// Get max volumes per node
    pub const fn get_max_volumes_per_node(&self) -> i32 {
        self.node.max_volumes_per_node
    }

    /// Get node by ID
    pub async fn get_node_by_id(&self, node_id: &str) -> DatenLordResult<DatenLordNode> {
        let get_res = self
            .etcd_delegate
            .get_at_most_one_value(&format!("{}/{}", NODE_PREFIX, node_id))
            .await;
        match get_res {
            Ok(val) => val.ok_or(NodeNotFound {
                node_id: node_id.to_string(),
                context: vec![format!("Node ID={} is not found in etcd", node_id)],
            }),
            Err(e) => {
                warn!("failed to get node ID={}, the error is: {}", node_id, e);
                Err(e.with_context(|| format!("failed to get node ID={}", node_id)))
            }
        }
    }

    /// Get snapshot by ID
    pub async fn get_snapshot_by_id(&self, snap_id: &str) -> DatenLordResult<DatenLordSnapshot> {
        let get_res = self
            .etcd_delegate
            .get_at_most_one_value(&format!("{}/{}", SNAPSHOT_ID_PREFIX, snap_id))
            .await;
        match get_res {
            Ok(val) => val.ok_or(SnapshotNotFound {
                snapshot_id: snap_id.to_string(),
                context: vec![format!("Snapshot ID={} is not found in etcd", snap_id)],
            }),
            Err(e) => {
                warn!("failed to get snapshot ID={}, the error is: {}", snap_id, e);
                Err(e.with_context(|| format!("failed to get snapshot ID={} from etcd", snap_id)))
            }
        }
    }

    /// Find snapshot by name
    pub async fn get_snapshot_by_name(
        &self,
        snap_name: &str,
    ) -> DatenLordResult<DatenLordSnapshot> {
        let snap_name_key = format!("{}/{}", SNAPSHOT_NAME_PREFIX, snap_name);
        let snap_id: String = match self
            .etcd_delegate
            .get_at_most_one_value(&snap_name_key)
            .await
        {
            Ok(val) => {
                if let Some(sid) = val {
                    sid
                } else {
                    debug!("failed to find snapshot name={} from etcd", snap_name);
                    return Err(SnapshotNotFound {
                        snapshot_id: snap_name.to_string(),
                        context: vec![format!("Snapshot name={} is not found in etcd", snap_name)],
                    });
                }
            }
            Err(e) => {
                debug!(
                    "failed to find snapshot name={} from etcd, the error is: {}",
                    snap_name, e
                );
                return Err(e.with_context(|| {
                    format!("failed to find snapshot name={} from etcd", snap_name)
                }));
            }
        };
        debug!("found snap ID={} and name={} from etcd", snap_id, snap_name,);
        self.get_snapshot_by_id(&snap_id).await
    }

    /// Find snapshot by source volume ID, each source volume ID has one snapshot at most
    pub async fn get_snapshot_by_src_volume_id(
        &self,
        src_volume_id: &str,
    ) -> DatenLordResult<DatenLordSnapshot> {
        let src_vol_id_key = format!("{}/{}", SNAPSHOT_SOURCE_ID_PREFIX, src_volume_id);
        let snap_id: String = match self
            .etcd_delegate
            .get_at_most_one_value(&src_vol_id_key)
            .await
        {
            Ok(val) => {
                if let Some(s) = val {
                    s
                } else {
                    debug!(
                        "failed to find snapshot by source volume ID={} from etcd",
                        src_volume_id
                    );
                    return Err(SnapshotNotFound {
                        snapshot_id: "".to_string(),
                        context: vec![format!(
                            "failed to find snapshot by source volume ID={} from etcd",
                            src_volume_id
                        )],
                    });
                }
            }
            Err(e) => {
                debug!(
                    "failed to find snapshot by source volume ID={} from etcd, the error is: {}",
                    src_volume_id, e,
                );
                return Err(e.with_context(|| {
                    format!(
                        "failed to find snapshot by source volume ID={} from etcd",
                        src_volume_id
                    )
                }));
            }
        };
        debug!(
            "found snap ID={} by source volume ID={} from etcd",
            snap_id, src_volume_id,
        );
        self.get_snapshot_by_id(&snap_id).await
    }

    /// The helper function to list elements
    fn list_helper<E, T, F>(
        collection: impl Into<Vec<E>>,
        starting_token: &str,
        max_entries: i32,
        f: F,
    ) -> DatenLordResult<(Vec<T>, usize)>
    where
        F: Fn(&E) -> Option<T>,
    {
        let vector = collection.into();
        let total_num = vector.len();
        let starting_pos = if starting_token.is_empty() {
            0
        } else if let Ok(i) = starting_token.parse::<usize>() {
            i
        } else {
            return Err(StartingTokenInvalid {
                starting_token: starting_token.to_string(),
                context: vec![format!("invalid starting position {}", starting_token)],
            });
        };
        if starting_pos > 0 && starting_pos >= total_num {
            return Err(StartingTokenInvalid {
                starting_token: starting_token.to_string(),
                context: vec![format!(
                    "invalid starting token={}, larger than or equal to the list size={} of volumes",
                    starting_token,
                    total_num,
                )],
            });
        }
        let (remaining, ofr) = total_num.overflowing_sub(starting_pos);
        debug_assert!(
            !ofr,
            "total_num={} subtract num_to_list={} overflowed",
            total_num, starting_pos,
        );

        let num_to_list = if max_entries > 0 {
            if remaining < max_entries.cast() {
                remaining
            } else {
                max_entries.cast()
            }
        } else {
            remaining
        };
        let (next_pos, ofnp) = starting_pos.overflowing_add(num_to_list);
        debug_assert!(
            !ofnp,
            "sarting_pos={} add num_to_list={} overflowed",
            starting_pos, num_to_list,
        );

        let result_vec = vector
            .iter()
            .enumerate()
            .filter_map(|(idx, elem)| {
                if idx < starting_pos {
                    return None;
                }
                let (end_idx_not_list, ofen) = starting_pos.overflowing_add(num_to_list);
                debug_assert_eq!(
                    ofen, false,
                    "starting_pos={} add num_to_list={} overflowed",
                    starting_pos, num_to_list,
                );
                if idx >= end_idx_not_list {
                    return None;
                }
                f(elem)
            })
            .collect::<Vec<_>>();
        Ok((result_vec, next_pos))
    }

    /// List volumes
    pub async fn list_volumes(
        &self,
        starting_token: &str,
        max_entries: i32,
    ) -> DatenLordResult<(Vec<ListVolumesResponse_Entry>, usize)> {
        let vol_list: Vec<DatenLordVolume> = self
            .etcd_delegate
            .get_list(&format!("{}/", VOLUME_ID_PREFIX))
            .await?;

        Self::list_helper(vol_list, starting_token, max_entries, |vol| {
            let mut entry = ListVolumesResponse_Entry::new();
            entry.mut_volume().set_capacity_bytes(vol.get_size());
            entry.mut_volume().set_volume_id(vol.vol_id.clone());
            entry.mut_volume().set_content_source(VolumeContentSource {
                field_type: vol.content_source.as_ref().map(|vcs| vcs.clone().into()),
                ..VolumeContentSource::default()
            });

            Some(entry)
        })
    }

    /// List snapshots except those creation time failed to convert to proto timestamp
    pub async fn list_snapshots(
        &self,
        starting_token: &str,
        max_entries: i32,
    ) -> DatenLordResult<(Vec<ListSnapshotsResponse_Entry>, usize)> {
        let snap_list: Vec<DatenLordSnapshot> = self
            .etcd_delegate
            .get_list(&format!("{}/", SNAPSHOT_ID_PREFIX))
            .await?;

        Self::list_helper(snap_list, starting_token, max_entries, |snap| {
            let mut entry = ListSnapshotsResponse_Entry::new();
            entry.mut_snapshot().set_size_bytes(snap.size_bytes);
            entry.mut_snapshot().set_snapshot_id(snap.snap_id.clone());
            entry
                .mut_snapshot()
                .set_source_volume_id(snap.vol_id.clone());
            entry.mut_snapshot().set_creation_time(
                match util::generate_proto_timestamp(&snap.creation_time) {
                    Ok(ts) => ts,
                    Err(e) => panic!("failed to generate proto timestamp, the error is: {}", e,),
                },
            );
            entry.mut_snapshot().set_ready_to_use(snap.ready_to_use);

            Some(entry)
        })
    }

    /// Find volume by ID
    pub async fn find_volume_by_id(&self, vol_id: &str) -> DatenLordResult<bool> {
        match self.get_volume_by_id(vol_id).await {
            Ok(_) => Ok(true),
            Err(e) => {
                if let VolumeNotFound { .. } = e {
                    Ok(false)
                } else {
                    Err(e)
                }
            }
        }
    }

    /// Get volume by ID
    pub async fn get_volume_by_id(&self, vol_id: &str) -> DatenLordResult<DatenLordVolume> {
        match self
            .etcd_delegate
            .get_at_most_one_value(&format!("{}/{}", VOLUME_ID_PREFIX, vol_id))
            .await
        {
            Ok(val) => val.ok_or(VolumeNotFound {
                volume_id: vol_id.to_string(),
                context: vec![format!("Volume ID={} is not found in etcd", vol_id)],
            }),
            Err(e) => {
                debug!(
                    "failed to find volume ID={} from etcd, the error is: {}",
                    vol_id, e
                );
                Err(e.with_context(|| format!("failed to find volume ID={} from etcd", vol_id)))
            }
        }
    }

    /// Get volume by name
    pub async fn get_volume_by_name(&self, vol_name: &str) -> DatenLordResult<DatenLordVolume> {
        let vol_name_key = format!("{}/{}", VOLUME_NAME_PREFIX, vol_name);
        let vol_id: String = match self
            .etcd_delegate
            .get_at_most_one_value(&vol_name_key)
            .await
        {
            Ok(val) => {
                if let Some(v) = val {
                    v
                } else {
                    debug!("volume with name={} is not found in etcd", vol_name,);
                    return Err(VolumeNotFound {
                        volume_id: vol_name.to_string(),
                        context: vec![format!(
                            "Volume with name={} is not found in etcd",
                            vol_name
                        )],
                    });
                }
            }
            Err(e) => {
                debug!(
                    "failed to find volume by name={} from etcd, the error is: {}",
                    vol_name, e,
                );
                return Err(e.with_context(|| {
                    format!("failed to find volume by name={} from etcd", vol_name)
                }));
            }
        };
        debug!("found volume ID={} and name={} from etcd", vol_id, vol_name);
        self.get_volume_by_id(&vol_id).await
    }

    /// Add new snapshot meta data
    pub async fn add_snapshot_meta_data(
        &self,
        snap_id: &str,
        snapshot: &DatenLordSnapshot,
    ) -> DatenLordResult<()> {
        info!("adding the meta data of snapshot ID={}", snap_id);
        let snap_id_str = snap_id.to_owned();

        // TODO: use etcd transancation?
        let snap_id_key = format!("{}/{}", SNAPSHOT_ID_PREFIX, snap_id);
        self.etcd_delegate
            .write_new_kv(&snap_id_key, snapshot)
            .await
            .with_context(|| format!("failed to add new snapshot key={} to etcd", snap_id_key))?;

        let snap_name_key = format!("{}/{}", SNAPSHOT_NAME_PREFIX, snapshot.snap_name);
        self.etcd_delegate
            .write_new_kv(&snap_name_key, &snap_id_str)
            .await
            .with_context(|| format!("failed to add new snapshot key={} to etcd", snap_name_key))?;

        let snap_source_id_key = format!("{}/{}", SNAPSHOT_SOURCE_ID_PREFIX, snapshot.vol_id);
        self.etcd_delegate
            .write_new_kv(&snap_source_id_key, &snap_id_str)
            .await
            .with_context(|| {
                format!(
                    "failed to add new snapshot key={} to etcd",
                    snap_source_id_key,
                )
            })?;

        let node_snap_key = format!("{}/{}/{}", NODE_SNAPSHOT_PREFIX, snapshot.node_id, snap_id);
        self.etcd_delegate
            .write_new_kv(&node_snap_key, &snapshot.ready_to_use)
            .await
            .with_context(|| format!("failed to add new snapshot key={} to etcd", node_snap_key))?;

        Ok(())
    }

    /// Delete the snapshot meta data
    pub async fn delete_snapshot_meta_data(
        &self,
        snap_id: &str,
    ) -> DatenLordResult<DatenLordSnapshot> {
        info!("deleting the meta data of snapshot ID={}", snap_id);

        // TODO: use etcd transancation?
        let snap_id_key = format!("{}/{}", SNAPSHOT_ID_PREFIX, snap_id);
        let snap_id_pre_value: DatenLordSnapshot = self
            .etcd_delegate
            .delete_exact_one_value(&snap_id_key)
            .await?;

        let snap_name_key = format!("{}/{}", SNAPSHOT_NAME_PREFIX, snap_id_pre_value.snap_name);
        let snap_name_pre_value: String = self
            .etcd_delegate
            .delete_exact_one_value(&snap_name_key)
            .await?;

        let snap_source_id_key =
            format!("{}/{}", SNAPSHOT_SOURCE_ID_PREFIX, snap_id_pre_value.vol_id);
        let snap_source_pre_value: String = self
            .etcd_delegate
            .delete_exact_one_value(&snap_source_id_key)
            .await?;

        let node_snap_key = format!(
            "{}/{}/{}",
            NODE_SNAPSHOT_PREFIX, snap_id_pre_value.node_id, snap_id
        );
        let _node_snap_pre_value: bool = self
            .etcd_delegate
            .delete_exact_one_value(&node_snap_key)
            .await?;

        debug!(
            "deleted snapshot ID={} name={} source volume ID={} at node ID={}",
            snap_id, snap_name_pre_value, snap_source_pre_value, snap_id_pre_value.node_id,
        );
        Ok(snap_id_pre_value)
    }

    /// Update the existing volume meta data
    pub async fn update_volume_meta_data(
        &self,
        vol_id: &str,
        volume: &DatenLordVolume,
    ) -> DatenLordResult<DatenLordVolume> {
        info!("updating the meta data of volume ID={}", vol_id);
        let vol_id_str = vol_id.to_owned();

        let vol_id_key = format!("{}/{}", VOLUME_ID_PREFIX, vol_id);
        let vol_id_pre_value = self
            .etcd_delegate
            .update_existing_kv(&vol_id_key, volume)
            .await?;
        debug_assert_eq!(
            vol_id_pre_value.vol_id, vol_id,
            "replaced volume key={} value not match",
            vol_id_key,
        );

        let vol_name_key = format!("{}/{}", VOLUME_NAME_PREFIX, volume.vol_name);
        let vol_name_pre_value = self
            .etcd_delegate
            .update_existing_kv(&vol_name_key, &vol_id_str)
            .await?;
        debug_assert_eq!(
            vol_name_pre_value, vol_id,
            "replaced volume key={} value not match",
            vol_name_key,
        );

        // Volume ephemeral field cannot be changed
        let node_vol_key = format!("{}/{}/{}", NODE_VOLUME_PREFIX, volume.node_id, vol_id);
        let node_vol_pre_value = self
            .etcd_delegate
            .update_existing_kv(&node_vol_key, &volume.ephemeral)
            .await?;
        debug_assert_eq!(
            node_vol_pre_value, vol_id_pre_value.ephemeral,
            "replaced volume key={} value not match",
            node_vol_key,
        );

        Ok(vol_id_pre_value)
    }

    /// Add new volume meta data
    pub async fn add_volume_meta_data(
        &self,
        vol_id: &str,
        volume: &DatenLordVolume,
    ) -> DatenLordResult<()> {
        info!("adding the meta data of volume ID={}", vol_id);
        let vol_id_str = vol_id.to_owned();

        let vol_id_key = format!("{}/{}", VOLUME_ID_PREFIX, vol_id);
        self.etcd_delegate
            .write_new_kv(&vol_id_key, volume)
            .await
            .with_context(|| format!("failed to add new volume key={} to etcd", vol_id_key,))?;

        let vol_name_key = format!("{}/{}", VOLUME_NAME_PREFIX, volume.vol_name);
        self.etcd_delegate
            .write_new_kv(&vol_name_key, &vol_id_str)
            .await
            .with_context(|| format!("failed to add new volume key={} to etcd", vol_name_key,))?;

        let node_vol_key = format!("{}/{}/{}", NODE_VOLUME_PREFIX, volume.node_id, vol_id);
        self.etcd_delegate
            .write_new_kv(&node_vol_key, &volume.ephemeral)
            .await
            .with_context(|| format!("failed to add new volume key={} to etcd", node_vol_key,))?;

        Ok(())
    }

    /// Delete the volume meta data
    pub async fn delete_volume_meta_data(&self, vol_id: &str) -> DatenLordResult<DatenLordVolume> {
        info!("deleting volume ID={}", vol_id);

        // TODO: use etcd transancation?
        let vol_id_key = format!("{}/{}", VOLUME_ID_PREFIX, vol_id);
        let vol_id_pre_value: DatenLordVolume = self
            .etcd_delegate
            .delete_exact_one_value(&vol_id_key)
            .await?;

        let vol_name_key = format!("{}/{}", VOLUME_NAME_PREFIX, vol_id_pre_value.vol_name);
        let vol_name_pre_value: String = self
            .etcd_delegate
            .delete_exact_one_value(&vol_name_key)
            .await?;

        let node_vol_key = format!(
            "{}/{}/{}",
            NODE_VOLUME_PREFIX, vol_id_pre_value.node_id, vol_id
        );
        let _node_vol_pre_value: bool = self
            .etcd_delegate
            .delete_exact_one_value(&node_vol_key)
            .await?;

        let get_mount_path_res = self.get_volume_bind_mount_path(vol_id).await;
        if let Ok(pre_mount_path_vec) = get_mount_path_res {
            let vol_id_owned = vol_id.to_owned();
            let pre_mount_path_vec_ref = Arc::new(pre_mount_path_vec);
            let pre_mount_path_vec_ref_clone =
                Arc::<HashSet<String>>::clone(&pre_mount_path_vec_ref);
            smol::unblock(move || {
                pre_mount_path_vec_ref_clone
                    .iter()
                    .for_each(|pre_mount_path| {
                        let umount_res = util::umount_volume_bind_path(pre_mount_path);
                        if let Err(e) = umount_res {
                            panic!(
                                "failed to un-mount volume ID={} bind path={}, \
                                    the error is: {}",
                                vol_id_owned, pre_mount_path, e,
                            );
                        }
                    });
            })
            .await;
            if !pre_mount_path_vec_ref.is_empty() {
                let deleted_path_vec = self.delete_volume_all_bind_mount_path(vol_id).await?;
                debug_assert_eq!(
                    *pre_mount_path_vec_ref, deleted_path_vec,
                    "the volume bind mount paths and \
                        the deleted paths not match when delete volume meta data",
                );
            }
        }
        debug!(
            "deleted volume ID={} name={} at node ID={}",
            vol_id, vol_name_pre_value, vol_id_pre_value.node_id,
        );
        Ok(vol_id_pre_value)
    }

    /// Decompress snapshot of volume to destination
    fn decompress_snapshot(snap_path: &Path, dst_path: &Path) -> DatenLordResult<()> {
        let tar_gz = File::open(snap_path)
            .with_context(|| format!("failed to open path={:?}", snap_path))?;

        let tar_file = flate2::read::GzDecoder::new(tar_gz);
        let mut archive = tar::Archive::new(tar_file);
        archive
            .unpack(dst_path)
            .with_context(|| format!("failed to decompress snapshot to {:?}", dst_path))?;
        Ok(())
    }

    /// Populate the given destPath with data from the snapshot ID
    pub async fn copy_volume_from_snapshot(
        &self,
        dst_volume_size: i64,
        src_snapshot_id: &str,
        dst_volume_id: &str,
    ) -> DatenLordResult<()> {
        let dst_path = self.get_volume_path(dst_volume_id);

        let src_snapshot = self.get_snapshot_by_id(src_snapshot_id).await?;
        assert_eq!(
            src_snapshot.node_id,
            self.get_node_id(),
            "snapshot ID={} is on node ID={} not on local node ID={}",
            src_snapshot_id,
            src_snapshot.node_id,
            self.get_node_id(),
        );
        if !src_snapshot.ready_to_use {
            error!(
                "source snapshot ID={} and name={} is not yet ready to use",
                src_snapshot.snap_id, src_snapshot.snap_name
            );
            return Err(SnapshotNotReady {
                snapshot_id: src_snapshot.snap_id.to_owned(),
                context: vec![],
            });
        }
        if src_snapshot.size_bytes > dst_volume_size {
            let error_msg = format!(
                "source snapshot ID={} and name={} has size={} \
                            greater than requested volume size={}",
                src_snapshot.snap_id,
                src_snapshot.snap_name,
                src_snapshot.size_bytes,
                dst_volume_size
            );

            error!("{}", &error_msg);
            return Err(ArgumentInvalid {
                context: vec![error_msg],
            });
        }

        debug_assert!(
            dst_path.is_dir(),
            "the volume of mount access type should have a directory path: {:?}",
            dst_path,
        );
        let snap_path_owned = src_snapshot.snap_path.to_owned();
        let dst_path_owned = dst_path.to_owned();
        smol::unblock(move || Self::decompress_snapshot(&snap_path_owned, &dst_path_owned)).await?;

        Ok(())
    }

    /// Populate the given destPath with data from the `src_volume_id`
    pub async fn copy_volume_from_volume(
        &self,
        dst_volume_size: i64,
        src_volume_id: &str,
        dst_volume_id: &str,
    ) -> DatenLordResult<()> {
        let dst_path = self.get_volume_path(dst_volume_id);

        let src_volume = self.get_volume_by_id(src_volume_id).await?;
        assert_eq!(
            src_volume.node_id,
            self.get_node_id(),
            "volume ID={} is on node ID={} not on local node ID={}",
            src_volume_id,
            src_volume.node_id,
            self.get_node_id(),
        );
        if src_volume.get_size() > dst_volume_size {
            return Err(ArgumentInvalid {
                context: vec![format!(
                    "source volume ID={} and name={} has size={} \
                                greater than requested volume size={}",
                    src_volume.vol_id,
                    src_volume.vol_name,
                    src_volume.get_size(),
                    dst_volume_size,
                )],
            });
        }

        let vol_path_owned = src_volume.vol_path.to_owned();
        let dst_path_owned = dst_path.to_owned();
        let copy_res = smol::unblock(move || {
            util::copy_directory_recursively(
                &vol_path_owned,
                &dst_path_owned,
                false, // follow symlink or not
            )
        })
        .await
        .with_context(|| {
            format!(
                "failed to pre-populate data from source mount volume {} and name={}",
                src_volume.vol_id, src_volume.vol_name,
            )
        });
        match copy_res {
            Ok(copy_size) => {
                info!(
                    "successfully copied {} files from {:?} to {:?}",
                    copy_size, src_volume.vol_path, dst_path,
                );
            }
            Err(e) => {
                return Err(e.with_context(|| {
                    format!(
                        "failed to pre-populate data from source mount volume {} and name={}",
                        src_volume.vol_id, src_volume.vol_name,
                    )
                }))
            }
        }

        Ok(())
    }

    /// Delete one bind path of a volume from etcd,
    /// return all bind paths before deletion
    pub async fn delete_volume_one_bind_mount_path(
        &self,
        vol_id: &str,
        mount_path: &str,
    ) -> DatenLordResult<HashSet<String>> {
        let mut mount_path_set =
            self.get_volume_bind_mount_path(vol_id)
                .await
                .with_context(|| {
                    format!("failed to get the bind mount paths of volume ID={}", vol_id,)
                })?;
        if mount_path_set.contains(mount_path) {
            let volume_mount_path_key = format!(
                "{}/{}/{}",
                VOLUME_BIND_MOUNT_PATH_PREFIX,
                self.get_node_id(),
                vol_id,
            );
            mount_path_set.remove(mount_path);
            if mount_path_set.is_empty() {
                // Delete the last mount path
                self.delete_volume_all_bind_mount_path(vol_id).await
            } else {
                let mount_path_value_in_etcd = mount_path_set
                    .into_iter()
                    .collect::<Vec<_>>()
                    .join(VOLUME_BIND_MOUNT_PATH_SEPARATOR);
                let volume_mount_paths: String = self
                    .etcd_delegate
                    .update_existing_kv(&volume_mount_path_key, &mount_path_value_in_etcd)
                    .await
                    .with_context(|| {
                        format!(
                            "failed to delete one mount path={} of volume ID={}",
                            mount_path, vol_id
                        )
                    })?;
                Ok((&volume_mount_paths)
                    .split(VOLUME_BIND_MOUNT_PATH_SEPARATOR)
                    .map(std::borrow::ToOwned::to_owned)
                    .collect())
            }
        } else {
            Ok(mount_path_set)
        }
    }

    /// Delete all bind path of a volume from etcd
    pub async fn delete_volume_all_bind_mount_path(
        &self,
        vol_id: &str,
    ) -> DatenLordResult<HashSet<String>> {
        let volume_mount_path_key = format!(
            "{}/{}/{}",
            VOLUME_BIND_MOUNT_PATH_PREFIX,
            self.get_node_id(),
            vol_id,
        );
        let mount_paths: String = self
            .etcd_delegate
            .delete_exact_one_value(&volume_mount_path_key)
            .await?;

        Ok((&mount_paths)
            .split(VOLUME_BIND_MOUNT_PATH_SEPARATOR)
            .map(std::borrow::ToOwned::to_owned)
            .collect())
    }

    /// Get volume bind mount path from etcd
    pub async fn get_volume_bind_mount_path(
        &self,
        vol_id: &str,
    ) -> DatenLordResult<HashSet<String>> {
        let volume_mount_path_key = format!(
            "{}/{}/{}",
            VOLUME_BIND_MOUNT_PATH_PREFIX,
            self.get_node_id(),
            vol_id,
        );
        let get_opt: Option<String> = self
            .etcd_delegate
            .get_at_most_one_value(&volume_mount_path_key)
            .await?;
        match get_opt {
            Some(pre_mount_paths) => Ok((&pre_mount_paths)
                .split(VOLUME_BIND_MOUNT_PATH_SEPARATOR)
                .map(std::borrow::ToOwned::to_owned)
                .collect()),
            None => Ok(HashSet::new()),
        }
    }

    /// Write volume bind mount path to etcd,
    /// if volume has multiple bind mount path, then append to the value in etcd
    // TODO: make it fault tolerant when etcd is down
    async fn save_volume_bind_mount_path(
        &self,
        vol_id: &str,
        target_path: &str,
        bind_mount_mode: BindMountMode,
    ) {
        let get_mount_path_res = self.get_volume_bind_mount_path(vol_id).await;
        let mut mount_path_set = match get_mount_path_res {
            Ok(v) => v,
            Err(_) => HashSet::new(),
        };
        let volume_mount_path_key = format!(
            "{}/{}/{}",
            VOLUME_BIND_MOUNT_PATH_PREFIX,
            self.get_node_id(),
            vol_id,
        );
        let target_path_str = target_path.to_owned();
        mount_path_set.insert(target_path_str);
        let mount_path_value_in_etcd = mount_path_set
            .into_iter()
            .collect::<Vec<_>>()
            .join(VOLUME_BIND_MOUNT_PATH_SEPARATOR);
        match bind_mount_mode {
            BindMountMode::Single => {
                let write_res = self
                    .etcd_delegate
                    .write_new_kv(&volume_mount_path_key, &mount_path_value_in_etcd)
                    .await;
                if let Err(e) = write_res {
                    panic!(
                        "failed to write the mount path={} of volume ID={} to etcd, \
                            the error is: {}",
                        target_path, vol_id, e,
                    );
                }
            }
            BindMountMode::Multiple => {
                let update_res = self
                    .etcd_delegate
                    .update_existing_kv(&volume_mount_path_key, &mount_path_value_in_etcd)
                    .await;
                match update_res {
                    Ok(pre_mount_paths) => {
                        debug_assert!(
                            !pre_mount_paths.contains(target_path),
                            "the previous mount paths={} of volume ID={} \
                                should not contain new mount path={}",
                            pre_mount_paths,
                            vol_id,
                            target_path,
                        );
                    }
                    Err(e) => panic!(
                        "failed to add the mount path={} of volume ID={} to etcd, \
                            the error is: {}",
                        target_path, vol_id, e,
                    ),
                }
            }
            BindMountMode::Remount => {
                debug!("no need to update volume mount path in etcd when re-mount");
            }
        }
    }

    /// Bind mount volume directory to target path if root
    pub async fn bind_mount(
        &self,
        target_dir: &str,
        fs_type: &str,
        read_only: bool,
        vol_id: &str,
        mount_options: &str,
        ephemeral: bool,
    ) -> DatenLordResult<()> {
        let vol_path = self.get_volume_path(vol_id);
        // Bind mount from target_path to vol_path if run as root
        let target_path = Path::new(target_dir);
        if target_path.exists() {
            debug!("found target bind mount directory={:?}", target_path);
        } else {
            fs::create_dir_all(target_path).with_context(|| {
                format!(
                    "failed to create target bind mount directory={:?}",
                    target_dir,
                )
            })?;
        };

        let get_mount_path_res = self.get_volume_bind_mount_path(vol_id).await;
        let bind_mount_mode = match get_mount_path_res {
            Ok(pre_mount_path_set) => {
                if pre_mount_path_set.is_empty() {
                    BindMountMode::Single
                } else if pre_mount_path_set.contains(target_dir) {
                    debug!("re-mount volume ID={} to path={:?}", vol_id, target_path);
                    BindMountMode::Remount
                } else {
                    debug!("mount volume ID={} to a new path={:?}", vol_id, target_path);
                    BindMountMode::Multiple
                }
            }
            Err(e) => panic!(
                "failed to get mount path of volume ID={} from etcd, the error is: {}",
                vol_id, e,
            ),
        };
        let vol_path_owned = vol_path.to_owned();
        let target_path_owned = target_path.to_owned();
        let fs_type_owned = fs_type.to_owned();
        let mount_options_owned = mount_options.to_owned();
        let mount_res = smol::unblock(move || {
            util::mount_volume_bind_path(
                &vol_path_owned,
                &target_path_owned,
                bind_mount_mode,
                &mount_options_owned,
                &fs_type_owned,
                read_only,
            )
        })
        .await
        .with_context(|| {
            format!(
                "failed to bind mount from {:?} to {:?}",
                vol_path, target_path,
            )
        });
        if let Err(bind_err) = mount_res {
            if ephemeral {
                match self.delete_volume_meta_data(vol_id).await {
                    Ok(_) => debug!(
                        "successfully deleted ephemeral volume ID={}, when bind mount failed",
                        vol_id,
                    ),
                    Err(e) => error!(
                        "failed to delete ephemeral volume ID={}, \
                            when bind mount failed, the error is: {}",
                        vol_id, e,
                    ),
                }
            }
            return Err(bind_err);
        } else {
            info!(
                "successfully bind mounted volume path={:?} to target path={:?}",
                vol_path, target_dir,
            );
            self.save_volume_bind_mount_path(vol_id, target_dir, bind_mount_mode)
                .await;
        }

        Ok(())
    }

    /// Compress a volume and save as a tar file
    fn compress_volume(src_vol: &DatenLordVolume, snap_path: &Path) -> DatenLordResult<()> {
        /// Remove bad snapshot when compress error
        fn remove_bad_snapshot(snap_path: &Path) {
            let remove_res = fs::remove_file(snap_path)
                .with_context(|| format!("failed to remove bad snapshot file {:?}", snap_path,));
            if let Err(remove_err) = remove_res {
                error!(
                    "failed to remove bad snapshot file {:?}, the error is: {}",
                    snap_path, remove_err,
                );
            }
        }

        let vol_path = &src_vol.vol_path;
        let tar_gz = File::create(&snap_path)
            .with_context(|| format!("failed to create snapshot file {:?}", snap_path))?;
        let gz_file = flate2::write::GzEncoder::new(tar_gz, flate2::Compression::default());
        let mut tar_file = tar::Builder::new(gz_file);
        let tar_res = tar_file.append_dir_all("./", vol_path).with_context(|| {
            format!(
                "failed to generate snapshot for volume ID={} and name={}",
                src_vol.vol_id, src_vol.vol_name,
            )
        });
        if let Err(append_err) = tar_res {
            remove_bad_snapshot(snap_path);
            return Err(append_err);
        }
        let into_res = tar_file.into_inner().with_context(|| {
            format!(
                "failed to generate snapshot for volume ID={} and name={}",
                src_vol.vol_id, src_vol.vol_name,
            )
        });
        match into_res {
            Ok(gz_file) => {
                let gz_finish_res = gz_file.finish().with_context(|| {
                    format!(
                        "failed to generate snapshot for volume ID={} and name={}",
                        src_vol.vol_id, src_vol.vol_name,
                    )
                });
                if let Err(finish_err) = gz_finish_res {
                    remove_bad_snapshot(snap_path);
                    return Err(finish_err);
                }
            }
            Err(into_err) => {
                remove_bad_snapshot(snap_path);
                return Err(into_err);
            }
        }
        Ok(())
    }
    /// Build snapshot from source volume
    pub async fn build_snapshot_from_volume(
        &self,
        src_vol_id: &str,
        snap_id: &str,
        snap_name: &str,
    ) -> DatenLordResult<DatenLordSnapshot> {
        let src_vol = self.get_volume_by_id(src_vol_id).await?;
        assert_eq!(
            src_vol.node_id,
            self.get_node_id(),
            "volume ID={} is on node ID={} not on local node ID={}",
            src_vol_id,
            src_vol.node_id,
            self.get_node_id(),
        );

        let snap_path = self.get_snapshot_path(snap_id);
        let snap_path_owned = snap_path.to_owned();
        let src_vol_owned = src_vol.to_owned();

        smol::unblock(move || Self::compress_volume(&src_vol_owned, &snap_path_owned)).await?;

        let now = smol::unblock(std::time::SystemTime::now).await;
        let snapshot = DatenLordSnapshot::new(
            snap_name.to_owned(),
            snap_id.to_string(),
            src_vol_id.to_owned(),
            self.get_node_id().to_owned(),
            snap_path,
            now,
            src_vol.get_size(),
        );

        Ok(snapshot)
    }

    /// Expand volume size, return previous size
    pub async fn expand(
        &self,
        volume: &mut DatenLordVolume,
        new_size_bytes: i64,
    ) -> DatenLordResult<i64> {
        let old_size_bytes = volume.get_size();
        if volume.expand_size(new_size_bytes) {
            let prv_vol = self.update_volume_meta_data(&volume.vol_id, volume).await?;
            assert_eq!(
                prv_vol.get_size(),
                old_size_bytes,
                "the volume size before expand not match"
            );
            Ok(old_size_bytes)
        } else {
            Err(ArgumentInvalid {
                context: vec![format!(
                    "the new size={} is smaller than original size={}",
                    new_size_bytes, old_size_bytes,
                )],
            })
        }
    }
}

/// Volume access mode, copied from `VolumeCapability_AccessMode_Mode`
/// because `VolumeCapability_AccessMode_Mode` is not serializable
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum VolumeAccessMode {
    /// Volume access mode unknow
    Unknown = 0,
    /// Can only be published once as read/write on a single node, at
    /// any given time.
    SingleNodeWriter = 1,
    /// Can only be published once as readonly on a single node, at
    /// any given time.
    SingleNodeReadOnly = 2,
    /// Can be published as readonly at multiple nodes simultaneously.
    MultiNodeReadOnly = 3,
    /// Can be published at multiple nodes simultaneously. Only one of
    /// the node can be used as read/write. The rest will be readonly.
    MultiNodeSingleWriter = 4,
    /// Can be published as read/write at multiple nodes
    /// simultaneously.
    MultiNodeMultiWriter = 5,
}

impl From<VolumeCapability_AccessMode_Mode> for VolumeAccessMode {
    fn from(vc: VolumeCapability_AccessMode_Mode) -> Self {
        match vc {
            VolumeCapability_AccessMode_Mode::UNKNOWN => Self::Unknown,
            VolumeCapability_AccessMode_Mode::SINGLE_NODE_WRITER => Self::SingleNodeWriter,
            VolumeCapability_AccessMode_Mode::SINGLE_NODE_READER_ONLY => Self::SingleNodeReadOnly,
            VolumeCapability_AccessMode_Mode::MULTI_NODE_READER_ONLY => Self::MultiNodeReadOnly,
            VolumeCapability_AccessMode_Mode::MULTI_NODE_SINGLE_WRITER => {
                Self::MultiNodeSingleWriter
            }
            VolumeCapability_AccessMode_Mode::MULTI_NODE_MULTI_WRITER => Self::MultiNodeMultiWriter,
        }
    }
}

/// Volume source, copied from `VolumeContentSource_oneof_type`,
/// because `VolumeContentSource_oneof_type` is not serializable,
/// either source snapshot ID or source volume ID
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum VolumeSource {
    /// Volume source from a snapshot
    Snapshot(String),
    /// Volume source from anther volume
    Volume(String),
}

impl From<VolumeContentSource_oneof_type> for VolumeSource {
    fn from(vcs: VolumeContentSource_oneof_type) -> Self {
        match vcs {
            VolumeContentSource_oneof_type::snapshot(s) => {
                Self::Snapshot(s.get_snapshot_id().to_owned())
            }
            VolumeContentSource_oneof_type::volume(v) => Self::Volume(v.get_volume_id().to_owned()),
        }
    }
}

impl Into<VolumeContentSource_oneof_type> for VolumeSource {
    fn into(self) -> VolumeContentSource_oneof_type {
        match self {
            Self::Snapshot(snap_id) => {
                VolumeContentSource_oneof_type::snapshot(VolumeContentSource_SnapshotSource {
                    snapshot_id: snap_id,
                    ..VolumeContentSource_SnapshotSource::default()
                })
            }
            Self::Volume(vol_id) => {
                VolumeContentSource_oneof_type::volume(VolumeContentSource_VolumeSource {
                    volume_id: vol_id,
                    ..VolumeContentSource_VolumeSource::default()
                })
            }
        }
    }
}

/// `DatenLord` volume
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DatenLordVolume {
    /// Volume name
    pub vol_name: String,
    /// Volume ID
    pub vol_id: String,
    /// Volume size in bytes
    pub size_bytes: i64,
    /// The ID of the node the volume stored at
    pub node_id: String,
    /// The volume diretory path
    pub vol_path: PathBuf,
    /// Volume access mode
    pub vol_access_mode: Vec<VolumeAccessMode>,
    /// The content source of the volume
    pub content_source: Option<VolumeSource>,
    /// The volume is ephemeral or not
    pub ephemeral: bool,
}

/// The basic fields of a volume
struct DatenLordVolumeBasicFields {
    /// Volume name
    pub vol_name: String,
    /// Volume ID
    pub vol_id: String,
    /// Volume size in bytes
    pub size_bytes: i64,
    /// The ID of the node the volume stored at
    pub node_id: String,
    /// The volume diretory path
    pub vol_path: PathBuf,
    /// The volume is ephemeral or not
    pub ephemeral: bool,
}

impl DatenLordVolume {
    /// Create volume helper
    fn new(
        basic_fields: DatenLordVolumeBasicFields,
        // vol_id: String,
        // vol_name: String,
        // vol_size: i64,
        // node_id: String,
        // vol_path: PathBuf,
        vol_access_mode: impl Into<Vec<VolumeCapability_AccessMode_Mode>>,
        content_source: Option<VolumeContentSource_oneof_type>,
        // ephemeral: bool,
    ) -> DatenLordResult<Self> {
        assert!(!basic_fields.vol_id.is_empty(), "volume ID cannot be empty");
        assert!(
            !basic_fields.vol_name.is_empty(),
            "volume name cannot be empty"
        );
        assert!(!basic_fields.node_id.is_empty(), "node ID cannot be empty");
        assert!(
            basic_fields.size_bytes >= 0,
            "invalid volume size: {}",
            basic_fields.size_bytes
        );
        let vol_access_mode_vec = vol_access_mode.into();
        let converted_vol_access_mode_vec = vol_access_mode_vec
            .into_iter()
            .map(VolumeAccessMode::from)
            .collect::<Vec<_>>();
        let vol_source = content_source.map(VolumeSource::from);
        let vol = Self {
            vol_id: basic_fields.vol_id,
            vol_name: basic_fields.vol_name,
            size_bytes: basic_fields.size_bytes,
            node_id: basic_fields.node_id,
            vol_path: basic_fields.vol_path,
            vol_access_mode: converted_vol_access_mode_vec,
            content_source: vol_source,
            ephemeral: basic_fields.ephemeral,
        };

        if basic_fields.ephemeral {
            debug_assert!(
                vol.content_source.is_none(),
                "ephemeral volume cannot have content source",
            );
        }

        vol.create_vol_dir()?;
        Ok(vol)
    }

    /// Create ephemeral volume
    pub fn build_ephemeral_volume(
        vol_id: &str,
        vol_name: &str,
        node_id: &str,
        vol_path: &Path,
    ) -> DatenLordResult<Self> {
        Self::new(
            DatenLordVolumeBasicFields {
                vol_id: vol_id.to_owned(),
                vol_name: vol_name.to_owned(),
                size_bytes: util::EPHEMERAL_VOLUME_STORAGE_CAPACITY,
                node_id: node_id.to_owned(),
                vol_path: vol_path.to_owned(),
                ephemeral: true, // ephemeral
            },
            [VolumeCapability_AccessMode_Mode::SINGLE_NODE_WRITER],
            None, // content source
        )
    }

    /// Create volume from `CreateVolumeRequest`
    pub fn build_from_create_volume_req(
        req: &CreateVolumeRequest,
        vol_id: &str,
        node_id: &str,
        vol_path: &Path,
    ) -> DatenLordResult<Self> {
        Self::new(
            DatenLordVolumeBasicFields {
                vol_id: vol_id.to_owned(),
                vol_name: req.get_name().to_owned(),
                size_bytes: req.get_capacity_range().get_required_bytes(),
                node_id: node_id.to_owned(),
                vol_path: vol_path.to_owned(),
                ephemeral: false,
            },
            req.get_volume_capabilities()
                .iter()
                .map(|vc| vc.get_access_mode().get_mode())
                .collect::<Vec<_>>(),
            if req.has_volume_content_source() {
                req.get_volume_content_source().field_type.clone()
            } else {
                None
            },
        )
    }

    /// Create volume directory
    fn create_vol_dir(&self) -> DatenLordResult<()> {
        fs::create_dir_all(&self.vol_path).with_context(|| {
            format!(
                "failed to create directory={:?} for volume ID={} and name={}",
                self.vol_path, self.vol_id, self.vol_name,
            )
        })?;
        Ok(())
    }

    /// Delete volume directory
    pub fn delete_directory(&self) -> DatenLordResult<()> {
        std::fs::remove_dir_all(&self.vol_path).with_context(|| {
            format!("failed to remove the volume directory: {:?}", self.vol_path)
        })?;
        Ok(())
    }

    /// Get volume size
    pub const fn get_size(&self) -> i64 {
        // TODO: use more relaxed ordering
        self.size_bytes
    }

    /// Expand volume size
    pub fn expand_size(&mut self, new_size: i64) -> bool {
        if new_size > self.get_size() {
            self.size_bytes = new_size;
            true
        } else {
            false
        }
    }
}

/// Snapshot
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DatenLordSnapshot {
    /// Snapshot name
    pub snap_name: String,
    /// Snapshto ID
    pub snap_id: String,
    /// The source volume ID of the snapshot
    pub vol_id: String,
    /// The ID of the node the snapshot stored at
    pub node_id: String,
    /// Snapshot path
    pub snap_path: PathBuf,
    /// Snapshot creation time
    pub creation_time: std::time::SystemTime,
    /// Snapshot size in bytes
    pub size_bytes: i64,
    /// The snapshot is ready or not
    pub ready_to_use: bool,
}

impl DatenLordSnapshot {
    /// Create `DatenLordSnapshot`
    pub fn new(
        snap_name: String,
        snap_id: String,
        vol_id: String,
        node_id: String,
        snap_path: PathBuf,
        creation_time: std::time::SystemTime,
        size_bytes: i64,
    ) -> Self {
        assert!(!snap_id.is_empty(), "snapshot ID cannot be empty");
        assert!(!snap_name.is_empty(), "snapshot name cannot be empty");
        assert!(!vol_id.is_empty(), "source volume ID cannot be empty");
        assert!(!node_id.is_empty(), "node ID cannot be empty");
        assert!(size_bytes >= 0, "invalid snapshot size: {}", size_bytes);
        Self {
            snap_name,
            snap_id,
            vol_id,
            node_id,
            snap_path,
            creation_time,
            size_bytes,
            ready_to_use: true, // TODO: check whether the snapshot is ready to use or not
        }
    }

    /// Delete snapshot file
    pub fn delete_file(&self) -> DatenLordResult<()> {
        nix::unistd::unlink(&self.snap_path)
            .with_context(|| format!("failed to unlink snapshot file: {:?}", self.snap_path,))?;
        Ok(())
    }
}
