use super::fs_util::FileAttr;
use super::serial::SerialFileAttr;
use super::serial::{self, file_attr_to_serial};
use crate::async_fuse::fuse::file_system::FsAsyncResultSender;
use crate::async_fuse::fuse::protocol::INum;
use crate::async_fuse::memfs::dir::DirEntry;
use crate::async_fuse::memfs::s3_node::S3NodeData;
use crate::async_fuse::memfs::s3_wrapper::S3BackEnd;
use crate::common::error::DatenLordError;
use crate::common::error::DatenLordResult;
use clippy_utilities::OverflowArithmetic;
use log::debug;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::atomic::Ordering::Acquire;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};
use thiserror::Error;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

/// Dir prefix for s3 storage bucket key
const DIR_DATA_KEY_PREFIX: &str = "dir_";
/// Retry times of persisting to s3
const PERSIST_RETRY_TIMES: u32 = 3;

/// Errors of persist operations
#[derive(Error, Debug)]
#[allow(dead_code)]
pub enum PersistError {
    // #[error("data store disconnected")]
    // Disconnect(#[from] io::Error),
    // #[error("the data for key `{0}` is not available")]
    // Redaction(String),
    // #[error("invalid header (expected {expected:?}, found {found:?})")]
    // InvalidHeader {
    //     expected: String,
    //     found: String,
    // },
    /// Root dir should have a specific Option to store its attr, because it doesn't have a parent.
    #[error("missing root's file attr")]
    FileAttrMissingForRoot,

    /// `try_persist_one_dirty` will return error when there's no more data to persist.
    #[error("no more dirty to persist")]
    NoMoreDirtyToPersist,
}

/// Read persist dir content
/// read when child dir cache missed in lookup
///  or `open_root_node` when start.
pub(crate) async fn read_persisted_dir<S: S3BackEnd + Sync + Send + 'static>(
    s3_backend: &Arc<S>,
    dir_full_path: String,
) -> anyhow::Result<PersistDirContent> {
    let res = s3_backend
        .get_data(&format!("{DIR_DATA_KEY_PREFIX}{dir_full_path}"))
        .await?; // return if not exist
    match PersistDirContent::new_from_store(dir_full_path, res.as_slice()) {
        Ok(dir_content) => Ok(dir_content),
        Err(e) => {
            debug!("failed to deserialize dir data, {e}");
            Err(e)
        }
    }
}
/// Serial part of `PersistDirContent`
#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct PersistSerializePart {
    /// The map records child file name to attr
    pub(crate) file_map: HashMap<String, SerialFileAttr>,
    /// Root dir dosnt has a parent, we need to store the attr with the dir
    pub(crate) root_attr: Option<SerialFileAttr>,
}
/// Persist related dir content
#[derive(Debug)]
pub(crate) struct PersistDirContent {
    /// Full path of dir
    pub(crate) dir_full_path: String,
    /// Part to be serialized and persisted
    pub(crate) persist_serialized: PersistSerializePart,
}
impl PersistDirContent {
    /// Root dir without parent need to store attr specificly
    pub(crate) fn try_get_root_attr(&self) -> anyhow::Result<FileAttr> {
        match self.persist_serialized.root_attr.as_ref() {
            Some(attr) => Ok(serial::serial_to_file_attr(attr)),
            None => Err(anyhow::Error::from(PersistError::FileAttrMissingForRoot)),
        }
    }

    /// New `PersistDirContent` from mem cache data
    pub(crate) fn new_from_cache(
        dir_full_path: String,
        files: &BTreeMap<String, DirEntry>,
        f_attr: SerialFileAttr,
    ) -> PersistDirContent {
        let root_attr = (dir_full_path == "/").then_some(f_attr);

        PersistDirContent {
            dir_full_path,
            persist_serialized: PersistSerializePart {
                file_map: files
                    .iter()
                    .map(|(fname, entry)| {
                        (fname.clone(), file_attr_to_serial(&entry.file_attr_ref()))
                    })
                    .collect(),
                root_attr,
            },
        }
    }

    #[allow(dead_code)]
    /// make `S3NodeData::Dir` from persisted data.
    pub(crate) fn new_s3_node_data_dir(&self) -> S3NodeData {
        let dir_map = self
            .persist_serialized
            .file_map
            .iter()
            .map(|(name, fattr)| {
                (
                    name.clone(),
                    DirEntry::new(name.clone(), serial::serial_to_file_attr(fattr)),
                )
            })
            .collect();
        S3NodeData::Directory(dir_map)
    }

    /// deserialize from stored data
    fn new_from_store(
        dir_full_path: String,
        file_map_data: &[u8],
    ) -> anyhow::Result<PersistDirContent> {
        let persist_serialized = bincode::deserialize(file_map_data)?;
        let new = PersistDirContent {
            dir_full_path,
            persist_serialized,
        };
        Ok(new)
    }
    /// Serialize the content
    fn serialize(&self) -> Vec<u8> {
        bincode::serialize(&self.persist_serialized)
            .unwrap_or_else(|e| panic!("fail to serialize `PersistDirContent.file_map`, {e}"))
    }
}

/// Mark the dirty directory. inum - (waiting list, clone of dirty content)
type PersistDirtyMap = HashMap<INum, (VecDeque<Arc<Notify>>, PersistDirContent)>;
/// Shared state between persist task and persist handle hold by fs
#[derive(Debug)]
struct PersistSharedState {
    /// inum - (waiting list, clone of dirty content)
    dirty_map: RwLock<PersistDirtyMap>, // todo: directly do the serail when mark dirty.
    /// Mark for end the persist loop
    system_end: AtomicBool,
}

/// Persist handle for fs to interact with persist task.
#[derive(Debug)]
pub(crate) struct PersistHandle {
    /// Shared state between persist task and persist handle hold by fs
    shared: Arc<PersistSharedState>,
}
impl PersistHandle {
    /// Create `PersistHandle`
    fn new(shared: Arc<PersistSharedState>) -> PersistHandle {
        PersistHandle { shared }
    }
    /// For case of need to sync the persist of a directory.
    #[allow(dead_code)]
    pub(crate) async fn wait_persist(&self, inum: INum) {
        // check if persisted
        // if not, regist notify and wait
        let notify = self.shared.dirty_map.write().get_mut(&inum).map(
            |&mut (ref mut notify_queue, ref mut _dir_content)| {
                let notify = Arc::new(Notify::new());
                notify_queue.push_back(Arc::clone(&notify));
                notify
            },
        );
        if let Some(notify) = notify {
            notify.notified().await;
        }
    }
    /// Mark changed directory with inum and clone current content
    ///  currently only support directory
    #[allow(dead_code)]
    pub(crate) fn mark_dirty(&self, inum: INum, data_clone: PersistDirContent) {
        let mut dirty_map_locked = self.shared.dirty_map.write();
        dirty_map_locked
            .entry(inum)
            .or_insert_with(|| (VecDeque::new(), data_clone));
    }
    /// Stop the persist task loop
    #[inline]
    pub(crate) fn system_end(&self) {
        self.shared.system_end.store(true, Ordering::SeqCst);
    }
}

/// State held by persist task
pub(crate) struct PersistTask<S: S3BackEnd + Sync + Send + 'static> {
    /// Shared state between persist task and persist handle hold by fs
    shared: Arc<PersistSharedState>,
    /// S3 backend for persistence.
    s3_backend: Arc<S>,
}

impl<S: S3BackEnd + Sync + Send + 'static> PersistTask<S> {
    /// Called by persist task loop to take out one and persist to s3 each time.
    #[inline]
    async fn try_persist_one_dirty(
        &self,
        fs_async_result_sender: &FsAsyncResultSender,
    ) -> DatenLordResult<()> {
        let mut next_to_persist = None;
        let mut left_more_dirty = false;
        {
            // takeout one dirty data
            let mut dirty_map_locked = self.shared.dirty_map.write();
            let inum = dirty_map_locked.iter().next().map(|(inum, _)| *inum);
            if let Some(inum) = inum {
                let (notify_list, data) = dirty_map_locked.remove(&inum).unwrap_or_else(|| {
                    panic!("We already knew the key:{inum} exists, it's impossible to panic.")
                });
                next_to_persist = Some((inum, notify_list, data));
                left_more_dirty = !dirty_map_locked.is_empty();
            }
        }
        if let Some((_, waiting_list, dirdata)) = next_to_persist {
            // Do persist

            // Handle backend failure
            //  retry times then send error to main thread
            let dirdata_serial = dirdata.serialize();
            for i in 0..PERSIST_RETRY_TIMES {
                if let Err(e) = self
                    .s3_backend
                    .put_data(
                        &format!("{DIR_DATA_KEY_PREFIX}{}", dirdata.dir_full_path),
                        &dirdata_serial,
                        0,
                        dirdata_serial.len(),
                    )
                    .await
                {
                    debug!("Failed to persist dir data, retried {}/{PERSIST_RETRY_TIMES} times, {e:#?}",i.overflow_add(1));
                    if i == PERSIST_RETRY_TIMES.overflow_sub(1) {
                        // send err to main thread
                        fs_async_result_sender
                            .send(Err(DatenLordError::Unimplemented { context: vec![] }))
                            .await
                            .unwrap_or_else(|e| {
                                debug!("Failed to send error to main thread,{e}");
                            });
                    }
                } else {
                    debug!(
                        "Success to persist latest dir content for {}",
                        dirdata.dir_full_path
                    );
                    break;
                }
            }
            // Notify waiting operations
            for notify in &waiting_list {
                notify.notify_one();
            }
        }
        if left_more_dirty {
            Ok(())
        } else {
            Err(DatenLordError::Unimplemented {
                context: Vec::new(),
            })
        }
    }
    /// Spawn the async persist task.
    pub(crate) fn spawn(
        s3_backend: Arc<S>,
        fs_async_result_sender: FsAsyncResultSender,
    ) -> (PersistHandle, JoinHandle<()>) {
        let shared = Arc::new(PersistSharedState {
            dirty_map: RwLock::default(),
            system_end: AtomicBool::new(false),
        });
        (
            PersistHandle::new(Arc::clone(&shared)),
            tokio::spawn(async {
                let fs_async_result_sender = fs_async_result_sender;
                let taskstate = PersistTask { shared, s3_backend };
                //lazy scan for dirty data
                loop {
                    // persist dirty until there's no dirty data
                    while taskstate
                        .try_persist_one_dirty(&fs_async_result_sender)
                        .await
                        .is_ok()
                    {}
                    if taskstate.shared.system_end.load(Acquire) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    if taskstate.shared.system_end.load(Acquire) {
                        break;
                    }
                }
                debug!("persist task end");
            }),
        )
    }
}

#[allow(clippy::unwrap_used, clippy::match_wild_err_arm)]
#[cfg(test)]
mod test {
    use rand::distributions::Alphanumeric;
    use rand::prelude::Distribution;

    // use super::PersistDirContent;
    // use super::PersistSerializePart;
    use crate::async_fuse::fuse::file_system;
    use crate::async_fuse::memfs::fs_util::FileAttr;
    use crate::async_fuse::memfs::persist::{
        self, PersistDirContent, PersistSerializePart, PersistTask,
    };
    use crate::async_fuse::memfs::s3_wrapper::{MockS3BackEnd, S3Error};
    use crate::async_fuse::memfs::serial::file_attr_to_serial;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::UNIX_EPOCH;
    use std::time::{Duration, SystemTime};
    // use std::rc::Rc;
    use parking_lot::RwLock;

    fn create_mock() -> Arc<MockS3BackEnd> {
        let mut mock = MockS3BackEnd::new();

        let map = Arc::new(RwLock::new(HashMap::new()));
        let map_clone = Arc::clone(&map);
        mock.expect_put_data()
            .withf(move |file, data, offset, len| {
                map_clone
                    .write()
                    .insert(file.to_owned(), (data.to_vec(), *offset, *len));
                true
            })
            .returning(|_, _, _, _| Ok(()));

        let map_clone = Arc::clone(&map);
        mock.expect_get_data().returning(move |f| {
            map_clone.read().get(f).map_or_else(
                || Err(S3Error::S3InternalError("data not existed".to_owned())),
                |e| Ok(e.0.clone()),
            )
        });
        Arc::new(mock)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_mock_s3() {
        let _m = create_mock();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stop_task() {
        let mock = create_mock();

        let (tx, _rx) = file_system::new_fs_async_result_chan();
        let (handle, join) = PersistTask::spawn(mock, tx);
        let begin = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(10)).await;
            handle.system_end();
        });
        join.await.unwrap();
        let end = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let dur = (end - begin).as_millis();
        assert!(dur > 10000);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_persist() {
        let mock = create_mock();
        let mock_clone = Arc::clone(&mock);

        let (tx, _rx) = file_system::new_fs_async_result_chan();
        let (handle, join) = PersistTask::spawn(mock_clone, tx);

        for _ in 0_i32..100_i32 {
            let mut rng = rand::thread_rng();
            let s: String = Alphanumeric
                .sample_iter(&mut rng)
                .take(7)
                .map(char::from)
                .collect::<String>()
                .to_uppercase();
            println!("rand path {s}");
            let path = format!("/{s}/");
            let file_map = {
                let mut map = HashMap::new();
                let s: String = Alphanumeric
                    .sample_iter(&mut rng)
                    .take(7)
                    .map(char::from)
                    .collect::<String>()
                    .to_uppercase();
                map.insert(s, file_attr_to_serial(&FileAttr::now()));
                map
            };
            let begin = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
            handle.mark_dirty(
                0,
                PersistDirContent {
                    dir_full_path: path.clone(),
                    persist_serialized: PersistSerializePart {
                        file_map: file_map.clone(),
                        root_attr: None,
                    },
                },
            );
            handle.wait_persist(0).await;
            let end = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
            let duration = (end - begin).as_millis();
            // smaller than persist cycle
            assert!(duration < 110);
            // should get the dir
            match persist::read_persisted_dir(&mock, path).await {
                Ok(dir) => {
                    assert_eq!(
                        dir.persist_serialized.file_map.iter().next().unwrap().0,
                        file_map.iter().next().unwrap().0
                    );
                }
                Err(_) => {
                    panic!("dir should be persisted and exists")
                }
            }
        }

        handle.system_end();
        join.await.unwrap();
    }
}
