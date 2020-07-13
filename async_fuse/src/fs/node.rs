use anyhow::{self, Context};
use log::debug;
use nix::fcntl::{self, FcntlArg, OFlag};
use nix::sys::stat::SFlag;
use nix::sys::stat::{self, Mode};
use nix::unistd;
use smol::blocking;
use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::os::unix::{ffi::OsStrExt, io::RawFd};
use std::path::Path;
use std::sync::atomic::{self, AtomicI64};
use std::time::SystemTime;

use super::super::protocol::*;
use super::dir::*;
use super::util::{self, FileAttr};

#[derive(Debug)]
enum NodeData {
    DirData(BTreeMap<OsString, DirEntry>),
    FileData(Vec<u8>),
}

#[derive(Debug)]
pub(crate) struct Node {
    parent: u64,
    name: OsString,
    attr: FileAttr,
    data: NodeData,
    fd: RawFd,
    open_count: AtomicI64,
    lookup_count: AtomicI64,
}

impl Drop for Node {
    fn drop(&mut self) {
        // TODO: check unsaved data in cache
        unistd::close(self.fd).unwrap_or_else(|_| {
            panic!(
                "DirNode::drop() failed to clode the file handler \
                of the node name={:?} ino={}",
                self.name, self.attr.ino
            )
        });
    }
}

impl Node {
    #[inline]
    pub fn get_ino(&self) -> INum {
        self.get_attr().ino
    }

    pub fn get_fd(&self) -> RawFd {
        self.fd
    }

    pub fn get_parent_ino(&self) -> INum {
        self.parent
    }

    #[allow(dead_code)]
    fn set_parent_ino(&mut self, parent: u64) -> INum {
        let old_parent = self.parent;
        self.parent = parent;
        old_parent
    }

    pub fn get_name(&self) -> &OsStr {
        self.name.as_os_str()
    }

    #[allow(dead_code)]
    fn set_name(&mut self, name: OsString) {
        self.name = name;
    }

    pub fn get_type(&self) -> SFlag {
        match &self.data {
            NodeData::DirData(..) => SFlag::S_IFDIR,
            NodeData::FileData(..) => SFlag::S_IFREG,
        }
    }

    pub fn get_attr(&self) -> FileAttr {
        self.attr
    }
    pub fn set_attr(&mut self, new_attr: FileAttr) -> FileAttr {
        let old_attr = self.get_attr();
        match &self.data {
            NodeData::DirData(..) => debug_assert_eq!(new_attr.kind, SFlag::S_IFDIR),
            NodeData::FileData(..) => debug_assert_eq!(new_attr.kind, SFlag::S_IFREG),
        }
        self.attr = new_attr;
        old_attr
    }

    pub fn lookup_attr(&self) -> FileAttr {
        let attr = self.get_attr();
        self.inc_lookup_count();
        attr
    }

    pub fn get_open_count(&self) -> i64 {
        self.open_count.load(atomic::Ordering::Relaxed)
    }
    fn inc_open_count(&self) -> i64 {
        self.open_count.fetch_add(1, atomic::Ordering::Relaxed)
    }
    pub fn dec_open_count(&self) -> i64 {
        self.open_count.fetch_sub(1, atomic::Ordering::Relaxed)
    }

    pub fn get_lookup_count(&self) -> i64 {
        self.lookup_count.load(atomic::Ordering::Relaxed)
    }
    fn inc_lookup_count(&self) -> i64 {
        self.lookup_count.fetch_add(1, atomic::Ordering::Relaxed)
    }
    pub fn dec_lookup_count_by(&self, nlookup: u64) -> i64 {
        debug_assert!(nlookup < std::i64::MAX as u64);
        self.lookup_count
            .fetch_sub(nlookup as i64, atomic::Ordering::Relaxed)
    }

    #[allow(dead_code)]
    async fn load_attribute(&self) -> anyhow::Result<FileAttr> {
        let attr = util::load_attr(self.fd).await.context(format!(
            "load_attribute() failed to get the attribute of the node ino={}",
            self.get_ino(),
        ))?;
        match &self.data {
            NodeData::DirData(..) => debug_assert_eq!(SFlag::S_IFDIR, attr.kind),
            NodeData::FileData(..) => debug_assert_eq!(SFlag::S_IFREG, attr.kind),
        };
        Ok(attr)
    }

    pub async fn dup_fd(&self, oflags: OFlag) -> anyhow::Result<RawFd> {
        let raw_fd = self.fd;
        let ino = self.get_ino();
        let new_fd = blocking!(unistd::dup(raw_fd)).context(format!(
            "dup_fd() failed to duplicate the handler ino={} raw fd={:?}",
            ino, raw_fd,
        ))?;
        // increase open count once dup() success
        self.inc_open_count();

        let fcntl_oflags = FcntlArg::F_SETFL(oflags);
        blocking!(fcntl::fcntl(new_fd, fcntl_oflags)).context(format!(
            "dup_fd() failed to set the flags={:?} of duplicated handler of ino={}",
            oflags, ino,
        ))?;
        // blocking!(unistd::dup3(raw_fd, new_fd, oflags)).context(format!(
        //     "dup_fd() failed to set the flags={:?} of duplicated handler of ino={}",
        //     oflags, ino,
        // ))?;
        Ok(new_fd)
    }

    pub fn is_node_data_empty(&self) -> bool {
        match &self.data {
            NodeData::DirData(dir_node) => dir_node.is_empty(),
            NodeData::FileData(file_node) => file_node.is_empty(),
        }
    }

    pub fn need_load_file_data(&self) -> bool {
        if !self.is_node_data_empty() {
            debug!(
                "need_load_file_data() found node data of name={:?} \
                    and ino={} is in cache, no need to load",
                self.get_name(),
                self.get_ino(),
            );
            false
        } else if self.get_attr().size > 0 {
            debug!(
                "need_load_file_data() found node size of name={:?} \
                    and ino={} is non-zero, need to load",
                self.get_name(),
                self.get_ino(),
            );
            true
        } else {
            debug!(
                "need_load_file_data() found node size of name={:?} \
                    and ino={} is zero, no need to load",
                self.get_name(),
                self.get_ino(),
            );
            false
        }
    }

    // Directory only methods

    pub fn get_entry(&self, name: &OsStr) -> Option<&DirEntry> {
        match &self.data {
            NodeData::DirData(dir_data) => match dir_data.get(name) {
                Some(dir_entry) => Some(dir_entry),
                None => None,
            },
            NodeData::FileData(..) => panic!("forbidden to get entry from FileData"),
        }
    }

    async fn open_child_dir_helper(
        &mut self,
        child_dir_name: OsString,
        mode: Mode,
        create_dir: bool,
    ) -> anyhow::Result<Node> {
        let ino = self.get_ino();
        let fd = self.fd;
        let dir_data = match &mut self.data {
            NodeData::DirData(dir_data) => dir_data,
            NodeData::FileData(..) => panic!("forbidden to load DirData from file node"),
        };

        if create_dir {
            debug_assert!(
                !dir_data.contains_key(&child_dir_name),
                "open_child_dir_helper() cannot create duplicated directory name={:?}",
                child_dir_name
            );
            let child_dir_name_clone = child_dir_name.clone();
            blocking!(stat::mkdirat(fd, child_dir_name_clone.as_os_str(), mode)).context(
                format!(
                    "open_child_dir_helper() failed to create directory \
                        name={:?} under parent ino={}",
                    child_dir_name, ino,
                ),
            )?;
        }

        let child_dir_name_clone = child_dir_name.clone();
        let child_raw_fd = util::open_dir_at(fd, child_dir_name_clone)
            .await
            .context(format!(
                "open_child_dir_helper() failed to open the new directory name={:?} \
                    under parent ino={}",
                child_dir_name, ino,
            ))?;

        // get new directory attribute
        let child_attr = util::load_attr(child_raw_fd).await.context(
            "open_child_dir_helper() failed to get the attribute of the new child directory"
                .to_string(),
        )?;
        debug_assert_eq!(SFlag::S_IFDIR, child_attr.kind);

        if create_dir {
            // insert new entry to parent directory
            // TODO: support thread-safe
            let previous_value = dir_data.insert(
                child_dir_name.clone(),
                DirEntry::new(child_attr.ino, child_dir_name.clone(), SFlag::S_IFDIR),
            );
            debug_assert!(previous_value.is_none()); // double check creation race
        }

        // lookup count and open count are increased to 1 by creation
        let mut child_node = Node {
            parent: self.get_ino(),
            name: child_dir_name,
            attr: child_attr,
            data: NodeData::DirData(BTreeMap::new()),
            fd: child_raw_fd,
            open_count: AtomicI64::new(1),
            lookup_count: AtomicI64::new(1),
        };

        if !create_dir {
            // load directory data on open
            child_node.load_data().await?;
        }

        Ok(child_node)
    }

    pub async fn open_child_dir(&mut self, child_dir_name: OsString) -> anyhow::Result<Node> {
        self.open_child_dir_helper(child_dir_name, Mode::empty(), false)
            .await
    }

    pub async fn create_child_dir(
        &mut self,
        child_dir_name: OsString,
        mode: Mode,
    ) -> anyhow::Result<Node> {
        self.open_child_dir_helper(child_dir_name, mode, true).await
    }

    async fn open_child_file_helper(
        &mut self,
        child_file_name: OsString,
        oflags: OFlag,
        mode: Mode,
        create_file: bool,
    ) -> anyhow::Result<Node> {
        let ino = self.get_ino();
        let fd = self.fd;
        let dir_data = match &mut self.data {
            NodeData::DirData(dir_data) => dir_data,
            NodeData::FileData(..) => panic!("forbidden to load DirData from file node"),
        };

        if create_file {
            debug_assert!(
                !dir_data.contains_key(&child_file_name),
                "open_child_file_helper() cannot create duplicated file name={:?}",
                child_file_name
            );
            debug_assert!(oflags.contains(OFlag::O_CREAT));
        }
        let child_file_name_clone = child_file_name.clone();
        let child_fd = blocking!(fcntl::openat(
            fd,
            child_file_name_clone.as_os_str(),
            oflags,
            mode
        ))
        .context(format!(
            "open_child_file_helper() failed to open a file name={:?} \
                    under parent ino={} with oflags={:?} and mode={:?}",
            child_file_name, ino, oflags, mode,
        ))?;

        // get new file attribute
        let child_attr = util::load_attr(child_fd).await.context(
            "open_child_file_helper() failed to get the attribute of the new child".to_string(),
        )?;
        debug_assert_eq!(SFlag::S_IFREG, child_attr.kind);

        if create_file {
            // insert new entry to parent directory
            // TODO: support thread-safe
            let previous_value = dir_data.insert(
                child_file_name.clone(),
                DirEntry::new(child_attr.ino, child_file_name.clone(), SFlag::S_IFREG),
            );
            debug_assert!(previous_value.is_none()); // double check creation race
        }

        // lookup count and open count are increased to 1 by creation
        Ok(Node {
            parent: self.get_ino(),
            name: child_file_name,
            attr: child_attr,
            data: NodeData::FileData(Vec::new()),
            fd: child_fd,
            open_count: AtomicI64::new(1),
            lookup_count: AtomicI64::new(1),
        })
    }

    pub async fn open_child_file(
        &mut self,
        child_file_name: OsString,
        oflags: OFlag,
    ) -> anyhow::Result<Node> {
        self.open_child_file_helper(child_file_name, oflags, Mode::empty(), false)
            .await
    }

    pub async fn create_child_file(
        &mut self,
        child_file_name: OsString,
        oflags: OFlag,
        mode: Mode,
    ) -> anyhow::Result<Node> {
        self.open_child_file_helper(child_file_name, oflags, mode, true)
            .await
    }

    // TODO: to remove
    async fn load_dir_data_helper(&self) -> nix::Result<BTreeMap<OsString, DirEntry>> {
        let fd = self.fd;
        let dir = blocking!(Dir::from_fd(fd))?;

        let dir_entry_map = blocking!(
            let dir_entry_map: BTreeMap<OsString, DirEntry> = dir
                .filter(|e| e.is_ok()) // filter out error result
                .map(|e| e.unwrap()) // safe to use unwrap() here
                .filter(|e| {
                    let bytes = e.entry_name().as_bytes();
                    !bytes.starts_with(&[b'.']) // skip hidden entries, '.' and '..'
                })
                .filter(|e| match e.entry_type() {
                    SFlag::S_IFDIR => true,
                    SFlag::S_IFREG => true,
                    _ => false,
                })
                .map(|e| (e.entry_name().into(), e))
                .collect();
            dir_entry_map
        );
        Ok(dir_entry_map)
    }

    // TODO: to remove
    async fn load_file_data_helper(&self) -> anyhow::Result<Vec<u8>> {
        let ino = self.get_ino();
        let fd = self.fd;
        let file_size = self.attr.size;
        // TODO: load file data to cache
        let file_data_vec = blocking!(
            let mut file_data_vec: Vec<u8> = Vec::with_capacity(file_size as usize);
            unsafe {
                file_data_vec.set_len(file_data_vec.capacity());
            }
            let read_size = unistd::read(fd, &mut *file_data_vec).context(format!(
                "load_file_data_helper() failed to \
                    read the file of ino={} from disk",
                ino,
            ))?;
            unsafe {
                file_data_vec.set_len(read_size as usize);
            }
            // TODO: should explicitly highlight the error type?
            Ok::<Vec<u8>, anyhow::Error>(file_data_vec)
        )?;
        debug_assert_eq!(file_data_vec.len(), file_size as usize);
        Ok(file_data_vec)
    }

    pub async fn load_data(&mut self) -> anyhow::Result<usize> {
        match &mut self.data {
            NodeData::DirData(..) => {
                let dir_entry_map = self.load_dir_data_helper().await?;
                let entry_count = dir_entry_map.len();
                debug!("load_data() successfully load {} entries", entry_count,);
                self.data = NodeData::DirData(dir_entry_map);
                Ok(entry_count)
            }
            NodeData::FileData(..) => {
                let file_data_vec = self.load_file_data_helper().await?;
                let read_size = file_data_vec.len();
                debug!("load_data() successfully load {} byte data", read_size,);
                self.data = NodeData::FileData(file_data_vec);
                Ok(read_size)
            }
        }
    }

    #[allow(dead_code)]
    fn insert_entry(&mut self, child_entry: DirEntry) -> Option<DirEntry> {
        let dir_data = match &mut self.data {
            NodeData::DirData(dir_data) => dir_data,
            NodeData::FileData(..) => panic!("forbidden to load DirData from file node"),
        };

        let previous_entry = dir_data.insert(child_entry.entry_name().into(), child_entry);
        debug!(
            "insert_entry() successfully inserted new entry \
                and replaced previous entry={:?}",
            previous_entry,
        );

        previous_entry
    }

    // fn remove_entry(&mut self, child_name: OsString) -> Option<DirEntry> {
    //     let dir_data = match &mut self.data {
    //         NodeData::DirData(dir_data) => dir_data,
    //         NodeData::FileData(..) => panic!("forbidden to load DirData from file node"),
    //     };

    //     dir_data.remove(child_name.as_os_str())
    // }

    pub async fn unlink_entry(&mut self, child_name: OsString) -> anyhow::Result<DirEntry> {
        let dir_data = match &mut self.data {
            NodeData::DirData(dir_data) => dir_data,
            NodeData::FileData(..) => panic!("forbidden to load DirData from file node"),
        };

        let removed_entry = dir_data.remove(child_name.as_os_str());
        debug_assert!(
            removed_entry.is_some(),
            "unlink_entry() found fs is inconsistent, the entry of name={:?} \
                is not in directory of name={:?} and ino={}",
            child_name,
            self.get_name(),
            self.get_ino(),
        );
        let removed_entry = removed_entry.unwrap(); // safe to use unwrap() here
        let child_name_clone = child_name.clone();
        let fd = self.fd;
        // delete from disk and close the handler
        match removed_entry.entry_type() {
            SFlag::S_IFDIR => {
                blocking!(unistd::unlinkat(
                    Some(fd),
                    child_name.as_os_str(),
                    unistd::UnlinkatFlags::RemoveDir,
                ))
                .context(format!(
                    "unlink_entry() failed to delete the file name={:?} from disk",
                    child_name_clone,
                ))?;
            }
            SFlag::S_IFREG => {
                blocking!(unistd::unlinkat(
                    Some(fd),
                    child_name.as_os_str(),
                    unistd::UnlinkatFlags::NoRemoveDir,
                ))
                .context(format!(
                    "unlink_entry() failed to delete the file name={:?} from disk",
                    child_name_clone,
                ))?;
            }
            _ => panic!(
                "unlink_entry() found unsupported entry type={:?}",
                removed_entry.entry_type()
            ),
        }

        Ok(removed_entry)
    }

    pub fn read_dir(&self, func: impl FnOnce(&BTreeMap<OsString, DirEntry>) -> usize) -> usize {
        // debug_assert!(
        //     !self.need_load_file_data(),
        //     "directory data should be load before read",
        // );
        let dir_data = match &self.data {
            NodeData::DirData(dir_data) => dir_data,
            NodeData::FileData(..) => panic!("forbidden to load DirData from file node"),
        };

        func(&dir_data)
    }

    // File only methods

    // TODO: maybe this function is not needed, consider refactory
    pub fn read_file(
        &self,
        func: impl FnOnce(&Vec<u8>) -> anyhow::Result<Vec<u8>>,
    ) -> anyhow::Result<Vec<u8>> {
        debug_assert!(
            !self.need_load_file_data(),
            "file data should be load before read".to_string(),
        );
        let file_data = match &self.data {
            NodeData::DirData(..) => panic!("forbidden to load FileData from dir node"),
            NodeData::FileData(file_data) => file_data,
        };

        func(&file_data)
    }

    pub async fn write_file(
        &mut self,
        fh: u64,
        offset: i64,
        data: Vec<u8>,
        oflags: OFlag,
        write_to_disk: bool,
    ) -> anyhow::Result<usize> {
        let ino = self.get_ino();
        let file_data_vec = match &mut self.data {
            NodeData::DirData(..) => panic!("forbidden to load FileData from dir node"),
            NodeData::FileData(file_data) => file_data,
        };

        let size_after_write = offset as usize + data.len();
        if file_data_vec.capacity() < size_after_write {
            let before_cap = file_data_vec.capacity();
            let extra_space_size = size_after_write - file_data_vec.capacity();
            file_data_vec.reserve(extra_space_size);
            // TODO: handle OOM when reserving
            // let result = file_data.try_reserve(extra_space_size);
            // if result.is_err() {
            //     warn!(
            //         "write_file() cannot reserve enough space, \
            //            the write space needed {} bytes",
            //         extra_space_size);
            //     reply.error(ENOMEM);
            //     return;
            // }
            debug!(
                "write_file() enlarged the file data vector capacity from {} to {}",
                before_cap,
                file_data_vec.capacity(),
            );
        }
        match file_data_vec.len().cmp(&(offset as usize)) {
            std::cmp::Ordering::Greater => {
                file_data_vec.truncate(offset as usize);
                debug!(
                    "write_file() truncated the file of ino={} to size={}",
                    ino, offset
                );
            }
            std::cmp::Ordering::Less => {
                let zero_padding_size = (offset as usize) - file_data_vec.len();
                let mut zero_padding_vec = vec![0u8; zero_padding_size];
                file_data_vec.append(&mut zero_padding_vec);
            }
            std::cmp::Ordering::Equal => (),
        }
        // TODO: consider zero copy
        file_data_vec.extend_from_slice(&data);

        let fcntl_oflags = fcntl::FcntlArg::F_SETFL(oflags);
        let fd = fh as RawFd;
        fcntl::fcntl(fd, fcntl_oflags).context(format!(
            "write_file() failed to set the flags={:?} to file handler={} of ino={}",
            oflags, fd, ino,
        ))?;
        let mut written_size = data.len();
        if write_to_disk {
            let data_len = data.len();
            written_size = blocking!(nix::sys::uio::pwrite(fd, &data, offset))
                .context("write_file() failed to write to disk")?;
            debug_assert_eq!(data_len, written_size);
        }
        // update the attribute of the written file
        self.attr.size = file_data_vec.len() as u64;
        let ts = SystemTime::now();
        self.attr.mtime = ts;

        Ok(written_size)
    }

    pub async fn open_root_node(
        root_ino: INum,
        name: OsString,
        path: impl AsRef<Path>,
    ) -> anyhow::Result<Node> {
        let path = path.as_ref();
        let dir_fd = util::open_dir(path).await?;
        let mut attr = util::load_attr(dir_fd).await?;
        attr.ino = root_ino; // replace root ino with 1

        let mut root_node = Node {
            parent: root_ino,
            name,
            attr,
            data: NodeData::DirData(BTreeMap::new()),
            fd: dir_fd,
            // lookup count set to 1 by creation
            open_count: AtomicI64::new(1),
            // open count set to 1 by creation
            lookup_count: AtomicI64::new(1),
        };
        // load root directory data on open
        root_node.load_data().await?;

        Ok(root_node)
    }

    #[allow(dead_code)]
    fn move_file(
        old_parent_node: &Node,
        old_name: &OsStr,
        new_parent_node: &Node,
        new_name: &OsStr,
    ) -> nix::Result<()> {
        debug!(
            "move_file() about to move file of old name={:?} \
                from directory={:?} to directory={:?} with new name={:?}",
            old_name,
            old_parent_node.get_name(),
            new_parent_node.get_name(),
            new_name,
        );
        fcntl::renameat(
            Some(old_parent_node.get_fd()),
            Path::new(old_name),
            Some(new_parent_node.get_fd()),
            Path::new(new_name),
        )
    }
}

#[cfg(test)]
mod test {
    use anyhow::bail;
    use nix::fcntl::{self, FcntlArg, OFlag};
    use nix::sys::stat::Mode;
    use nix::unistd;
    use std::fs::File;
    use std::io::prelude::*;
    use std::os::unix::io::FromRawFd;
    use std::path::Path;

    #[test]
    fn test_dup_fd() -> anyhow::Result<()> {
        let path = Path::new("/tmp/dup_fd_test.txt");
        let oflags = OFlag::O_CREAT | OFlag::O_TRUNC | OFlag::O_RDWR;
        let res = fcntl::open(path, oflags, Mode::from_bits_truncate(644));
        let fd = match res {
            Ok(fd) => fd,
            Err(e) => bail!("failed to open fiile {:?}, the error is: {}", path, e),
        };
        let res = unistd::unlink(path);
        if let Err(e) = res {
            unistd::close(fd)?;
            bail!("failed to unlink file, the error is: {}", e);
        }

        let new_oflags = OFlag::O_WRONLY | OFlag::O_APPEND;
        let fcntl_oflags = FcntlArg::F_SETFL(new_oflags);
        let res = fcntl::fcntl(fd, fcntl_oflags);
        let dup_fd = match res {
            Ok(fd) => fd,
            Err(e) => {
                unistd::close(fd)?;
                bail!("failed to dup fd, the error is: {}", e);
            }
        };

        let mut file = unsafe { File::from_raw_fd(fd) };
        let mut dup_file = unsafe { File::from_raw_fd(dup_fd) };

        dup_file.write_all(b"ABCDEFGHJKLMNOPQRSTUVWXYZ")?;

        let mut content = String::new();
        file.read_to_string(&mut content)?;
        println!("the content is: {}", content);

        Ok(())
    }
}
