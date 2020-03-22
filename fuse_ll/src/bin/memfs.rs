use fuse_ll::fuse::{
    self, FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyWrite, Request,
};
use libc::{EEXIST, EINVAL, EISDIR, ENODATA, ENOENT, ENOTDIR, ENOTEMPTY};
use log::debug; // error, info, warn
use nix::sys::stat::{Mode, SFlag};
use std::cmp;
use std::collections::{btree_map::Entry, BTreeMap};
use std::env;
use std::ffi::OsStr;
use std::path::PathBuf;
use std::sync::atomic::{self, AtomicU64};
use std::sync::RwLock;
use std::time::{Duration, SystemTime};

const MY_UID: u32 = 502;
const MY_GID: u32 = 20;
const MY_TTL_SEC: u64 = 1;
const MY_GENERATION: u64 = 1;
const MY_DIR_MODE: u16 = 0o755;
// const MY_FILE_MODE: u16 = 0o644;
const STARTING_INODE: u64 = 1;

#[derive(Debug)]
enum FileData {
    Directory(BTreeMap<PathBuf, u64>),
    File(Vec<u8>),
}

#[derive(Debug)]
struct Node {
    parent: u64,
    name: PathBuf,
    attr: FileAttr,
    data: FileData,
}

#[derive(Debug)]
struct MemoryFilesystem {
    tree: RwLock<BTreeMap<u64, Node>>,
    max_ino: AtomicU64,
}

impl<'a> MemoryFilesystem {
    fn new() -> MemoryFilesystem {
        let max_ino = AtomicU64::new(STARTING_INODE);
        let root_ino = max_ino.fetch_add(1, atomic::Ordering::SeqCst);
        let ts = SystemTime::now();

        let attr = FileAttr {
            ino: root_ino,
            size: 0,
            blocks: 0,
            atime: ts,
            mtime: ts,
            ctime: ts,
            crtime: ts,
            kind: FileType::Directory,
            perm: MY_DIR_MODE,
            nlink: 2,
            uid: MY_UID,
            gid: MY_GID,
            rdev: 0,  // TODO: what's rdev?
            flags: 0, // TODO: what's flags?
        };

        let mut root_data = BTreeMap::new();
        root_data.insert(PathBuf::from("."), root_ino);
        // root has no parent
        root_data.insert(PathBuf::from(".."), root_ino);
        let root_data = FileData::Directory(root_data);

        let root = Node {
            name: PathBuf::from("/"),
            data: root_data,
            attr,
            parent: root_ino, // root has no parent
        };
        let mut tree = BTreeMap::new();
        tree.insert(root_ino, root);
        MemoryFilesystem {
            tree: RwLock::new(tree),
            max_ino,
        }
    }
}

impl Filesystem for MemoryFilesystem {
    fn getattr(&mut self, req: &Request, ino: u64, reply: ReplyAttr) {
        debug!("getattr(ino={}, req={:?})", ino, req.request);

        let tree = &self
            .tree
            .read()
            .expect("getattr cannot get the read lock of fs");
        match tree.get(&ino) {
            Some(node) => {
                let ttl = Duration::new(MY_TTL_SEC, 0);
                reply.attr(&ttl, &node.attr);
                debug!(
                    "getattr successfully got the attribute of
                     the file name={:?} of ino={}, the attr is: {:?}",
                    &node.name, ino, &node.attr,
                );
            }
            None => {
                reply.error(ENOENT);
                panic!("getattr failed to find the i-node of ino={}", ino);
            }
        };
    }

    fn read(&mut self, req: &Request, ino: u64, fh: u64, offset: i64, size: u32, reply: ReplyData) {
        debug!(
            "read(ino={}, fh={}, offset={}, size={}, req={:?})",
            ino, fh, offset, size, req.request,
        );

        let tree = &self
            .tree
            .read()
            .expect("read cannot get the read lock of fs");
        match tree.get(&ino) {
            Some(node) => match &node.data {
                FileData::File(content) => {
                    if (offset as usize) < content.len() {
                        let read_data = if ((offset + size as i64) as usize) < content.len() {
                            &content[(offset as usize)..(offset + size as i64) as usize]
                        } else {
                            &content[(offset as usize)..]
                        };
                        debug!(
                            "read successfully from the file of ino={}, the read size is: {:?}",
                            ino,
                            read_data.len(),
                        );
                        reply.data(read_data);
                    } else {
                        debug!(
                            "read offset={} is beyond the length of the file of ino={}",
                            offset, ino
                        );
                        reply.error(EINVAL);
                    }
                }
                FileData::Directory(_) => {
                    reply.error(EISDIR);
                    panic!(
                        "read found fs is inconsistent,
                         the node type of ino={} should be a file not a directory",
                        ino,
                    );
                }
            },
            None => {
                debug!("read failed to find the i-node of the file of ino={}", ino);
                reply.error(ENOENT);
            }
        }
    }

    fn readdir(
        &mut self,
        req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        debug!(
            "readdir(ino={}, fh={}, offset={}, req={:?})",
            ino, fh, offset, req.request,
        );
        let tree = &self
            .tree
            .read()
            .expect("readdir cannot get the read lock of fs");
        match tree.get(&ino) {
            Some(dir_node) => {
                match &dir_node.data {
                    FileData::Directory(dir_data) => {
                        let mut num_child_entries = 0;
                        for (i, (child_name, child_inode)) in
                            dir_data.iter().enumerate().skip(offset as usize)
                        {
                            let child_node = tree.get(child_inode).expect(&format!(
                                "readdir found fs is inconsistent, the file name={:?} of ino={}
                                 found under the parent of ino={}, but no i-node found for this file",
                                child_name, child_inode, ino,
                            ));
                            reply.add(
                                child_inode.clone(),
                                offset + i as i64 + 1, // i + 1 means the index of the next entry
                                child_node.attr.kind,
                                child_name,
                            );
                            num_child_entries += 1;
                            debug!(
                                "readdir found one child name={:?} ino={} offset={} attr={:?}
                                 under the directory of ino={}",
                                child_name,
                                child_inode,
                                offset + i as i64 + 1,
                                child_node.attr,
                                ino,
                            );
                        }
                        debug!(
                            "readdir successfully read {} children under the directory of ino={},
                             the reply is: {:?}",
                            num_child_entries, ino, &reply,
                        );
                        reply.ok();
                    }
                    FileData::File(_) => {
                        reply.error(ENOTDIR);
                        panic!(
                            "readdir found fs is inconsistent,
                             the node type of ino={} should be a directory not a file",
                            ino,
                        );
                    }
                }
            }
            None => {
                debug!("readdir failed to find the directory i-node of ino={}", ino);
                reply.error(ENOENT);
            }
        }
    }

    fn lookup(&mut self, req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let file_name = PathBuf::from(name);
        debug!(
            "lookup(parent={}, name={:?}, req={:?})",
            parent, file_name, req.request,
        );

        let tree = &self
            .tree
            .read()
            .expect("lookup cannot get the read lock of fs");
        match tree.get(&parent) {
            Some(parent_node) => match &parent_node.data {
                FileData::Directory(parent_data) => match parent_data.get(&file_name) {
                    Some(ino) => {
                        let ttl = Duration::new(MY_TTL_SEC, 0);
                        let file_data = tree.get(ino).expect(&format!(
                            "lookup found fs is inconsistent, the file name={:?} of ino={}
                             found under parent ino={}, but no i-node found for the file",
                            file_name, ino, parent,
                        ));
                        // TODO: reply parent dir attr or the file attr?
                        reply.entry(&ttl, &file_data.attr, MY_GENERATION);
                        debug!(
                            "lookup successfully found the file name={:?} under parent ino={}, the attr is: {:?}",
                            file_name,
                            ino,
                            &file_data.attr,
                        );
                    }
                    None => {
                        debug!(
                            "lookup failed to find file name={:?} under parent directory ino={}",
                            file_name, parent
                        );
                        reply.error(ENOENT);
                    }
                },
                FileData::File(_) => {
                    reply.error(ENOTDIR);
                    panic!(
                        "lookup found fs is inconsistent,
                         the node type of parent ino={} should be a directory not a file",
                        parent
                    );
                }
            },
            None => {
                debug!("lookup failed to find parent i-node of ino={}", parent);
                reply.error(ENOENT);
            }
        };
    }

    // Begin non-read functions

    /// called by the VFS to set attributes for a file. This method
    /// is called by chmod(2) and related system calls.
    fn setattr(
        &mut self,
        req: &Request,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<SystemTime>,
        mtime: Option<SystemTime>,
        fh: Option<u64>,
        crtime: Option<SystemTime>,
        chgtime: Option<SystemTime>,
        bkuptime: Option<SystemTime>,
        flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        debug!(
            "setattr(ino={}, fh={:?}, size={:?}, mode={:?}, uid={:?}, gid={:?},
             atime={:?}, mtime={:?}, crtime={:?}, chgtime={:?}, bkuptime={:?}, flags={:?}, req={:?})",
            ino, fh, size, mode, uid, gid, atime, mtime, crtime, chgtime, bkuptime, flags, req.request,
        );

        let tree = &mut self
            .tree
            .write()
            .expect("setattr cannot get the write lock of fs");
        match tree.get_mut(&ino) {
            Some(node) => {
                let ttl = Duration::new(MY_TTL_SEC, 0);
                let ts = SystemTime::now();
                let attr = &mut node.attr;

                if let Some(b) = mode {
                    debug_assert!(
                        b < std::u16::MAX as u32,
                        "setattr found mode overflow, larger than u16::MAX"
                    );
                    let fmode = Mode::from_bits_truncate(b as u16);
                    attr.perm = fmode.bits();
                    debug!("setattr set permission as: {} = {:?}", attr.perm, fmode);
                    let fflag = SFlag::from_bits_truncate(b as u16);
                    if fflag.contains(SFlag::S_IFREG) {
                        attr.kind = FileType::RegularFile;
                        debug!("setattr set file type as: {:?}", fflag);
                    } else if fflag.contains(SFlag::S_IFDIR) {
                        attr.kind = FileType::Directory;
                        debug!("setattr set file type as: {:?}", fflag);
                    } else {
                        debug!("setattr received unsupported file types other than S_IFREG and S_IFDIR: {:?}", fflag);
                    }
                }
                // if no input, don't replace
                attr.uid = uid.unwrap_or(attr.uid);
                attr.gid = gid.unwrap_or(attr.gid);
                attr.size = size.unwrap_or(attr.size);
                attr.atime = atime.unwrap_or(attr.atime);
                attr.mtime = mtime.unwrap_or(attr.mtime);
                attr.crtime = crtime.unwrap_or(attr.crtime);
                attr.flags = flags.unwrap_or(attr.flags);

                if mode.is_some()
                    || uid.is_some()
                    || gid.is_some()
                    || size.is_some()
                    || atime.is_some()
                    || mtime.is_some()
                    || crtime.is_some()
                    || chgtime.is_some()
                    || bkuptime.is_some()
                    || flags.is_some()
                {
                    attr.ctime = ts; // update ctime, since meta data might change in setattr
                    reply.attr(&ttl, attr);
                    debug!(
                        "setattr successfully set the attribute of ino={}, the set attr is {:?}",
                        ino, attr,
                    );
                } else {
                    reply.error(ENODATA);
                    panic!(
                        "setattr found all the input attributes are empty for the file of ino={}",
                        ino,
                    );
                }
            }
            None => {
                reply.error(ENOENT);
                panic!("setattr failed to find the file i-node of ino={}", ino);
            }
        }
    }

    fn write(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        flags: u32,
        reply: ReplyWrite,
    ) {
        debug!(
            "write(ino={}, fh={}, offset={}, data-size={})",
            // "write(ino={}, fh={}, offset={}, data-size={}, req={:?})",
            ino,
            fh,
            offset,
            data.len(),
            // req.request,
        );

        let tree = &mut self
            .tree
            .write()
            .expect("write cannot get the write lock of fs");
        match tree.get_mut(&ino) {
            Some(file_node) => {
                match &mut file_node.data {
                    FileData::File(dir_data) => {
                        let size_after_write = offset as usize + data.len();
                        if dir_data.capacity() < size_after_write {
                            let before_cap = dir_data.capacity();
                            let extra_space_size = size_after_write - dir_data.capacity();
                            dir_data.reserve(extra_space_size);
                            // TODO: handle OOM when reserving
                            // let result = dir_data.try_reserve(extra_space_size);
                            // if result.is_err() {
                            //     warn!(
                            //         "write cannot reserve enough space, the space size needed is {} byte",
                            //         extra_space_size);
                            //     reply.error(ENOMEM);
                            //     return;
                            // }
                            debug!(
                                "write enlarged the file data vector capacity from {} to {}",
                                before_cap,
                                dir_data.capacity(),
                            );
                        }
                        match dir_data.len().cmp(&(offset as usize)) {
                            cmp::Ordering::Greater => {
                                dir_data.truncate(offset as usize);
                                debug!(
                                    "write truncated the file of ino={} to size={}",
                                    ino, offset
                                );
                            }
                            cmp::Ordering::Less => {
                                let zero_padding_size = (offset as usize) - dir_data.len();
                                let mut zero_padding_vec = vec![0u8; zero_padding_size];
                                dir_data.append(&mut zero_padding_vec);
                            }
                            cmp::Ordering::Equal => (),
                        }
                        dir_data.extend_from_slice(data);
                        reply.written(data.len() as u32);

                        // update the attribute of the written file
                        let attr = &mut file_node.attr;
                        attr.size = dir_data.len() as u64;
                        attr.flags = flags;
                        let ts = SystemTime::now();
                        attr.mtime = ts;

                        debug!(
                            "write successfully wrote {} byte data to file ino={} at offset={},
                             the attr is: {:?}, the first at most 100 byte data are: {:?}",
                            data.len(),
                            ino,
                            offset,
                            &attr,
                            if data.len() > 100 {
                                &data[0..100]
                            } else {
                                data
                            },
                        );
                    }
                    FileData::Directory(_) => {
                        reply.error(EISDIR);
                        panic!(
                            "write found fs is inconsistent, the node type of ino={} should be a file not a directory",
                            ino,
                        );
                    }
                }
            }
            None => {
                reply.error(ENOENT);
                panic!("write failed to find the i-node of ino={}", ino);
            }
        };
    }

    fn mknod(
        &mut self,
        req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        let file_name = PathBuf::from(name);
        debug!(
            "mknod(parent={}, name={:?}, mode={}, rdev={}, req={:?})",
            parent, file_name, mode, rdev, req.request,
        );

        let tree = &mut self
            .tree
            .write()
            .expect("mknod cannot get the write lock of fs");
        match tree.get_mut(&parent) {
            Some(parent_node) => match &mut parent_node.data {
                FileData::Directory(parent_data) => {
                    match parent_data.entry(file_name) {
                        Entry::Occupied(occupied) => {
                            debug!(
                                "mknod found the directory of ino={} already exists a child with name {:?} and ino={}",
                                parent,
                                name,
                                occupied.get()
                            );
                            reply.error(EEXIST);
                        }
                        Entry::Vacant(vacant_entry) => {
                            let cur_ino = self.max_ino.fetch_add(1, atomic::Ordering::SeqCst);
                            vacant_entry.insert(cur_ino);

                            debug_assert!(
                                mode < std::u16::MAX as u32,
                                "mknod found mode overflow, larger than u16::MAX"
                            );
                            let fmode = Mode::from_bits_truncate(mode as u16);
                            let perm = fmode.bits();
                            debug!("mknod set permission as: {} = {:?}", perm, fmode);

                            let ttl = Duration::new(MY_TTL_SEC, 0);
                            let ts = SystemTime::now();
                            let attr = FileAttr {
                                ino: cur_ino,
                                size: 0,
                                blocks: 0,
                                atime: ts,
                                mtime: ts,
                                ctime: ts,
                                crtime: ts,
                                kind: FileType::RegularFile,
                                perm: perm,
                                nlink: 1,
                                uid: MY_UID,
                                gid: MY_GID,
                                rdev,     // TODO: what's rdev
                                flags: 0, // TODO: what's flags
                            };
                            let file_data = FileData::File(Vec::new());
                            let file_name = PathBuf::from(name);
                            let new_node = Node {
                                name: file_name,
                                attr,
                                data: file_data,
                                parent,
                            };
                            tree.insert(cur_ino, new_node);

                            reply.entry(&ttl, &attr, MY_GENERATION);
                            debug!(
                                "mknod successfully created the new file name={:?} of ino={} under parent ino={}",
                                name, cur_ino, parent,
                            );
                        }
                    }
                }
                FileData::File(_) => {
                    reply.error(ENOTDIR);
                    panic!(
                        "mknod found fs is inconsistent, the node type of parent ino={} should be a directory not a file",
                        parent);
                }
            },
            None => {
                debug!(
                    "mknod failed to find the i-node of parent directory ino={}",
                    parent
                );
                reply.error(ENOENT);
            }
        }
    }

    fn unlink(&mut self, req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let file_name = PathBuf::from(name);
        debug!(
            "unlink(parent={}, name={:?}, req={:?}",
            parent, file_name, req.request,
        );

        let tree = &mut self
            .tree
            .write()
            .expect("unlink cannot get the write lock of fs");
        match tree.get_mut(&parent) {
            Some(parent_node) => match &mut parent_node.data {
                FileData::Directory(parent_data) => {
                    match parent_data.entry(file_name) {
                        Entry::Occupied(parent_entry) => {
                            // remove the entry of this file from parent directory
                            let ino = parent_entry.remove();

                            // remove inode of the file to delete
                            let dir_data = tree.remove(&ino).expect(&format!(
                                "unlink found fs is inconsistent, file name={:?} of ino={}
                                 found under the parent ino={}, but no i-node found for the file",
                                name, ino, parent,
                            ));
                            debug!(
                                "unlink successfully removed the file name={:?} of ino={}
                                 under parent ino={}, its attr is: {:?}",
                                name, ino, parent, &dir_data.attr,
                            );
                            reply.ok();
                        }
                        Entry::Vacant(_) => {
                            debug!(
                                "unlink failed to find file name={:?} under parent directory ino={}",
                                name, parent,
                            );
                            reply.error(ENOENT);
                        }
                    }
                }
                FileData::File(_) => {
                    reply.error(ENOTDIR);
                    panic!(
                        "unlink found fs is inconsistent,
                         the node type of parent ino={} should be a directory not a file",
                        parent,
                    );
                }
            },
            None => {
                debug!("unlink failed to find the i-node of parent ino={}", parent);
                reply.error(ENOENT);
            }
        };
    }

    fn mkdir(&mut self, req: &Request, parent: u64, name: &OsStr, mode: u32, reply: ReplyEntry) {
        let dir_name = PathBuf::from(name);
        debug!(
            "mkdir(parent={}, name={:?}, mode={}, req={:?})",
            parent, dir_name, mode, req.request,
        );

        let tree = &mut self
            .tree
            .write()
            .expect("mdkir cannot get the write lock of fs");
        match tree.get_mut(&parent) {
            Some(parent_node) => match &mut parent_node.data {
                FileData::Directory(parent_data) => {
                    match parent_data.entry(dir_name) {
                        Entry::Occupied(occupied) => {
                            debug!(
                                "mkdir found the parent directory of ino={}
                                 already has a child with name={:?} of ino={}",
                                parent,
                                name,
                                occupied.get()
                            );
                            reply.error(EEXIST);
                        }
                        Entry::Vacant(empty_val) => {
                            let cur_ino = self.max_ino.fetch_add(1, atomic::Ordering::SeqCst);
                            empty_val.insert(cur_ino);
                            let ttl = Duration::new(MY_TTL_SEC, 0);
                            let ts = SystemTime::now();

                            debug_assert!(
                                mode < std::u16::MAX as u32,
                                "mkdir found mode overflow, larger than u16::MAX"
                            );
                            let fmode = Mode::from_bits_truncate(mode as u16);
                            let perm = fmode.bits();
                            debug!("mkdir set permission as: {} = {:?}", perm, fmode);
                            let attr = FileAttr {
                                ino: cur_ino,
                                size: 0,
                                blocks: 0,
                                atime: ts,
                                mtime: ts,
                                ctime: ts,
                                crtime: ts,
                                kind: FileType::Directory,
                                perm: perm,
                                nlink: 2,
                                uid: MY_UID,
                                gid: MY_GID,
                                rdev: 0,  // TODO: what's rdev?
                                flags: 0, // TODO: what's rdev?
                            };

                            let mut new_dir_data = BTreeMap::new();
                            new_dir_data.insert(PathBuf::from("."), cur_ino);
                            new_dir_data.insert(PathBuf::from(".."), parent);
                            let new_dir_data = FileData::Directory(new_dir_data);
                            let new_dir_name = PathBuf::from(name);
                            let new_dir_node = Node {
                                name: new_dir_name,
                                attr,
                                data: new_dir_data,
                                parent,
                            };
                            tree.insert(cur_ino, new_dir_node);

                            reply.entry(&ttl, &attr, MY_GENERATION);
                            debug!(
                                "mkdir successfully created the new directory name={:?} of ino={}
                                 under the parent directory ino={}",
                                name, cur_ino, parent,
                            );
                        }
                    }
                }
                FileData::File(_) => {
                    reply.error(ENOTDIR);
                    panic!(
                        "mkdir found fs is inconsistent,
                         the node type of parent ino={} should be a directory not a file",
                        parent,
                    );
                }
            },
            None => {
                debug!(
                    "mkdir failed to find the i-node of parent directory ino={}",
                    parent,
                );
                reply.error(ENOENT);
            }
        }
    }

    fn rmdir(&mut self, req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let dir_name = PathBuf::from(name);
        debug!(
            "rmdir(parent={}, name={:?}, req={:?})",
            parent, dir_name, req.request,
        );

        let tree = &mut self
            .tree
            .write()
            .expect("rm cannot get the write lock of fs");

        // do all kinds of checks, if fail return error
        match tree.get(&parent) {
            Some(parent_node) => match &parent_node.data {
                FileData::Directory(parent_data) => {
                    match parent_data.get(&dir_name) {
                        Some(dir_ino) => {
                            match tree.get(dir_ino) {
                                Some(dir_node) => match &dir_node.data {
                                    FileData::Directory(dir_data) => {
                                        if !(dir_data.len() == 2
                                            && dir_data.contains_key(&PathBuf::from("."))
                                            && dir_data.contains_key(&PathBuf::from("..")))
                                        {
                                            reply.error(ENOTEMPTY);
                                            debug!(
                                                "rmdir cannot remove the non-empty sub directory name={:?} of ino={}
                                                 under the parent directory of ino={}",
                                                dir_name, dir_ino, parent,
                                            );
                                            return;
                                        }
                                    }
                                    FileData::File(_) => {
                                        reply.error(ENOTDIR);
                                        debug!(
                                            "rmdir cannot remove a file, the node type of name={:?} of ino={}
                                            under the parent ino={} is a file not a directory",
                                            dir_name, dir_ino, parent,
                                        );
                                        return;
                                    }
                                },
                                None => {
                                    reply.error(ENOENT);
                                    panic!(
                                        "rmdir found fs is inconsistent, the sub directory of name={:?} and ino={} 
                                        found under the parent ino={}, but no i-node for the sub directory of to remove",
                                        dir_name, dir_ino, parent,
                                    );
                                    // return;
                                }
                            }
                        }
                        None => {
                            reply.error(ENOENT);
                            debug!(
                                "rmdir failed to find the sub directory name={:?} under the parent ino={}",
                                dir_name, parent,
                            );
                            return;
                        }
                    }
                }
                FileData::File(_) => {
                    reply.error(ENOTDIR);
                    panic!(
                        "rmdir found fs is inconsistent,
                         the node type of parent ino={} should be a directory not a file",
                        parent,
                    );
                    // return;
                }
            },
            None => {
                reply.error(ENOENT);
                debug!("rmdir failed to find the i-node of parent ino={}", parent,);
                return;
            }
        };

        // all checks passed, ready to rmdir, it's safe to use unwrap(), won't panic
        if let FileData::Directory(parent_node) = &mut tree.get_mut(&parent).unwrap().data {
            // remove the sub directory from the parent directory
            let dir_ino = parent_node.remove(&dir_name).unwrap();

            // remove the sub directory from the tree
            let dir_node = tree.remove(&dir_ino).unwrap();

            reply.ok();
            debug!(
                "rmdir successfully removed the sub directory name={:?} of ino={}
                 under parent directory ino={}, the sub dir node is: {:?}",
                dir_name, dir_ino, parent, dir_node,
            );
            return;
        }
        panic!("rmdir should never reach here");
    }

    /// Rename a file
    /// The filesystem must return -EINVAL for any unsupported or
    /// unknown flags. Currently the following flags are implemented:
    /// (1) RENAME_NOREPLACE: this flag indicates that if the target
    /// of the rename exists the rename should fail with -EEXIST
    /// instead of replacing the target.  The VFS already checks for
    /// existence, so for local filesystems the RENAME_NOREPLACE
    /// implementation is equivalent to plain rename.
    /// (2) RENAME_EXCHANGE: exchange source and target.  Both must
    /// exist; this is checked by the VFS.  Unlike plain rename,
    /// source and target may be of different type.
    fn rename(
        &mut self,
        req: &Request,
        parent: u64,
        name: &OsStr,
        new_parent: u64,
        newname: &OsStr,
        reply: ReplyEmpty,
    ) {
        let (old_name, new_name) = (PathBuf::from(name), PathBuf::from(newname));
        debug!(
            "rename(old parent={}, old name={:?}, new parent={}, new name={:?}, req={:?})",
            parent, old_name, new_parent, new_name, req.request,
        );

        let tree = &mut self
            .tree
            .write()
            .expect("rename cannot get the write lock of fs");

        // check the new parent has no entry with the same name as the rename file
        match tree.get(&new_parent) {
            Some(new_parent_node) => match &new_parent_node.data {
                FileData::Directory(new_parent_data) => {
                    if new_parent_data.contains_key(&new_name) {
                        reply.error(EEXIST); // RENAME_NOREPLACE
                        debug!(
                            "rename found the new parent directory of ino={} already has a child with name={:?}",
                            new_parent, new_name,
                        );
                        return;
                    }
                }
                FileData::File(_) => {
                    reply.error(ENOTDIR);
                    panic!(
                        "rename found fs is inconsistent, the node type of new parent ino={} should be a directory not a file",
                        parent,
                    );
                    // return;
                }
            },
            None => {
                reply.error(ENOENT);
                debug!(
                    "rename failed to find the i-node of new parent directory ino={}",
                    parent,
                );
                return;
            }
        };

        let rename_ino: u64;
        // check the old parent contains the rename file
        match tree.get(&parent) {
            Some(old_parent_node) => match &old_parent_node.data {
                FileData::Directory(old_parent_data) => match old_parent_data.get(&old_name) {
                    Some(old_ino) => rename_ino = old_ino.clone(),
                    None => {
                        reply.error(ENOENT);
                        debug!(
                                "rename cannot find the old file name={:?} under the old parent directory of ino={}",
                                old_name, parent,
                            );
                        return;
                    }
                },
                FileData::File(_) => {
                    reply.error(ENOTDIR);
                    panic!(
                        "rename found fs is inconsistent,
                         the node type of old parent ino={} should be a directory not a file",
                        parent,
                    );
                    // return;
                }
            },
            None => {
                reply.error(ENOENT);
                debug!(
                    "rename failed to find the i-node of old parent directory ino={}",
                    parent,
                );
                return;
            }
        }

        // check the i-node of rename file exists
        if !tree.contains_key(&rename_ino) {
            reply.error(ENOENT);
            panic!(
                "rename found fs is inconsistent, the file name={:?} of ino={}
                 found under the parent ino={}, but no i-node found for the file",
                old_name, rename_ino, parent,
            );
            // return;
        }

        // all checks passed, ready to rename, it's safe to use unwrap()
        if let FileData::Directory(old_parent_data) = &mut tree.get_mut(&parent).unwrap().data {
            // remove the inode of old file from old directory
            let rename_ino = old_parent_data.remove(&old_name).unwrap();
            if let FileData::Directory(new_parent_data) =
                &mut tree.get_mut(&new_parent).unwrap().data
            {
                // move from old parent directory to new parent
                new_parent_data.insert(new_name, rename_ino);

                let moved_file_info = tree.get_mut(&rename_ino).unwrap();
                // update the parent inode of the moved file
                moved_file_info.parent = new_parent;

                // change the ctime of the moved file
                let ts = SystemTime::now();
                moved_file_info.attr.ctime = ts;

                reply.ok();
                debug!(
                    "rename successfully moved the old file name={:?} of ino={} under old parent ino={}
                     to the new file name={:?} ino={} under new parent ino={}",
                    old_name, rename_ino, parent, newname, rename_ino, new_parent,
                );
                return;
            }
        }
        panic!("rename should never reach here");
    }
}

fn main() {
    env_logger::init();

    let mountpoint = match env::args_os().nth(1) {
        Some(path) => path,
        None => {
            println!("Usage: {} <MOUNTPOINT>", env::args().nth(0).unwrap());
            return;
        }
    };
    let options = [
        // "-d",
        //"-r",
        "-s",
        "-f",
        "-o",
        "debug",
        "-o",
        "fsname=fuse_rs_demo",
        "-o",
        "kill_on_unmount",
    ]
    .iter()
    .map(|o| o.as_ref())
    .collect::<Vec<&OsStr>>();
    dbg!(&options);

    let fs = MemoryFilesystem::new();
    dbg!(&fs);
    fuse::mount(fs, mountpoint, &options).expect("Couldn't mount filesystem");
}

#[cfg(test)]
mod test {
    #[test]
    fn test_tmp() {
        fn u64fn(u64ref: u64) {
            dbg!(u64ref);
        }
        let num: u64 = 100;
        let u64ref = &num;
        u64fn(u64ref.clone());
    }

    #[test]
    fn test_skip() {
        let v = vec![1, 2, 3, 4];
        for e in v.iter().skip(5) {
            dbg!(e);
        }
    }

    #[test]
    fn test_vec() {
        let mut v = vec![1, 2, 3, 4, 5];
        let cap = v.capacity();
        v.truncate(3);
        assert_eq!(v.len(), 3);
        assert_eq!(v.capacity(), cap);

        let mut v2 = vec![0; 3];
        v.append(&mut v2);
        assert_eq!(v.len(), 6);
        assert!(v2.is_empty());
    }

    #[test]
    fn test_map_swap() {
        use std::collections::{btree_map::Entry, BTreeMap};
        use std::ptr;
        use std::sync::RwLock;
        let mut map = BTreeMap::<String, Vec<u8>>::new();
        let (k1, k2, k3, k4) = ("A", "B", "C", "D");
        map.insert(k1.to_string(), vec![1]);
        map.insert(k2.to_string(), vec![2, 2]);
        map.insert(k3.to_string(), vec![3, 3]);
        map.insert(k4.to_string(), vec![4, 4, 4, 4]);

        let lock = RwLock::new(map);
        let mut map = lock.write().unwrap();

        let e1 = map.get_mut(k1).unwrap() as *mut _;
        let e2 = map.get_mut(k2).unwrap() as *mut _;
        // mem::swap(e1, e2);
        unsafe {
            ptr::swap(e1, e2);
        }
        dbg!(&map[k1]);
        dbg!(&map[k2]);

        let e3 = map.get_mut(k3).unwrap();
        e3.push(3);
        dbg!(&map[k3]);

        let k5 = "E";
        let e = map.entry(k5.to_string());
        if let Entry::Vacant(v) = e {
            v.insert(vec![5, 5, 5, 5, 5]);
        }
        dbg!(&map[k5]);
    }
    #[test]
    fn test_map_entry() {
        use std::collections::BTreeMap;
        use std::mem;
        let mut m1 = BTreeMap::<String, Vec<u8>>::new();
        let mut m2 = BTreeMap::<String, Vec<u8>>::new();
        let (k1, k2, k3, k4, k5) = ("A", "B", "C", "D", "E");
        m1.insert(k1.to_string(), vec![1]);
        m1.insert(k2.to_string(), vec![2, 2]);
        m2.insert(k3.to_string(), vec![3, 3, 3]);
        m2.insert(k4.to_string(), vec![4, 4, 4, 4]);

        let e1 = &mut m1.entry(k1.to_string());
        let e2 = &mut m2.entry(k5.to_string());
        mem::swap(e1, e2);

        dbg!(m1);
        dbg!(m2);
    }
}
