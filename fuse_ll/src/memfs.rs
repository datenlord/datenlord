use crate::fuse::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyOpen, ReplyWrite, Request, FUSE_ROOT_ID,
};
use libc::{EEXIST, EINVAL, ENODATA, ENOENT, ENOTEMPTY};
use log::{debug, error}; // info, warn
use nix::dir::{Dir, Entry, Type};
use nix::fcntl::{self, FcntlArg, OFlag};
use nix::sys::stat::{self, FileStat, Mode, SFlag};
use nix::sys::uio;
use nix::unistd::{self, UnlinkatFlags};
use std::cell::{Cell, RefCell};
use std::cmp;
use std::collections::{BTreeMap, BTreeSet};
use std::convert::AsRef;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::ops::{Deref, Drop};
use std::os::raw::c_int;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::{Path, PathBuf};
use std::sync::atomic::{self, AtomicI64};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const MY_TTL_SEC: u64 = 1; // TODO: should be a long value, say 1 hour
const MY_GENERATION: u64 = 1;
// const MY_DIR_MODE: u16 = 0o755;
// const MY_FILE_MODE: u16 = 0o644;
// const FUSE_ROOT_ID: u64 = 1; // defined in include/fuse_kernel.h

mod util {
    use super::*;

    pub fn parse_oflag(flags: u32) -> OFlag {
        debug_assert!(
            flags < std::i32::MAX as u32,
            format!(
                "helper_parse_oflag() found flags={} overflow, larger than u16::MAX",
                flags,
            ),
        );
        let oflags = OFlag::from_bits_truncate(flags as i32);
        debug!("helper_parse_oflag() read file flags: {:?}", oflags);
        oflags
    }

    pub fn parse_mode(mode: u32) -> Mode {
        debug_assert!(
            mode < std::u16::MAX as u32,
            format!(
                "helper_parse_mode() found mode={} overflow, larger than u16::MAX",
                mode,
            ),
        );

        #[cfg(target_os = "linux")]
        let fmode = Mode::from_bits_truncate(mode);
        #[cfg(target_os = "macos")]
        let fmode = Mode::from_bits_truncate(mode as u16);
        debug!("helper_parse_mode() read file mode: {:?}", fmode);
        fmode
    }
    pub fn parse_mode_bits(mode: u32) -> u16 {
        #[cfg(target_os = "linux")]
        let bits = parse_mode(mode).bits() as u16;
        #[cfg(target_os = "macos")]
        let bits = parse_mode(mode).bits();

        bits
    }

    pub fn parse_sflag(flags: u32) -> SFlag {
        debug_assert!(
            flags < std::u16::MAX as u32,
            format!(
                "parse_sflag() found flags={} overflow, larger than u16::MAX",
                flags,
            ),
        );

        #[cfg(target_os = "linux")]
        let sflag = SFlag::from_bits_truncate(flags);
        #[cfg(target_os = "macos")]
        let sflag = SFlag::from_bits_truncate(flags as u16);
        debug!("convert_sflag() read file type as: {:?}", sflag);
        sflag
    }

    pub fn convert_sflag(sflag: SFlag) -> FileType {
        match sflag {
            SFlag::S_IFDIR => FileType::Directory,
            SFlag::S_IFREG => FileType::RegularFile,
            _ => panic!("convert_sflag() found unsupported file type: {:?}", sflag),
        }
    }

    pub fn convert_node_type(file_type: &Type) -> FileType {
        match file_type {
            Type::Directory => FileType::Directory,
            Type::File => FileType::RegularFile,
            _ => panic!(
                "helper_convert_node_type() found unsupported file type: {:?}",
                file_type,
            ),
        }
    }

    pub fn open_dir(path: &Path) -> Result<Dir, nix::Error> {
        let oflags = OFlag::O_RDONLY | OFlag::O_DIRECTORY;
        // let dfd = fcntl::open(path, oflags, Mode::empty())?;
        let dfd = Dir::open(path, oflags, Mode::empty())?;
        Ok(dfd)
    }

    pub fn open_dir_at(dir: &Dir, child_name: &OsStr) -> Result<Dir, nix::Error> {
        let oflags = OFlag::O_RDONLY | OFlag::O_DIRECTORY;
        let dir = Dir::openat(dir.as_raw_fd(), child_name, oflags, Mode::empty())?;
        Ok(dir)
    }

    pub fn read_attr(fd: RawFd) -> Result<FileAttr, nix::Error> {
        let st = stat::fstat(fd)?;

        #[cfg(target_os = "macos")]
        fn build_crtime(st: &FileStat) -> Option<SystemTime> {
            UNIX_EPOCH.checked_add(Duration::new(
                st.st_birthtime as u64,
                st.st_birthtime_nsec as u32,
            ))
        }
        #[cfg(target_os = "linux")]
        fn build_crtime(_st: &FileStat) -> Option<SystemTime> {
            None
        }

        let atime =
            UNIX_EPOCH.checked_add(Duration::new(st.st_atime as u64, st.st_atime_nsec as u32));
        let mtime =
            UNIX_EPOCH.checked_add(Duration::new(st.st_mtime as u64, st.st_mtime_nsec as u32));
        let ctime =
            UNIX_EPOCH.checked_add(Duration::new(st.st_ctime as u64, st.st_ctime_nsec as u32));
        let crtime = build_crtime(&st);

        let perm = parse_mode_bits(st.st_mode as u32);
        debug!("read_attr() got file permission as: {}", perm);
        let sflag = parse_sflag(st.st_mode as u32);
        let kind = convert_sflag(sflag);

        let nt = SystemTime::now();
        let attr = FileAttr {
            ino: st.st_ino,
            size: st.st_size as u64,
            blocks: st.st_blocks as u64,
            atime: atime.unwrap_or(nt),
            mtime: mtime.unwrap_or(nt),
            ctime: ctime.unwrap_or(nt),
            crtime: crtime.unwrap_or(nt),
            kind,
            perm,
            nlink: st.st_nlink as u32,
            uid: st.st_uid,
            gid: st.st_gid,
            rdev: st.st_rdev as u32,
            #[cfg(target_os = "linux")]
            flags: 0,
            #[cfg(target_os = "macos")]
            flags: st.st_flags,
        };
        Ok(attr)
    }
}

#[derive(Debug)]
struct DirEntry {
    ino: u64,
    name: OsString,
    entry_type: Type,
}

#[derive(Debug)]
struct DirNode {
    parent: Cell<u64>,
    name: RefCell<OsString>,
    attr: Cell<FileAttr>,
    data: RefCell<BTreeMap<OsString, DirEntry>>,
    dir_fd: RefCell<Dir>,
    open_count: AtomicI64,
    lookup_count: AtomicI64,
}

#[derive(Debug)]
struct FileNode {
    parent: Cell<u64>,
    name: RefCell<OsString>,
    attr: Cell<FileAttr>,
    data: RefCell<Vec<u8>>,
    fd: RawFd,
    open_count: AtomicI64,
    lookup_count: AtomicI64,
}

impl Drop for FileNode {
    fn drop(&mut self) {
        unistd::close(self.fd).unwrap_or_else(|_| {
            panic!(
                "FileNode::drop() failed to clode the file handler of
                file name {:?} ino={}",
                self.name,
                self.attr.get_mut().ino
            )
        });
    }
}

#[derive(Debug)]
enum INode {
    DIR(DirNode),
    FILE(FileNode),
}

impl INode {
    fn helper_get_dir_node(&self) -> &DirNode {
        match self {
            INode::DIR(dir_node) => dir_node,
            INode::FILE(_) => panic!("helper_get_dir_node() cannot read FileNode"),
        }
    }

    fn helper_get_file_node(&self) -> &FileNode {
        match self {
            INode::DIR(_) => panic!("helper_get_file_node() cannot read DirNode"),
            INode::FILE(file_node) => file_node,
        }
    }

    fn get_ino(&self) -> u64 {
        self.get_attr().ino
    }

    fn get_parent_ino(&self) -> u64 {
        match self {
            INode::DIR(dir_node) => dir_node.parent.get(),
            INode::FILE(file_node) => file_node.parent.get(),
        }
    }

    fn set_parent_ino(&self, parent: u64) -> u64 {
        match self {
            INode::DIR(dir_node) => dir_node.parent.replace(parent),
            INode::FILE(file_node) => file_node.parent.replace(parent),
        }
    }

    fn get_name(&self) -> impl Deref<Target = OsString> + '_ {
        match self {
            INode::DIR(dir_node) => dir_node.name.borrow(),
            INode::FILE(file_node) => file_node.name.borrow(),
        }
    }

    fn set_name(&self, name: OsString) -> OsString {
        match self {
            INode::DIR(dir_node) => dir_node.name.replace(name),
            INode::FILE(file_node) => file_node.name.replace(name),
        }
    }

    fn get_type(&self) -> Type {
        match self {
            INode::DIR(_) => Type::Directory,
            INode::FILE(_) => Type::File,
        }
    }

    fn get_attr(&self) -> FileAttr {
        match self {
            INode::DIR(dir_node) => dir_node.attr.get(),
            INode::FILE(file_node) => file_node.attr.get(),
        }
    }

    fn lookup_attr(&self, func: impl FnOnce(&FileAttr)) {
        let attr = match self {
            INode::DIR(dir_node) => {
                let attr = dir_node.attr.get();
                debug_assert_eq!(attr.kind, FileType::Directory);
                attr
            }
            INode::FILE(file_node) => {
                let attr = file_node.attr.get();
                debug_assert_eq!(attr.kind, FileType::RegularFile);
                attr
            }
        };
        func(&attr);
        self.inc_lookup_count();
    }

    fn set_attr(&mut self, func: impl FnOnce(&mut FileAttr)) {
        match self {
            INode::DIR(dir_node) => {
                let attr = dir_node.attr.get_mut();
                debug_assert_eq!(attr.kind, FileType::Directory);
                func(attr);
            }
            INode::FILE(file_node) => {
                let attr = file_node.attr.get_mut();
                debug_assert_eq!(attr.kind, FileType::RegularFile);
                func(attr);
            }
        }
    }

    fn inc_open_count(&self) -> i64 {
        match self {
            INode::DIR(dir_node) => dir_node.open_count.fetch_add(1, atomic::Ordering::SeqCst),
            INode::FILE(file_node) => file_node.open_count.fetch_add(1, atomic::Ordering::SeqCst),
        }
    }

    fn dec_open_count(&self) -> i64 {
        match self {
            INode::DIR(dir_node) => dir_node.open_count.fetch_sub(1, atomic::Ordering::SeqCst),
            INode::FILE(file_node) => file_node.open_count.fetch_sub(1, atomic::Ordering::SeqCst),
        }
    }

    fn get_open_count(&self) -> i64 {
        match self {
            INode::DIR(dir_node) => dir_node.open_count.load(atomic::Ordering::SeqCst),
            INode::FILE(file_node) => file_node.open_count.load(atomic::Ordering::SeqCst),
        }
    }

    fn inc_lookup_count(&self) -> i64 {
        match self {
            INode::DIR(dir_node) => dir_node.lookup_count.fetch_add(1, atomic::Ordering::SeqCst),
            INode::FILE(file_node) => file_node
                .lookup_count
                .fetch_add(1, atomic::Ordering::SeqCst),
        }
    }

    fn dec_lookup_count_by(&self, nlookup: u64) -> i64 {
        debug_assert!(nlookup < std::i64::MAX as u64);
        match self {
            INode::DIR(dir_node) => dir_node
                .lookup_count
                .fetch_sub(nlookup as i64, atomic::Ordering::SeqCst),
            INode::FILE(file_node) => file_node
                .lookup_count
                .fetch_sub(nlookup as i64, atomic::Ordering::SeqCst),
        }
    }

    fn get_lookup_count(&self) -> i64 {
        match self {
            INode::DIR(dir_node) => dir_node.lookup_count.load(atomic::Ordering::SeqCst),
            INode::FILE(file_node) => file_node.lookup_count.load(atomic::Ordering::SeqCst),
        }
    }

    fn get_entry(&self, name: &OsString) -> Option<DirEntry> {
        let parent_node = self.helper_get_dir_node();
        match parent_node.data.borrow().get(name) {
            // TODO: how to return value within RefCell without copy explicitly
            Some(dir_entry) => Some(DirEntry {
                ino: dir_entry.ino,
                name: dir_entry.name.clone(),
                entry_type: dir_entry.entry_type,
            }),
            None => None,
        }
    }

    fn open_root_inode(root_ino: u64, name: OsString, path: &Path) -> INode {
        let dir_fd = util::open_dir(path)
            .unwrap_or_else(|_| panic!("new_dir_inode() failed to open directory {:?}", path));
        let mut attr = util::read_attr(dir_fd.as_raw_fd()).unwrap_or_else(|_| {
            panic!(
                "new_dir_inode() failed to read directory attribute {:?}",
                path
            )
        });
        attr.ino = root_ino; // replace root ino with 1

        // lookup count and open count are increased to 1 by creation
        let root_inode = INode::DIR(DirNode {
            parent: Cell::new(root_ino),
            name: RefCell::new(name),
            attr: Cell::new(attr),
            data: RefCell::new(BTreeMap::new()),
            dir_fd: RefCell::new(dir_fd),
            open_count: AtomicI64::new(1),
            lookup_count: AtomicI64::new(1),
        });

        if root_inode.need_load_data() {
            root_inode.helper_load_dir_data();
        }

        root_inode
    }

    fn helper_open_child_dir(
        &self,
        child_dir_name: &OsString,
        mode: Mode,
        create_dir: bool,
    ) -> INode {
        let parent_node = self.helper_get_dir_node();
        let parent = self.get_ino();

        if create_dir {
            stat::mkdirat(
                parent_node.dir_fd.borrow().as_raw_fd(),
                &PathBuf::from(child_dir_name),
                mode,
            )
            .unwrap_or_else(|_| panic!("helper_open_child_dir() failed to create directory name={:?} under parent ino={}", child_dir_name, parent));
        }

        let child_dir_fd = util::open_dir_at(&parent_node.dir_fd.borrow(), child_dir_name)
            .unwrap_or_else(|_| {
                panic!(
                    "helper_open_child_dir() failed to open the new directory name={:?}
                    under parent ino={}",
                    child_dir_name, parent
                )
            });
        let child_raw_fd = child_dir_fd.as_raw_fd();

        // get new directory attribute
        let child_attr = util::read_attr(child_raw_fd).unwrap_or_else(|_| {
            panic!(
                "helper_open_child_dir() failed to get the attribute of the new child directory"
                    .to_string()
            )
        });
        debug_assert_eq!(FileType::Directory, child_attr.kind);

        if create_dir {
            // insert new entry to parent directory
            // TODO: support thread-safe
            let parent_data = &mut *parent_node.data.borrow_mut();
            let previous_value = parent_data.insert(
                child_dir_name.clone(),
                DirEntry {
                    ino: child_attr.ino,
                    name: child_dir_name.clone(),
                    entry_type: Type::Directory,
                },
            );
            debug_assert!(previous_value.is_none());
        }

        // lookup count and open count are increased to 1 by creation
        let child_inode = INode::DIR(DirNode {
            parent: Cell::new(parent),
            name: RefCell::new(child_dir_name.clone()),
            attr: Cell::new(child_attr),
            data: RefCell::new(BTreeMap::new()),
            dir_fd: RefCell::new(child_dir_fd),
            open_count: AtomicI64::new(1),
            lookup_count: AtomicI64::new(1),
        });

        if child_inode.need_load_data() {
            child_inode.helper_load_dir_data();
        }

        child_inode
    }

    fn open_child_dir(&self, child_dir_name: &OsString) -> INode {
        self.helper_open_child_dir(child_dir_name, Mode::empty(), false)
    }

    fn create_child_dir(&self, child_dir_name: &OsString, mode: Mode) -> INode {
        self.helper_open_child_dir(child_dir_name, mode, true)
    }

    fn helper_load_dir_data(&self) {
        let dir_node = self.helper_get_dir_node();
        let dir_entry: Vec<Entry> = dir_node
            .dir_fd
            .borrow_mut()
            .iter()
            .filter(|e| e.is_ok())
            .map(|e| e.unwrap()) // safe to use unwrap() here
            .filter(|e| {
                let bytes = e.file_name().to_bytes();
                !bytes.starts_with(&[b'.']) // skip hidden entries, '.' and '..'
            })
            .filter(|e| match e.file_type() {
                Some(t) => match t {
                    Type::Fifo => false,
                    Type::CharacterDevice => false,
                    Type::Directory => true,
                    Type::BlockDevice => false,
                    Type::File => true,
                    Type::Symlink => false,
                    Type::Socket => false,
                },
                None => false,
            })
            .collect();

        dir_entry.iter().for_each(|e| {
            let name = OsString::from(OsStr::from_bytes(e.file_name().to_bytes()));
            dir_node.data.borrow_mut().insert(
                // TODO: use functional way to load dir
                name.clone(),
                DirEntry {
                    ino: e.ino(),
                    name,
                    entry_type: e.file_type().unwrap(), // safe to use unwrap() here
                },
            );
        });

        let entry_count = dir_entry.len();
        debug!(
            "helper_load_dir_data() successfully load {} directory entries",
            entry_count,
        );
    }

    fn helper_load_file_data(&self) {
        let file_node = self.helper_get_file_node();
        let ino = self.get_ino();
        let fd = file_node.fd;
        let file_size = file_node.attr.get().size;
        let file_data: &mut Vec<u8> = &mut file_node.data.borrow_mut();
        file_data.reserve(file_size as usize);
        #[allow(unsafe_code)]
        unsafe {
            file_data.set_len(file_data.capacity());
        }
        let res = unistd::read(fd, &mut *file_data);
        #[allow(unsafe_code)]
        match res {
            Ok(s) => unsafe {
                file_data.set_len(s);
            },
            Err(e) => {
                panic!(
                    "helper_load_file_data() failed to
                        read the file of ino={} from disk, the error is: {:?}",
                    ino, e,
                );
            }
        }
        debug_assert_eq!(file_data.len(), file_size as usize);
        debug!(
            "helper_load_file_data() successfully load {} byte data",
            file_size,
        );
    }

    fn helper_reload_attribute(&self) -> FileAttr {
        let raw_fd = match self {
            INode::DIR(dir_node) => dir_node.dir_fd.borrow().as_raw_fd(),
            INode::FILE(file_node) => file_node.fd,
        };
        let attr = util::read_attr(raw_fd).unwrap_or_else(|_| {
            panic!(
                "helper_reload_attribute() failed to get the attribute of the node ino={}",
                self.get_ino()
            )
        });
        match self {
            INode::DIR(_) => debug_assert_eq!(FileType::Directory, attr.kind),
            INode::FILE(_) => debug_assert_eq!(FileType::RegularFile, attr.kind),
        };
        attr
    }

    // to open child, parent dir must have been opened
    fn helper_open_child_file(
        &self,
        child_file_name: &OsString,
        oflags: OFlag,
        mode: Mode,
        create_file: bool,
    ) -> INode {
        let parent_node = self.helper_get_dir_node();
        let parent = self.get_ino();

        if create_file {
            debug_assert!(oflags.contains(OFlag::O_CREAT));
        }
        let child_fd = fcntl::openat(
            parent_node.dir_fd.borrow().as_raw_fd(),
            &PathBuf::from(child_file_name),
            oflags,
            mode,
        )
        .unwrap_or_else(|_| {
            panic!(
                "helper_open_child_file() failed to open a file name={:?}
                under parent ino={} with oflags: {:?} and mode: {:?}",
                child_file_name, parent, oflags, mode
            )
        });

        // get new file attribute
        let child_attr = util::read_attr(child_fd).unwrap_or_else(|_| {
            panic!(
                "helper_open_child_file() failed to get the attribute of the new child".to_string()
            )
        });
        debug_assert_eq!(FileType::RegularFile, child_attr.kind);

        if create_file {
            // insert new entry to parent directory
            // TODO: support thread-safe
            let parent_data = &mut *parent_node.data.borrow_mut();
            let previous_value = parent_data.insert(
                child_file_name.clone(),
                DirEntry {
                    ino: child_attr.ino,
                    name: child_file_name.clone(),
                    entry_type: Type::File,
                },
            );
            debug_assert!(previous_value.is_none());
        }

        // lookup count and open count are increased to 1 by creation
        INode::FILE(FileNode {
            parent: Cell::new(parent),
            name: RefCell::new(child_file_name.clone()),
            attr: Cell::new(child_attr),
            data: RefCell::new(Vec::new()),
            fd: child_fd,
            open_count: AtomicI64::new(1),
            lookup_count: AtomicI64::new(1),
        })
    }

    fn open_child_file(&self, child_file_name: &OsString, oflags: OFlag) -> INode {
        self.helper_open_child_file(child_file_name, oflags, Mode::empty(), false)
    }

    fn create_child_file(&self, child_file_name: &OsString, oflags: OFlag, mode: Mode) -> INode {
        self.helper_open_child_file(child_file_name, oflags, mode, true)
    }

    fn dup_fd(&self, oflags: OFlag) -> RawFd {
        let raw_fd: RawFd;
        match self {
            INode::DIR(dir_node) => {
                raw_fd = dir_node.dir_fd.borrow().as_raw_fd();
            }
            INode::FILE(file_node) => {
                raw_fd = file_node.fd;
            }
        }
        let ino = self.get_ino();
        let new_fd = unistd::dup(raw_fd).unwrap_or_else(|_| {
            panic!(
                "dup_fd() failed to duplicate the handler ino={} raw fd={:?}",
                ino, raw_fd
            )
        });
        // let fcntl_oflags = FcntlArg::F_SETFL(oflags);
        // fcntl::fcntl(new_fd, fcntl_oflags).expect(&format!(
        //     "dup_fd() failed to set the flags {:?} of duplicated handler of ino={}",
        //     oflags, ino,
        // ));
        unistd::dup3(raw_fd, new_fd, oflags).unwrap_or_else(|_| {
            panic!(
                "dup_fd() failed to set the flags {:?} of duplicated handler of ino={}",
                oflags, ino
            )
        });
        self.inc_open_count();
        new_fd
    }

    fn insert_entry(&self, child_entry: DirEntry) -> Option<DirEntry> {
        let parent_node = self.helper_get_dir_node();
        let previous_entry = parent_node
            .data
            .borrow_mut()
            .insert(child_entry.name.clone(), child_entry);
        debug!(
            "insert_entry() successfully inserted new entry and replaced previous entry: {:?}",
            previous_entry,
        );

        previous_entry
    }

    fn remove_entry(&self, child_name: &OsString) -> DirEntry {
        let parent_node = self.helper_get_dir_node();
        parent_node
            .data
            .borrow_mut()
            .remove(child_name)
            .unwrap_or_else(|| {
                panic!(
                    "unlink_entry found fs is inconsistent, the entry of name={:?}
                is not in directory of name={:?} and ino={}",
                    child_name,
                    self.get_name().as_os_str(),
                    self.get_ino()
                )
            })
    }

    fn unlink_entry(&self, child_name: &OsString) -> DirEntry {
        let parent_node = self.helper_get_dir_node();
        let child_entry = self.remove_entry(child_name);
        // delete from disk and close the handler
        match child_entry.entry_type {
            Type::Directory => {
                unistd::unlinkat(
                    Some(parent_node.dir_fd.borrow().as_raw_fd()),
                    &PathBuf::from(child_name),
                    UnlinkatFlags::RemoveDir,
                )
                .unwrap_or_else(|_| {
                    panic!(
                        "unlink_entry() failed to delete the file name {:?} from disk",
                        child_name
                    )
                });
            }
            Type::File => {
                unistd::unlinkat(
                    Some(parent_node.dir_fd.borrow().as_raw_fd()),
                    &PathBuf::from(child_name),
                    UnlinkatFlags::NoRemoveDir,
                )
                .unwrap_or_else(|_| {
                    panic!(
                        "unlink_entry() failed to delete the file name {:?} from disk",
                        child_name
                    )
                });
            }
            _ => panic!(
                "unlink_entry() found unsupported entry type: {:?}",
                child_entry.entry_type
            ),
        }

        child_entry
    }

    fn is_empty(&self) -> bool {
        match self {
            INode::DIR(dir_node) => dir_node.data.borrow().is_empty(),
            INode::FILE(file_node) => file_node.data.borrow().is_empty(),
        }
    }

    fn need_load_data(&self) -> bool {
        if !self.is_empty() {
            debug!(
                "need_load_data() found node data of name={:?} and ino={} is in cache, no need to load",
                self.get_name().as_os_str(),
                self.get_ino(),
            );
            false
        } else if self.get_attr().size > 0 {
            debug!(
                "need_load_data() found node size of name={:?} and ino={} is non-zero, need to load",
                self.get_name().as_os_str(),
                self.get_ino(),
            );
            true
        } else {
            debug!(
                "need_load_data() found node size of name={:?} and ino={} is zero, no need to load",
                self.get_name().as_os_str(),
                self.get_ino(),
            );
            false
        }
    }

    fn read_dir(&self, func: impl FnOnce(&BTreeMap<OsString, DirEntry>)) {
        let dir_node = self.helper_get_dir_node();
        if self.need_load_data() {
            self.helper_load_dir_data();
        }
        func(&dir_node.data.borrow());
    }

    fn read_file(&self, func: impl FnOnce(&Vec<u8>)) {
        let file_node = self.helper_get_file_node();
        if self.need_load_data() {
            self.helper_load_file_data();
        }
        func(&file_node.data.borrow());
    }

    fn write_file(&mut self, fh: u64, offset: i64, data: &[u8], oflags: OFlag) -> usize {
        let file_node = match self {
            INode::DIR(_) => panic!("write_file() cannot write DirNode"),
            INode::FILE(file_node) => file_node,
        };
        let attr = file_node.attr.get_mut();
        let ino = attr.ino;
        let file_data = file_node.data.get_mut();

        let size_after_write = offset as usize + data.len();
        if file_data.capacity() < size_after_write {
            let before_cap = file_data.capacity();
            let extra_space_size = size_after_write - file_data.capacity();
            file_data.reserve(extra_space_size);
            // TODO: handle OOM when reserving
            // let result = file_data.try_reserve(extra_space_size);
            // if result.is_err() {
            //     warn!(
            //         "write cannot reserve enough space, the space size needed is {} byte",
            //         extra_space_size);
            //     reply.error(ENOMEM);
            //     return;
            // }
            debug!(
                "write_file() enlarged the file data vector capacity from {} to {}",
                before_cap,
                file_data.capacity(),
            );
        }
        match file_data.len().cmp(&(offset as usize)) {
            cmp::Ordering::Greater => {
                file_data.truncate(offset as usize);
                debug!(
                    "write() truncated the file of ino={} to size={}",
                    ino, offset
                );
            }
            cmp::Ordering::Less => {
                let zero_padding_size = (offset as usize) - file_data.len();
                let mut zero_padding_vec = vec![0u8; zero_padding_size];
                file_data.append(&mut zero_padding_vec);
            }
            cmp::Ordering::Equal => (),
        }
        file_data.extend_from_slice(data);

        let fcntl_oflags = FcntlArg::F_SETFL(oflags);
        let fd = fh as RawFd;
        fcntl::fcntl(fd, fcntl_oflags).unwrap_or_else(|_| {
            panic!(
                "write_file() failed to set the flags {:?} to file handler {} of ino={}",
                oflags, fd, ino
            )
        });
        let mut written_size = data.len();
        if true {
            // TODO: async write to disk
            written_size = uio::pwrite(fd, data, offset).expect("write() failed to write to disk");
            debug_assert_eq!(data.len(), written_size);
        }
        // update the attribute of the written file
        attr.size = file_data.len() as u64;
        let ts = SystemTime::now();
        attr.mtime = ts;

        written_size
    }

    fn helper_move_file(
        old_parent_inode: &INode,
        old_name: &OsStr,
        new_parent_inode: &INode,
        new_name: &OsStr,
    ) -> nix::Result<()> {
        let old_dir = old_parent_inode.helper_get_dir_node();
        let new_dir = new_parent_inode.helper_get_dir_node();

        debug!(
            "helper_move_file() about to move file of old name={:?}
                from directory {:?} to directory {:?} with new name={:?}",
            old_name,
            old_parent_inode.get_name().as_os_str(),
            new_parent_inode.get_name().as_os_str(),
            new_name,
        );
        fcntl::renameat(
            Some(old_dir.dir_fd.borrow().as_raw_fd()),
            Path::new(old_name),
            Some(new_dir.dir_fd.borrow().as_raw_fd()),
            Path::new(new_name),
        )
    }
}

pub struct MemoryFilesystem {
    // max_ino: AtomicU64,
    cache: BTreeMap<u64, INode>,
    trash: BTreeSet<u64>,
}

impl MemoryFilesystem {
    fn helper_create_node(
        &mut self,
        parent: u64,
        node_name: &OsString,
        mode: u32,
        node_type: Type,
        reply: ReplyEntry,
    ) {
        let node_kind = util::convert_node_type(&node_type);
        // pre-check
        let parent_inode = self.cache.get(&parent).unwrap_or_else(|| {
            panic!(
                "helper_create_node() found fs is inconsistent,
                parent of ino={} should be in cache before create it new child",
                parent
            )
        });
        if let Some(occupied) = parent_inode.get_entry(node_name) {
            debug!(
                "helper_create_node() found the directory of ino={}
                    already exists a child with name {:?} and ino={}",
                parent, node_name, occupied.ino,
            );
            reply.error(EEXIST);
            return;
        }
        // all checks are passed, ready to create new node
        let mflags = util::parse_mode(mode);
        let new_ino: u64;
        let new_inode: INode;
        match node_kind {
            FileType::Directory => {
                debug!(
                    "helper_create_node() about to create a directory with name={:?}, mode={:?}",
                    node_name, mflags,
                );
                new_inode = parent_inode.create_child_dir(node_name, mflags);
            }
            FileType::RegularFile => {
                let oflags = OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_RDWR;
                debug!(
                    "helper_create_node() about to
                        create a file with name={:?}, oflags={:?}, mode={:?}",
                    node_name, oflags, mflags,
                );
                new_inode = parent_inode.create_child_file(node_name, oflags, mflags);
            }
            _ => panic!(
                "helper_create_node() found unsupported file type: {:?}",
                node_kind
            ),
        }
        new_ino = new_inode.get_ino();
        let new_attr = new_inode.get_attr();
        self.cache.insert(new_ino, new_inode);

        let ttl = Duration::new(MY_TTL_SEC, 0);
        reply.entry(&ttl, &new_attr, MY_GENERATION);
        debug!(
            "helper_create_node() successfully created the new child name={:?}
                of ino={} under parent ino={}",
            node_name, new_ino, parent,
        );
    }

    fn helper_get_parent_inode(&self, ino: u64) -> &INode {
        let inode = self.cache.get(&ino).unwrap_or_else(|| {
            panic!(
                "helper_get_parent_inode() failed to find the i-node of ino={}",
                ino
            )
        });
        let parent_ino = inode.get_parent_ino();
        self.cache.get(&parent_ino).unwrap_or_else(|| panic!("helper_get_parent_inode() failed to find the parent of ino={} for i-node of ino={}", parent_ino, ino))
    }

    fn helper_may_deferred_delete_node(&mut self, ino: u64) {
        let parent_ino: u64;
        let mut deferred_deletion = false;
        {
            let inode = self.cache.get(&ino).unwrap_or_else(|| {
                panic!(
                    "helper_may_deferred_delete_node() failed to find the i-node of ino={}",
                    ino
                )
            });

            let parent_inode = self.helper_get_parent_inode(ino);
            parent_ino = parent_inode.get_ino();
            // remove entry from parent i-node
            let deleted_entry = parent_inode.unlink_entry(&inode.get_name());
            debug_assert_eq!(deleted_entry.ino, ino);
            debug_assert_eq!(inode.get_name().as_os_str(), &deleted_entry.name);
            debug_assert!(inode.get_lookup_count() >= 0); // lookup count cannot be negative
            if inode.get_lookup_count() > 0 {
                deferred_deletion = true;
            }
        }

        if deferred_deletion {
            // deferred deletion
            let inode = self.cache.get(&ino).unwrap(); // TODO: support thread-safe
            let insert_result = self.trash.insert(ino);
            debug_assert!(insert_result); // check thread-safe in case of duplicated deferred deletion requests
            debug!(
                "helper_may_deferred_delete_node() defered removed the node name={:?} of ino={}
                    under parent ino={}, open count is: {}, lookup count is : {}",
                inode.get_name().as_os_str(),
                ino,
                parent_ino,
                inode.get_open_count(),
                inode.get_lookup_count(),
            );
        } else {
            // complete deletion
            let inode = self.cache.remove(&ino).unwrap(); // TODO: support thread-safe
            debug!(
                "helper_may_deferred_delete_node() successfully removed the node name={:?} of ino={}
                    under parent ino={}, open count is: {}, lookup count is : {}",
                inode.get_name().as_os_str(),
                ino,
                parent_ino,
                inode.get_open_count(),
                inode.get_lookup_count(),
            );
        }
    }

    fn helper_remove_node(
        &mut self,
        parent: u64,
        node_name: &OsString,
        node_type: Type,
        reply: ReplyEmpty,
    ) {
        let node_kind = util::convert_node_type(&node_type);
        let node_ino: u64;
        {
            // pre-checks
            let parent_inode = self.cache.get(&parent).unwrap_or_else(|| {
                panic!(
                    "helper_remove_node() found fs is inconsistent,
                    parent of ino={} should be in cache before remove its child",
                    parent
                )
            });
            match parent_inode.get_entry(node_name) {
                None => {
                    debug!(
                        "helper_remove_node() failed to find node name={:?}
                            under parent of ino={}",
                        node_name, parent,
                    );
                    reply.error(ENOENT);
                    return;
                }
                Some(child_entry) => {
                    node_ino = child_entry.ino;
                    if let FileType::Directory = node_kind {
                        // check the directory to delete is empty
                        let dir_inode = self.cache.get(&node_ino).unwrap_or_else(|| {
                            panic!(
                                "helper_remove_node() found fs is inconsistent,
                                directory name={:?} of ino={} found under the parent of ino={},
                                but no i-node found for this directory",
                                node_name, node_ino, parent
                            )
                        });
                        if !dir_inode.is_empty() {
                            debug!(
                                "helper_remove_node() cannot remove
                                    the non-empty directory name={:?} of ino={}
                                    under the parent directory of ino={}",
                                node_name, node_ino, parent,
                            );
                            reply.error(ENOTEMPTY);
                            return;
                        }
                    }

                    let child_inode = self.cache.get(&node_ino).unwrap_or_else(|| panic!("helper_remove_node() found fs is inconsistent, node name={:?} of ino={}
                            found under the parent of ino={}, but no i-node found for this node", node_name, node_ino, parent));
                    debug_assert_eq!(node_ino, child_inode.get_ino());
                    debug_assert_eq!(node_name, child_inode.get_name().as_os_str());
                    debug_assert_eq!(parent, child_inode.get_parent_ino());
                    debug_assert_eq!(node_type, child_inode.get_type());
                    debug_assert_eq!(node_kind, child_inode.get_attr().kind);
                }
            }
        }
        {
            // all checks passed, ready to remove,
            // when deferred deletion, remove entry from directory first
            self.helper_may_deferred_delete_node(node_ino);
            reply.ok();
        }
    }

    pub fn new<P: AsRef<Path>>(mount_point: P) -> MemoryFilesystem {
        let mount_dir = PathBuf::from(mount_point.as_ref());
        if !mount_dir.is_dir() {
            panic!("the input mount path is not a directory");
        }
        let root_path = fs::canonicalize(&mount_dir).unwrap_or_else(|_| {
            panic!(
                "failed to convert the mount point {:?} to a full path",
                mount_dir
            )
        });

        let root_inode = INode::open_root_inode(FUSE_ROOT_ID, OsString::from("/"), &root_path);
        let mut cache = BTreeMap::new();
        cache.insert(FUSE_ROOT_ID, root_inode);
        let trash = BTreeSet::new(); // for deferred deletion

        MemoryFilesystem { cache, trash }
    }
}

impl Filesystem for MemoryFilesystem {
    fn init(&mut self, _req: &Request<'_>) -> Result<(), c_int> {
        // TODO:
        Ok(())
    }

    fn getattr(&mut self, req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        debug!("getattr(ino={}, req={:?})", ino, req.request);

        let inode = self.cache.get(&ino).unwrap_or_else(|| {
            panic!(
                "getattr() found fs is inconsistent, the i-node of ino={} should be in cache",
                ino
            )
        });
        let attr = inode.get_attr();
        debug!(
            "getattr() cache hit when searching the attribute of ino={}",
            ino,
        );
        let ttl = Duration::new(MY_TTL_SEC, 0);
        reply.attr(&ttl, &attr);
        debug!(
            "getattr() successfully got the attribute of ino={}, the attr is: {:?}",
            ino, &attr,
        );
    }

    // The order of calls is:
    //     init
    //     ...
    //     opendir
    //     readdir
    //     releasedir
    //     open
    //     read
    //     write
    //     ...
    //     flush
    //     release
    //     ...
    //     destroy
    fn open(&mut self, req: &Request<'_>, ino: u64, flags: u32, reply: ReplyOpen) {
        debug!("open(ino={}, flags={}, req={:?})", ino, flags, req.request,);
        let inode = self.cache.get(&ino).unwrap_or_else(|| {
            panic!(
                "open() found fs is inconsistent, the i-node of ino={} should be in cache",
                ino
            )
        });
        let oflags = util::parse_oflag(flags);
        let new_fd = inode.dup_fd(oflags);
        reply.opened(new_fd as u64, flags);
        debug!(
            "open() successfully duplicated the file handler of ino={}, fd={}, flags: {:?}",
            ino, new_fd, flags,
        );
    }

    fn release(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        fh: u64,
        flags: u32,
        lock_owner: u64,
        flush: bool,
        reply: ReplyEmpty,
    ) {
        debug!(
            "release(ino={}, fh={}, flags={}, lock_owner={}, flush={}, req={:?})",
            ino, fh, flags, lock_owner, flush, req.request,
        );
        let inode = self.cache.get(&ino).unwrap_or_else(|| {
            panic!(
                "release() found fs is inconsistent, the i-node of ino={} should be in cache",
                ino
            )
        });
        if flush {
            // TODO: support flush
        }

        // close the duplicated dir fd
        unistd::close(fh as RawFd).unwrap_or_else(|_| {
            panic!(
                "release() failed to close the file handler {} of ino={}",
                fh, ino
            )
        });
        reply.ok();
        inode.dec_open_count();
        debug!(
            "release() successfully closed the file handler {} of ino={}",
            fh, ino,
        );
    }

    fn opendir(&mut self, req: &Request<'_>, ino: u64, flags: u32, reply: ReplyOpen) {
        debug!(
            "opendir(ino={}, flags={}, req={:?})",
            ino, flags, req.request,
        );

        let inode = self.cache.get(&ino).unwrap_or_else(|| {
            panic!(
                "opendir() found fs is inconsistent, the i-node of ino={} should be in cache",
                ino
            )
        });
        let oflags = util::parse_oflag(flags);
        let new_fd = inode.dup_fd(oflags);

        reply.opened(new_fd as u64, flags);
        debug!(
            "opendir() successfully duplicated the file handler of ino={}, new fd={}, flags: {:?}",
            ino, new_fd, oflags,
        );
    }

    fn releasedir(&mut self, req: &Request<'_>, ino: u64, fh: u64, flags: u32, reply: ReplyEmpty) {
        debug!(
            "releasedir(ino={}, fh={}, flags={}, req={:?})",
            ino, fh, flags, req.request,
        );
        let inode = self.cache.get(&ino).unwrap_or_else(|| {
            panic!(
                "releasedir() found fs is inconsistent, the i-node of ino={} should be in cache",
                ino
            )
        });
        // close the duplicated dir fd
        unistd::close(fh as RawFd).unwrap_or_else(|_| {
            panic!(
                "releasedir() failed to close the file handler {} of ino={}",
                fh, ino
            )
        });
        reply.ok();
        inode.dec_open_count();
        debug!(
            "releasedir() successfully closed the file handler {} of ino={}",
            fh, ino,
        );
    }

    fn read(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        reply: ReplyData,
    ) {
        debug!(
            "read(ino={}, fh={}, offset={}, size={}, req={:?})",
            ino, fh, offset, size, req.request,
        );

        let read_helper = |content: &Vec<u8>| {
            if (offset as usize) < content.len() {
                let read_data = if ((offset + size as i64) as usize) < content.len() {
                    &content[(offset as usize)..(offset + size as i64) as usize]
                } else {
                    &content[(offset as usize)..]
                };
                debug!(
                    "read() successfully from the file of ino={}, the read size is: {:?}",
                    ino,
                    read_data.len(),
                );
                reply.data(read_data);
            } else {
                debug!(
                    "read() offset={} is beyond the length of the file of ino={}",
                    offset, ino
                );
                reply.error(EINVAL);
            }
        };

        let inode = self.cache.get(&ino).unwrap_or_else(|| {
            panic!(
                "read() found fs is inconsistent, the i-node of ino={} should be in cache",
                ino
            )
        });
        inode.read_file(read_helper);
    }

    fn readdir(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        debug!(
            "readdir(ino={}, fh={}, offset={}, req={:?})",
            ino, fh, offset, req.request,
        );

        let readdir_helper = |data: &BTreeMap<OsString, DirEntry>| {
            let mut num_child_entries = 0;
            for (i, (child_name, child_entry)) in data.iter().enumerate().skip(offset as usize) {
                let child_ino = child_entry.ino;
                reply.add(
                    child_ino,
                    offset + i as i64 + 1, // i + 1 means the index of the next entry
                    util::convert_node_type(&child_entry.entry_type),
                    child_name,
                );
                num_child_entries += 1;
                debug!(
                    "readdir() found one child name={:?} ino={} offset={} entry={:?}
                        under the directory of ino={}",
                    child_name,
                    child_ino,
                    offset + i as i64 + 1,
                    child_entry,
                    ino,
                );
            }
            debug!(
                "readdir() successfully read {} children under the directory of ino={},
                    the reply is: {:?}",
                num_child_entries, ino, &reply,
            );
            reply.ok();
        };

        let inode = self.cache.get(&ino).unwrap_or_else(|| {
            panic!(
                "readdir() found fs is inconsistent, the i-node of ino={} should be in cache",
                ino
            )
        });
        inode.read_dir(readdir_helper);
    }

    fn lookup(&mut self, req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let child_name = OsString::from(name);
        debug!(
            "lookup(parent={}, name={:?}, req={:?})",
            parent, child_name, req.request,
        );

        let ino: u64;
        let child_type: FileType;
        {
            // lookup child ino and type first
            let parent_inode = self.cache.get(&parent).unwrap_or_else(|| {
                panic!(
                    "lookup() found fs is inconsistent,
                    the parent i-node of ino={} should be in cache",
                    parent
                )
            });
            match parent_inode.get_entry(&child_name) {
                Some(child_entry) => {
                    ino = child_entry.ino;
                    child_type = util::convert_node_type(&child_entry.entry_type);
                }
                None => {
                    reply.error(ENOENT);
                    debug!(
                        "lookup() failed to find the file name={:?} under parent directory of ino={}",
                        child_name, parent
                    );
                    return;
                }
            }
        }

        let lookup_helper = |attr: &FileAttr| {
            let ttl = Duration::new(MY_TTL_SEC, 0);
            reply.entry(&ttl, &attr, MY_GENERATION);
            debug!(
                "lookup() successfully found the file name={:?} of ino={}
                    under parent ino={}, the attr is: {:?}",
                child_name, ino, parent, &attr,
            );
        };

        {
            // cache hit
            if let Some(inode) = self.cache.get(&ino) {
                debug!(
                    "lookup() cache hit when searching file of name={:?} and ino={} under parent ino={}",
                    child_name, ino, parent,
                );
                inode.lookup_attr(lookup_helper);
                return;
            }
        }
        {
            // cache miss
            debug!(
                "lookup() cache missed when searching parent ino={}
                    and file name={:?} of ino={}",
                parent, child_name, ino,
            );
            let parent_inode = self.cache.get(&parent).unwrap_or_else(|| {
                panic!(
                    "lookup() found fs is inconsistent, parent i-node of ino={} should be in cache",
                    parent
                )
            });
            let child_inode: INode;
            match child_type {
                FileType::Directory => {
                    child_inode = parent_inode.open_child_dir(&child_name);
                }
                FileType::RegularFile => {
                    let oflags = OFlag::O_RDONLY;
                    child_inode = parent_inode.open_child_file(&child_name, oflags);
                }
                _ => panic!("lookup() found unsupported file type: {:?}", child_type),
            };

            let child_ino = child_inode.get_ino();
            child_inode.lookup_attr(lookup_helper);
            self.cache.insert(child_ino, child_inode);
        }
    }

    fn forget(&mut self, req: &Request<'_>, ino: u64, nlookup: u64) {
        debug!(
            "forget(ino={}, nlookup={}, req={:?})",
            ino, nlookup, req.request,
        );
        let current_count: i64;
        {
            let inode = self.cache.get(&ino).unwrap_or_else(|| {
                panic!(
                    "forget() found fs is inconsistent, the i-node of ino={} should be in cache",
                    ino
                )
            });
            let previous_count = inode.dec_lookup_count_by(nlookup);
            current_count = inode.get_lookup_count();
            debug_assert!(current_count >= 0);
            debug_assert_eq!(previous_count - current_count, nlookup as i64); // assert thread-safe
            debug!(
                "forget() successfully reduced lookup count of ino={} from {} to {}",
                ino, previous_count, current_count,
            );
        }
        {
            if current_count == 0 {
                // TODO: support thread-safe
                if self.trash.contains(&ino) {
                    // deferred deletion
                    let deleted_inode = self.cache.remove(&ino).unwrap_or_else(|| {
                        panic!(
                            "forget() found fs is inconsistent, node of ino={}
                            found in trash, but no i-node found for deferred deletion",
                            ino
                        )
                    });
                    self.trash.remove(&ino);
                    debug_assert_eq!(deleted_inode.get_lookup_count(), 0);
                    debug!(
                        "forget() deferred deleted i-node of ino={}, the i-node is: {:?}",
                        ino, deleted_inode
                    );
                }
            }
        }
    }
    // Begin non-read functions

    /// called by the VFS to set attributes for a file. This method
    /// is called by chmod(2) and related system calls.
    fn setattr(
        &mut self,
        req: &Request<'_>,
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
            "setattr(ino={}, mode={:?}, uid={:?}, gid={:?}, size={:?},
                atime={:?}, mtime={:?}, fh={:?}, crtime={:?}, chgtime={:?},
                bkuptime={:?}, flags={:?}, req={:?})",
            ino,
            mode,
            uid,
            gid,
            size,
            atime,
            mtime,
            fh,
            crtime,
            chgtime,
            bkuptime,
            flags,
            req.request,
        );

        let setattr_helper = |attr: &mut FileAttr| {
            let ttl = Duration::new(MY_TTL_SEC, 0);
            let ts = SystemTime::now();

            if let Some(b) = mode {
                attr.perm = util::parse_mode_bits(b);
                debug!("setattr set permission as: {}", attr.perm);

                let sflag = util::parse_sflag(b);
                let kind = util::convert_sflag(sflag);
                debug_assert_eq!(kind, attr.kind);
            }
            // no replace
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
                error!(
                    "setattr found all the input attributes are empty for the file of ino={}",
                    ino,
                );
            }
        };

        let inode = self.cache.get_mut(&ino).unwrap_or_else(|| {
            panic!(
                "setattr() found fs is inconsistent, the i-node of ino={} should be in cache",
                ino
            )
        });
        inode.set_attr(setattr_helper);
        // TODO: write attribute to disk
    }

    fn mknod(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        let file_name = OsString::from(name);
        debug!(
            "mknod(parent={}, name={:?}, mode={}, rdev={}, req={:?})",
            parent, file_name, mode, rdev, req.request,
        );

        self.helper_create_node(parent, &file_name, mode, Type::File, reply);
    }

    fn unlink(&mut self, req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let file_name = OsString::from(name);
        debug!(
            "unlink(parent={}, name={:?}, req={:?}",
            parent, file_name, req.request,
        );
        self.helper_remove_node(parent, &file_name, Type::File, reply);
    }

    fn mkdir(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        reply: ReplyEntry,
    ) {
        let dir_name = OsString::from(name);
        debug!(
            "mkdir(parent={}, name={:?}, mode={}, req={:?})",
            parent, dir_name, mode, req.request,
        );

        self.helper_create_node(parent, &dir_name, mode, Type::Directory, reply);
    }

    fn rmdir(&mut self, req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let dir_name = OsString::from(name);
        debug!(
            "rmdir(parent={}, name={:?}, req={:?})",
            parent, dir_name, req.request,
        );
        self.helper_remove_node(parent, &dir_name, Type::Directory, reply);
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        flags: u32,
        reply: ReplyWrite,
    ) {
        debug!(
            "write(ino={}, fh={}, offset={}, data-size={}, flags={})",
            // "write(ino={}, fh={}, offset={}, data-size={}, req={:?})",
            ino,
            fh,
            offset,
            data.len(),
            flags,
            // req.request,
        );

        let inode = self.cache.get_mut(&ino).unwrap_or_else(|| {
            panic!(
                "write() found fs is inconsistent, the i-node of ino={} should be in cache",
                ino
            )
        });
        let oflags = util::parse_oflag(flags);
        let written_size = inode.write_file(fh, offset, data, oflags);
        reply.written(written_size as u32);
        debug!(
            "write() successfully wrote {} byte data to file ino={} at offset={},
                the first at most 100 byte data are: {:?}",
            data.len(),
            ino,
            offset,
            if data.len() > 100 {
                &data[0..100]
            } else {
                data
            },
        );
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
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        new_parent: u64,
        newname: &OsStr,
        reply: ReplyEmpty,
    ) {
        let (old_name, new_name) = (OsString::from(name), OsString::from(newname));
        debug!(
            "rename(old parent={}, old name={:?}, new parent={}, new name={:?}, req={:?})",
            parent, old_name, new_parent, new_name, req.request,
        );

        // let old_entry_ino: u64;
        // let mut need_to_replace = false;
        // let mut replaced_node_ino: u64 = 0;
        {
            // pre-check
            let parent_inode = self.cache.get(&parent).unwrap_or_else(|| {
                panic!(
                    "rename() found fs is inconsistent, parent i-node of ino={} should be in cache",
                    new_parent
                )
            });
            match parent_inode.get_entry(&old_name) {
                None => {
                    reply.error(ENOENT);
                    debug!(
                        "rename() failed to find child entry of name={:?} under parent directory ino={}",
                        old_name, parent,
                    );
                    return;
                }
                Some(old_entry) => {
                    // check the i-node to rename in cache
                    if self.cache.get(&old_entry.ino).is_none() {
                        panic!(
                            "rename() found fs is inconsistent, the i-node of name={:?} and ino={} to rename should be in cache",
                            old_name, old_entry.ino,
                        );
                        // return;
                    }
                }
            }

            let new_parent_inode = self.cache.get(&new_parent).unwrap_or_else(|| panic!("rename() found fs is inconsistent, new parent i-node of ino={} should be in cache", new_parent));
            if let Some(replace_entry) = new_parent_inode.get_entry(&new_name) {
                debug_assert_eq!(&new_name, &replace_entry.name);
                // replaced_node_ino = replace_entry.ino;
                // need_to_replace = true;
                // debug!(
                //     "rename() found the new parent directory of ino={} already has a child with name={:?}",
                //     new_parent, new_name,
                // );
                reply.error(EEXIST); // RENAME_NOREPLACE
                debug!(
                    "rename() found the new parent directory of ino={} already has a child with name={:?}",
                    new_parent, new_name,
                );
                return;
            }
        }

        // all checks passed, ready to rename
        {
            // TODO: support thread-safe
            let parent_inode = self.cache.get(&parent).unwrap();
            let new_parent_inode = self.cache.get(&new_parent).unwrap();

            let old_entry = parent_inode.get_entry(&old_name).unwrap();
            let child_inode = self.cache.get(&old_entry.ino).unwrap();
            child_inode.set_parent_ino(new_parent_inode.get_ino());
            child_inode.set_name(new_name.clone());

            let mut child_entry = parent_inode.remove_entry(&old_name);
            child_entry.name = new_name;
            let replaced_result = new_parent_inode.insert_entry(child_entry);
            debug_assert!(replaced_result.is_none());
            // if need_to_replace {
            //     debug_assert!(replaced_result.is_some());
            //     let replaced_entry = replaced_result.unwrap();
            //     debug_assert_eq!(replaced_entry.ino, replaced_node_ino);
            //     debug_assert_eq!(new_name, replaced_entry.name);
            // } else {
            // move child on disk
            INode::helper_move_file(&parent_inode, &old_name, &new_parent_inode, newname).unwrap_or_else(|_| panic!("rename() failed to move the old file name={:?} of ino={} under old parent ino={}
                    to the new file name={:?} under new parent ino={}", old_name, old_entry.ino, parent, newname, new_parent));
            debug!(
                "rename() moved on disk the old file name={:?} of ino={} under old parent ino={}
                    to the new file name={:?} ino={} under new parent ino={}",
                old_name, old_entry.ino, parent, newname, old_entry.ino, new_parent,
            );

            let child_attr = child_inode.helper_reload_attribute();
            debug_assert_eq!(child_attr.ino, child_inode.get_ino());
            debug_assert_eq!(child_attr.ino, old_entry.ino);

            debug!(
                "rename() successfully moved the old file name={:?} of ino={} under old parent ino={}
                    to the new file name={:?} ino={} under new parent ino={}",
                old_name, old_entry.ino, parent, newname, old_entry.ino, new_parent,
            );
            reply.ok();
        }
        // if need_to_replace {
        //     debug_assert_ne!(replaced_node_ino, 0);
        //     self.helper_may_deferred_delete_node(replaced_node_ino);
        //     debug!(
        //         "rename() successfully moved the old file name={:?} of ino={} under old parent ino={}
        //             to replace the new file name={:?} ino={} under new parent ino={}",
        //         old_name, old_entry_ino, parent, newname, replaced_node_ino, new_parent,
        //     );
        // } else {
    }
}

mod test {
    #[test]
    fn test_libc_renameat() {
        use nix::dir::Dir;
        use nix::fcntl::OFlag;
        use nix::sys::stat::Mode;
        use std::ffi::CString;
        use std::fs;
        use std::os::unix::io::AsRawFd;
        use std::path::Path;

        const DEFAULT_MOUNT_DIR: &str = "/tmp/fuse_test";
        const FILE_CONTENT: &str = "0123456789ABCDEF";
        let mount_dir = Path::new(DEFAULT_MOUNT_DIR);
        if !mount_dir.exists() {
            fs::create_dir(&mount_dir).unwrap();
        }

        let from_dir = Path::new(&mount_dir).join("from_dir");
        if from_dir.exists() {
            fs::remove_dir_all(&from_dir).unwrap();
        }
        fs::create_dir(&from_dir).unwrap();
        let to_dir = Path::new(&mount_dir).join("to_dir");
        if to_dir.exists() {
            fs::remove_dir_all(&to_dir).unwrap();
        }
        fs::create_dir(&to_dir).unwrap();
        let old_name = "old.txt";
        let old_file = from_dir.join(old_name);
        fs::write(&old_file, FILE_CONTENT).unwrap();
        let new_name = "new.txt";
        let new_file = to_dir.join(new_name);

        let oflags = OFlag::O_RDONLY | OFlag::O_DIRECTORY;
        let old_dir_fd = Dir::open(&from_dir, oflags, Mode::empty()).unwrap();
        let new_dir_fd = Dir::open(&to_dir, oflags, Mode::empty()).unwrap();
        let old_cstr = CString::new(old_name).unwrap();
        let new_cstr = CString::new(new_name).unwrap();
        let res = unsafe {
            libc::renameat(
                old_dir_fd.as_raw_fd(),
                old_cstr.as_ptr(),
                new_dir_fd.as_raw_fd(),
                new_cstr.as_ptr(),
            )
        };
        // fs::rename(&old_file, &new_file).unwrap();
        assert_eq!(res, 0);
        let bytes = fs::read(&new_file).unwrap();
        let content = String::from_utf8(bytes).unwrap();
        assert_eq!(content, FILE_CONTENT);
        assert!(!old_file.exists());
        assert!(new_file.exists());
        fs::remove_dir_all(&from_dir).unwrap();
        assert!(!from_dir.exists());
        fs::remove_dir_all(&to_dir).unwrap();
        assert!(!to_dir.exists());
        fs::remove_dir_all(&mount_dir).unwrap();
        assert!(!mount_dir.exists());
    }
}
