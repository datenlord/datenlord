use async_trait::async_trait;
use bytes::BytesMut;
use datenlord::common::error::DatenLordResult;
use datenlord::fs::datenlordfs::direntry::DirEntry;
use datenlord::fs::fs_util::{CreateParam, FileAttr, INum, RenameParam, SetAttrParam, StatFsParam};
use datenlord::fs::virtualfs::VirtualFs;
use opendal::services::Fs;
use opendal::Operator;
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::time::{Duration, SystemTime};
use std::path::Path;

#[derive(Debug)]
pub struct LocalFS {
    operator: Operator,
}

impl LocalFS {
    pub fn new() -> DatenLordResult<Self> {
        let mut builder = Fs::default();
        builder.root("/tmp");
        let op = Operator::new(builder).unwrap().finish();
        Ok(Self { operator: op })
    }

    fn fileattr_from_metadata(metadata: opendal::Metadata, ino: u64) -> FileAttr {
        let kind = if metadata.is_file() {
            nix::sys::stat::SFlag::S_IFREG
        } else if metadata.is_dir() {
            nix::sys::stat::SFlag::S_IFDIR
        } else {
            nix::sys::stat::SFlag::S_IFLNK
        };
        let size = metadata.content_length();
        let atime = metadata.last_modified().unwrap_or(SystemTime::now().into()).into();
        let mtime = metadata.last_modified().unwrap_or(SystemTime::now().into()).into();

        FileAttr {
            ino: ino as INum,
            size,
            blocks: size / 512,
            atime,
            mtime,
            ctime: mtime,
            kind,
            perm: 0o775,
            nlink: 1,
            uid: 1000,
            gid: 1000,
            rdev: 0,
        }
    }

    fn fileattr_from_local_metadata(metadata: fs::Metadata, ino: u64) -> FileAttr {
        let kind = if metadata.is_file() {
            nix::sys::stat::SFlag::S_IFREG
        } else if metadata.is_dir() {
            nix::sys::stat::SFlag::S_IFDIR
        } else {
            nix::sys::stat::SFlag::S_IFLNK
        };
        let size = metadata.len();
        let atime = metadata.modified().unwrap_or(SystemTime::now().into()).into();

        return FileAttr {
            ino: ino as INum,
            size,
            blocks: size / 512,
            atime,
            mtime: atime,
            ctime: atime,
            kind,
            perm: 0o775,
            nlink: 1,
            uid: 1000,
            gid: 1000,
            rdev: 0,
        };
    }
}

#[async_trait]
impl VirtualFs for LocalFS {
    async fn lookup(
        &self,
        _uid: u32,
        _gid: u32,
        _parent: INum,
        name: &str,
    ) -> DatenLordResult<(Duration, FileAttr, u64)> {
        let path = format!("/tmp/{}", name);
        let local_metadata = fs::metadata(path).unwrap();
        let ino = local_metadata.ino();
        let metadata = Self::fileattr_from_local_metadata(local_metadata, ino);
        Ok((Duration::from_secs(1), metadata, 0))
    }

    async fn getattr(&self, ino: u64) -> DatenLordResult<(Duration, FileAttr)> {
        Ok((Duration::from_secs(1), FileAttr::default()))
    }

    async fn setattr(
        &self,
        _uid: u32,
        _gid: u32,
        ino: u64,
        param: SetAttrParam,
    ) -> DatenLordResult<(Duration, FileAttr)> {
        Ok((Duration::from_secs(1), FileAttr::default()))
    }

    async fn readlink(&self, ino: u64) -> DatenLordResult<Vec<u8>> {
        Ok(Vec::new())
    }

    async fn open(&self, _uid: u32, _gid: u32, ino: u64, _flags: u32) -> DatenLordResult<u64> {
        Ok(0)
    }

    async fn read(
        &self,
        ino: u64,
        fh: u64,
        offset: u64,
        size: u32,
        buf: &mut BytesMut,
    ) -> DatenLordResult<usize> {
        // fill buf with size bytes from data
        let data = vec![0; size as usize];
        buf.copy_from_slice(&data);
        Ok(data.len())
    }

    async fn write(
        &self,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _flags: u32,
    ) -> DatenLordResult<()> {
        Ok(())
    }

    async fn unlink(&self, _uid: u32, _gid: u32, _parent: INum, name: &str) -> DatenLordResult<()> {
        Ok(())
    }

    async fn mkdir(&self, param: CreateParam) -> DatenLordResult<(Duration, FileAttr, u64)> {
        let path = format!("/tmp/{}", param.name);
        // self.operator.create_dir(&path).await.unwrap();

        let attr = FileAttr {
            ino: param.parent,
            size: 0,
            blocks: 0,
            atime: SystemTime::now(),
            mtime: SystemTime::now(),
            ctime: SystemTime::now(),
            kind: param.node_type,
            perm: param.mode as u16,
            nlink: 2,
            uid: param.uid,
            gid: param.gid,
            rdev: 0,
        };

        Ok((Duration::from_secs(1), attr, param.parent))
    }

    async fn rename(&self, _uid: u32, _gid: u32, param: RenameParam) -> DatenLordResult<()> {
        Ok(())
    }

    async fn release(
        &self,
        ino: u64,
        fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
    ) -> DatenLordResult<()> {
        Ok(())
    }

    async fn statfs(&self, _uid: u32, _gid: u32, ino: u64) -> DatenLordResult<StatFsParam> {
        Ok(StatFsParam{
            blocks: 0,
            bfree: 0,
            bavail: 0,
            files: 0,
            f_free: 0,
            bsize: 4096,
            namelen: 255,
            frsize: 4096,
        })
    }

    async fn fsync(&self, _ino: u64, _fh: u64, _datasync: bool) -> DatenLordResult<()> {
        Ok(())
    }

    async fn flush(&self, _ino: u64, _fh: u64, _lock_owner: u64) -> DatenLordResult<()> {
        Ok(())
    }

    async fn symlink(
        &self,
        _uid: u32,
        _gid: u32,
        _parent: INum,
        _name: &str,
        _target_path: &Path,
    ) -> DatenLordResult<(Duration, FileAttr, u64)> {
        Ok((Duration::from_secs(1), FileAttr::default(), 0))
    }

    async fn readdir(
        &self,
        uid: u32,
        gid: u32,
        ino: u64,
        fh: u64,
        offset: i64,
    ) -> DatenLordResult<Vec<DirEntry>> {
        Ok(Vec::new())
    }

    async fn rmdir(
        &self,
        uid: u32,
        gid: u32,
        parent: INum,
        dir_name: &str,
    ) -> DatenLordResult<Option<INum>> {
        Ok(None)
    }

    async fn link(&self, _newparent: u64, _newname: &str) -> DatenLordResult<()> {
        Ok(())
    }

    async fn forget(&self, _ino: u64, _nlookup: u64) {
    }

    async fn mknod(&self, _param: CreateParam) -> DatenLordResult<(Duration, FileAttr, u64)> {
        Ok((Duration::from_secs(1), FileAttr::default(), 0))
    }

    async fn opendir(&self, _uid: u32, _gid: u32, ino: u64, _flags: u32) -> DatenLordResult<u64> {
        Ok(0)
    }

    async fn releasedir(&self, _ino: u64, _fh: u64, _flags: u32) -> DatenLordResult<()> {
        Ok(())
    }

    async fn fsyncdir(&self, _ino: u64, _fh: u64, _datasync: bool) -> DatenLordResult<()> {
        Ok(())
    }
}
