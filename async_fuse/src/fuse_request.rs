use anyhow::Context;
use std::ffi::OsStr;
use std::fmt;
use std::mem;
use std::os::unix::ffi::OsStrExt;

use super::protocol::*;

pub(crate) struct ByteSlice<'a> {
    data: &'a [u8],
}

impl<'a> ByteSlice<'a> {
    pub fn new(data: &'a [u8]) -> ByteSlice<'a> {
        ByteSlice { data }
    }
    pub fn len(&self) -> usize {
        self.data.len()
    }
    pub fn fetch_all(&mut self) -> &'a [u8] {
        let bytes = self.data;
        self.data = &[];
        bytes
    }
    pub fn fetch_bytes(&mut self, amt: usize) -> anyhow::Result<&'a [u8]> {
        if amt > self.data.len() {
            return Err(anyhow::anyhow!(
                "no enough bytes to fetch, remaining {} bytes but to fetch {} bytes",
                self.data.len(),
                amt,
            ));
        }
        let bytes = &self.data[..amt];
        self.data = &self.data[amt..];
        Ok(bytes)
    }
    pub fn fetch<T>(&mut self) -> anyhow::Result<&'a T> {
        let len = mem::size_of::<T>();
        let bytes = self.fetch_bytes(len).context(format!(
            "failed to build FUSE request payload type {}",
            std::any::type_name::<T>(),
        ))?;
        let ret = unsafe { (bytes.as_ptr() as *const T).as_ref() };
        match ret {
            Some(obj) => Ok(obj),
            None => Err(anyhow::anyhow!(
                "failed to convert bytes to type={}",
                std::any::type_name::<T>()
            )),
        }
    }
    pub fn fetch_slice<T>(&mut self) -> anyhow::Result<Vec<&'a T>> {
        let elem_len = mem::size_of::<T>();
        if self.len() % elem_len != 0 {
            return Err(anyhow::anyhow!(
                "failed to convert bytes to a slice of type={}, \
                 the total bytes length={} % the type size={} is nonzero",
                std::any::type_name::<T>(),
                self.len(),
                elem_len,
            ));
        }
        let size = self.len() / elem_len;
        let mut result_slice = Vec::with_capacity(size);
        while self.len() > 0 {
            let elem = self.fetch()?;
            result_slice.push(elem)
        }
        Ok(result_slice)
    }
    pub fn fetch_str(&mut self) -> anyhow::Result<&'a OsStr> {
        let len = match self.data.iter().position(|&c| c == 0) {
            Some(pos) => pos,
            None => {
                return Err(anyhow::anyhow!(
                    "no trailing zero in bytes, cannot fetch c-string"
                ))
            }
        };
        let bytes = self.fetch_bytes(len)?;
        let _zero = self.fetch_bytes(1)?; // fetch the trailing zero of c-str
        Ok(OsStr::from_bytes(&bytes))
    }
}

#[derive(Debug)]
pub(crate) enum Operation<'a> {
    Lookup {
        // FUSE_LOOKUP = 1
        name: &'a OsStr,
    },
    Forget {
        // FUSE_FORGET = 2
        arg: &'a FuseForgetIn,
    },
    GetAttr, // FUSE_GETATTR = 3
    SetAttr {
        // FUSE_SETATTR = 4
        arg: &'a FuseSetAttrIn,
    },
    ReadLink, // FUSE_READLINK = 5
    SymLink {
        // FUSE_SYMLINK = 6
        name: &'a OsStr,
        link: &'a OsStr,
    },
    MkNod {
        // FUSE_MKNOD = 8
        arg: &'a FuseMkNodIn,
        name: &'a OsStr,
    },
    MkDir {
        // FUSE_MKDIR = 9
        arg: &'a FuseMkDirIn,
        name: &'a OsStr,
    },
    Unlink {
        // FUSE_UNLINK = 10
        name: &'a OsStr,
    },
    RmDir {
        // FUSE_RMDIR = 11
        name: &'a OsStr,
    },
    #[cfg(target_os = "macos")]
    Rename {
        // FUSE_RENAME = 12
        arg: &'a FuseRenameIn,
        oldname: &'a OsStr,
        newname: &'a OsStr,
    },
    #[cfg(target_os = "linux")]
    Rename {
        // FUSE_RENAME = 12
        arg: &'a FuseRenameIn, // TODO: verify abi protocal
        oldname: &'a OsStr,
        newname: &'a OsStr,
    },
    Link {
        // FUSE_LINK = 13
        arg: &'a FuseLinkIn,
        name: &'a OsStr,
    },
    Open {
        // FUSE_OPEN = 14
        arg: &'a FuseOpenIn,
    },
    Read {
        // FUSE_READ = 15
        arg: &'a FuseReadIn,
    },
    Write {
        // FUSE_WRITE = 16
        arg: &'a FuseWriteIn,
        data: &'a [u8],
    },
    StatFs, // FUSE_STATFS = 17
    Release {
        // FUSE_RELEASE = 18
        arg: &'a FuseReleaseIn,
    },
    FSync {
        // FUSE_FSYNC = 20
        arg: &'a FuseFSyncIn,
    },
    SetXAttr {
        // FUSE_SETXATTR = 21
        arg: &'a FuseSetXAttrIn,
        name: &'a OsStr,
        value: &'a [u8],
    },
    GetXAttr {
        // FUSE_GETXATTR = 22
        arg: &'a FuseGetXAttrIn,
        name: &'a OsStr,
    },
    ListXAttr {
        // FUSE_LISTXATTR = 23
        arg: &'a FuseGetXAttrIn,
    },
    RemoveXAttr {
        // FUSE_REMOVEXATTR = 24
        name: &'a OsStr,
    },
    Flush {
        // FUSE_FLUSH = 25
        arg: &'a FuseFlushIn,
    },
    Init {
        // FUSE_INIT = 26
        arg: &'a FuseInitIn,
    },
    OpenDir {
        // FUSE_OPENDIR = 27
        arg: &'a FuseOpenIn,
    },
    ReadDir {
        // FUSE_READDIR = 28
        arg: &'a FuseReadIn,
    },
    ReleaseDir {
        // FUSE_RELEASEDIR = 29
        arg: &'a FuseReleaseIn,
    },
    FSyncDir {
        // FUSE_FSYNCDIR = 30
        arg: &'a FuseFSyncIn,
    },
    GetLk {
        // FUSE_GETLK = 31
        arg: &'a FuseLockIn,
    },
    SetLk {
        // FUSE_SETLK = 32
        arg: &'a FuseLockIn,
    },
    SetLkW {
        // FUSE_SETLKW = 33
        arg: &'a FuseLockIn,
    },
    Access {
        // FUSE_ACCESS = 34
        arg: &'a FuseAccessIn,
    },
    Create {
        // FUSE_CREATE = 35
        arg: &'a FuseCreateIn,
        name: &'a OsStr,
    },
    Interrupt {
        // FUSE_INTERRUPT = 36
        arg: &'a FuseInterruptIn,
    },
    BMap {
        // FUSE_BMAP = 37
        arg: &'a FuseBMapIn,
    },
    Destroy, // FUSE_DESTROY = 38
    #[cfg(feature = "abi-7-11")]
    IoCtl {
        // FUSE_IOCTL = 39
        arg: &'a FuseIoCtlIn,
        data: &'a [u8],
    },
    #[cfg(feature = "abi-7-11")]
    Poll {
        // FUSE_POLL = 40
        arg: &'a FusePollIn,
    },
    #[cfg(feature = "abi-7-15")]
    NotifyReply {
        // FUSE_NOTIFY_REPLY = 41
        data: &'a [u8],
    },
    #[cfg(feature = "abi-7-16")]
    BatchForget {
        // FUSE_BATCH_FORGET = 42
        arg: &'a FuseBatchForgetIn,
        nodes: Vec<&'a FuseForgetOne>,
    },
    #[cfg(feature = "abi-7-19")]
    FAllocate {
        // FUSE_FALLOCATE = 43
        arg: &'a FuseFAllocateIn,
    },
    #[cfg(feature = "abi-7-21")]
    ReadDirPlus {
        // FUSE_READDIRPLUS = 44,
        arg: &'a FuseReadIn,
    },
    #[cfg(feature = "abi-7-23")]
    Rename2 {
        // FUSE_RENAME2 = 45,
        arg: &'a FuseRename2In,
        oldname: &'a OsStr,
        newname: &'a OsStr,
    },
    // TODO: find out the input args
    // #[cfg(feature = "abi-7-24")]
    // FUSE_LSEEK = 46,
    // #[cfg(feature = "abi-7-28")]
    // FUSE_COPY_FILE_RANGE = 47,
    #[cfg(target_os = "macos")]
    SetVolName {
        // FUSE_SETVOLNAME = 61
        name: &'a OsStr,
    },
    #[cfg(target_os = "macos")]
    GetXTimes, // FUSE_GETXTIMES = 62
    #[cfg(target_os = "macos")]
    Exchange {
        // FUSE_EXCHANGE = 63
        arg: &'a FuseExchangeIn,
        oldname: &'a OsStr,
        newname: &'a OsStr,
    },

    #[cfg(feature = "abi-7-11")]
    CuseInit {
        // CUSE_INIT = 4096
        arg: &'a FuseInitIn,
    },
}

impl<'a> Operation<'a> {
    fn parse(n: u32, data: &mut ByteSlice<'a>) -> anyhow::Result<Self> {
        let opcode = match n {
            1 => FuseOpCode::FUSE_LOOKUP,
            2 => FuseOpCode::FUSE_FORGET,
            3 => FuseOpCode::FUSE_GETATTR,
            4 => FuseOpCode::FUSE_SETATTR,
            5 => FuseOpCode::FUSE_READLINK,
            6 => FuseOpCode::FUSE_SYMLINK,
            8 => FuseOpCode::FUSE_MKNOD,
            9 => FuseOpCode::FUSE_MKDIR,
            10 => FuseOpCode::FUSE_UNLINK,
            11 => FuseOpCode::FUSE_RMDIR,
            12 => FuseOpCode::FUSE_RENAME,
            13 => FuseOpCode::FUSE_LINK,
            14 => FuseOpCode::FUSE_OPEN,
            15 => FuseOpCode::FUSE_READ,
            16 => FuseOpCode::FUSE_WRITE,
            17 => FuseOpCode::FUSE_STATFS,
            18 => FuseOpCode::FUSE_RELEASE,
            20 => FuseOpCode::FUSE_FSYNC,
            21 => FuseOpCode::FUSE_SETXATTR,
            22 => FuseOpCode::FUSE_GETXATTR,
            23 => FuseOpCode::FUSE_LISTXATTR,
            24 => FuseOpCode::FUSE_REMOVEXATTR,
            25 => FuseOpCode::FUSE_FLUSH,
            26 => FuseOpCode::FUSE_INIT,
            27 => FuseOpCode::FUSE_OPENDIR,
            28 => FuseOpCode::FUSE_READDIR,
            29 => FuseOpCode::FUSE_RELEASEDIR,
            30 => FuseOpCode::FUSE_FSYNCDIR,
            31 => FuseOpCode::FUSE_GETLK,
            32 => FuseOpCode::FUSE_SETLK,
            33 => FuseOpCode::FUSE_SETLKW,
            34 => FuseOpCode::FUSE_ACCESS,
            35 => FuseOpCode::FUSE_CREATE,
            36 => FuseOpCode::FUSE_INTERRUPT,
            37 => FuseOpCode::FUSE_BMAP,
            38 => FuseOpCode::FUSE_DESTROY,
            #[cfg(feature = "abi-7-11")]
            39 => FuseOpCode::FUSE_IOCTL,
            #[cfg(feature = "abi-7-11")]
            40 => FuseOpCode::FUSE_POLL,
            #[cfg(feature = "abi-7-15")]
            41 => FuseOpCode::FUSE_NOTIFY_REPLY,
            #[cfg(feature = "abi-7-16")]
            42 => FuseOpCode::FUSE_BATCH_FORGET,
            #[cfg(feature = "abi-7-19")]
            43 => FuseOpCode::FUSE_FALLOCATE,

            #[cfg(target_os = "macos")]
            61 => FuseOpCode::FUSE_SETVOLNAME,
            #[cfg(target_os = "macos")]
            62 => FuseOpCode::FUSE_GETXTIMES,
            #[cfg(target_os = "macos")]
            63 => FuseOpCode::FUSE_EXCHANGE,

            #[cfg(feature = "abi-7-11")]
            4096 => FuseOpCode::CUSE_INIT,

            _ => panic!("unknown FUSE OpCode={}", n),
        };

        Ok(match opcode {
            FuseOpCode::FUSE_LOOKUP => Operation::Lookup {
                name: data.fetch_str()?,
            },
            FuseOpCode::FUSE_FORGET => Operation::Forget { arg: data.fetch()? },
            FuseOpCode::FUSE_GETATTR => Operation::GetAttr,
            FuseOpCode::FUSE_SETATTR => Operation::SetAttr { arg: data.fetch()? },
            FuseOpCode::FUSE_READLINK => Operation::ReadLink,
            FuseOpCode::FUSE_SYMLINK => Operation::SymLink {
                name: data.fetch_str()?,
                link: data.fetch_str()?,
            },
            FuseOpCode::FUSE_MKNOD => Operation::MkNod {
                arg: data.fetch()?,
                name: data.fetch_str()?,
            },
            FuseOpCode::FUSE_MKDIR => Operation::MkDir {
                arg: data.fetch()?,
                name: data.fetch_str()?,
            },
            FuseOpCode::FUSE_UNLINK => Operation::Unlink {
                name: data.fetch_str()?,
            },
            FuseOpCode::FUSE_RMDIR => Operation::RmDir {
                name: data.fetch_str()?,
            },
            FuseOpCode::FUSE_RENAME => Operation::Rename {
                arg: data.fetch()?,
                oldname: data.fetch_str()?,
                newname: data.fetch_str()?,
            },
            FuseOpCode::FUSE_LINK => Operation::Link {
                arg: data.fetch()?,
                name: data.fetch_str()?,
            },
            FuseOpCode::FUSE_OPEN => Operation::Open { arg: data.fetch()? },
            FuseOpCode::FUSE_READ => Operation::Read { arg: data.fetch()? },
            FuseOpCode::FUSE_WRITE => Operation::Write {
                arg: data.fetch()?,
                data: data.fetch_all(),
            },
            FuseOpCode::FUSE_STATFS => Operation::StatFs,
            FuseOpCode::FUSE_RELEASE => Operation::Release { arg: data.fetch()? },
            FuseOpCode::FUSE_FSYNC => Operation::FSync { arg: data.fetch()? },
            FuseOpCode::FUSE_SETXATTR => Operation::SetXAttr {
                arg: data.fetch()?,
                name: data.fetch_str()?,
                value: data.fetch_all(),
            },
            FuseOpCode::FUSE_GETXATTR => Operation::GetXAttr {
                arg: data.fetch()?,
                name: data.fetch_str()?,
            },
            FuseOpCode::FUSE_LISTXATTR => Operation::ListXAttr { arg: data.fetch()? },
            FuseOpCode::FUSE_REMOVEXATTR => Operation::RemoveXAttr {
                name: data.fetch_str()?,
            },
            FuseOpCode::FUSE_FLUSH => Operation::Flush { arg: data.fetch()? },
            FuseOpCode::FUSE_INIT => Operation::Init { arg: data.fetch()? },
            FuseOpCode::FUSE_OPENDIR => Operation::OpenDir { arg: data.fetch()? },
            FuseOpCode::FUSE_READDIR => Operation::ReadDir { arg: data.fetch()? },
            FuseOpCode::FUSE_RELEASEDIR => Operation::ReleaseDir { arg: data.fetch()? },
            FuseOpCode::FUSE_FSYNCDIR => Operation::FSyncDir { arg: data.fetch()? },
            FuseOpCode::FUSE_GETLK => Operation::GetLk { arg: data.fetch()? },
            FuseOpCode::FUSE_SETLK => Operation::SetLk { arg: data.fetch()? },
            FuseOpCode::FUSE_SETLKW => Operation::SetLkW { arg: data.fetch()? },
            FuseOpCode::FUSE_ACCESS => Operation::Access { arg: data.fetch()? },
            FuseOpCode::FUSE_CREATE => Operation::Create {
                arg: data.fetch()?,
                name: data.fetch_str()?,
            },
            FuseOpCode::FUSE_INTERRUPT => Operation::Interrupt { arg: data.fetch()? },
            FuseOpCode::FUSE_BMAP => Operation::BMap { arg: data.fetch()? },
            FuseOpCode::FUSE_DESTROY => Operation::Destroy,
            #[cfg(feature = "abi-7-11")]
            FuseOpCode::FUSE_IOCTL => Operation::IoCtl {
                arg: data.fetch()?,
                data: data.fetch_all(),
            },
            #[cfg(feature = "abi-7-11")]
            FuseOpCode::FUSE_POLL => Operation::Poll { arg: data.fetch()? },
            #[cfg(feature = "abi-7-15")]
            FuseOpCode::FUSE_NOTIFY_REPLY => Operation::NotifyReply {
                data: data.fetch_all(),
            },
            #[cfg(feature = "abi-7-16")]
            FuseOpCode::FUSE_BATCH_FORGET => Operation::BatchForget {
                arg: data.fetch()?,
                nodes: data.fetch_slice()?,
            },
            #[cfg(feature = "abi-7-19")]
            FuseOpCode::FUSE_FALLOCATE => Operation::FAllocate { arg: data.fetch()? },

            #[cfg(target_os = "macos")]
            FuseOpCode::FUSE_SETVOLNAME => Operation::SetVolName {
                name: data.fetch_str()?,
            },
            #[cfg(target_os = "macos")]
            FuseOpCode::FUSE_GETXTIMES => Operation::GetXTimes,
            #[cfg(target_os = "macos")]
            FuseOpCode::FUSE_EXCHANGE => Operation::Exchange {
                arg: data.fetch()?,
                oldname: data.fetch_str()?,
                newname: data.fetch_str()?,
            },

            #[cfg(feature = "abi-7-11")]
            FuseOpCode::CUSE_INIT => Operation::CuseInit { arg: data.fetch()? },
        })
    }
}

impl<'a> fmt::Display for Operation<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Operation::Lookup { name } => write!(f, "LOOKUP name={:?}", name),
            Operation::Forget { arg } => write!(f, "FORGET nlookup={}", arg.nlookup),
            Operation::GetAttr => write!(f, "GETATTR"),
            Operation::SetAttr { arg } => write!(f, "SETATTR valid={:#x}", arg.valid),
            Operation::ReadLink => write!(f, "READLINK"),
            Operation::SymLink { name, link } => {
                write!(f, "SYMLINK name={:?}, link={:?}", name, link)
            }
            Operation::MkNod { arg, name } => write!(
                f,
                "MKNOD name={:?}, mode={:#05o}, rdev={}",
                name, arg.mode, arg.rdev
            ),
            Operation::MkDir { arg, name } => {
                write!(f, "MKDIR name={:?}, mode={:#05o}", name, arg.mode)
            }
            Operation::Unlink { name } => write!(f, "UNLINK name={:?}", name),
            Operation::RmDir { name } => write!(f, "RMDIR name={:?}", name),
            Operation::Rename {
                arg,
                oldname,
                newname,
            } => write!(
                f,
                "RENAME name={:?}, newdir={:#018x}, newname={:?}",
                oldname, arg.newdir, newname
            ),
            Operation::Link { arg, name } => {
                write!(f, "LINK name={:?}, oldnodeid={:#018x}", name, arg.oldnodeid)
            }
            Operation::Open { arg } => write!(f, "OPEN flags={:#x}", arg.flags),
            Operation::Read { arg } => write!(
                f,
                "READ fh={}, offset={}, size={}",
                arg.fh, arg.offset, arg.size
            ),
            Operation::Write { arg, .. } => write!(
                f,
                "WRITE fh={}, offset={}, size={}, write flags={:#x}",
                arg.fh, arg.offset, arg.size, arg.write_flags
            ),
            Operation::StatFs => write!(f, "STATFS"),
            Operation::Release { arg } => write!(
                f,
                "RELEASE fh={}, flags={:#x}, release flags={:#x}, lock owner={}",
                arg.fh, arg.flags, arg.release_flags, arg.lock_owner
            ),
            Operation::FSync { arg } => {
                write!(f, "FSYNC fh={}, fsync flags={:#x}", arg.fh, arg.fsync_flags)
            }
            Operation::SetXAttr { arg, name, .. } => write!(
                f,
                "SETXATTR name={:?}, size={}, flags={:#x}",
                name, arg.size, arg.flags
            ),
            Operation::GetXAttr { arg, name } => {
                write!(f, "GETXATTR name={:?}, size={}", name, arg.size)
            }
            Operation::ListXAttr { arg } => write!(f, "LISTXATTR size={}", arg.size),
            Operation::RemoveXAttr { name } => write!(f, "REMOVEXATTR name={:?}", name),
            Operation::Flush { arg } => {
                write!(f, "FLUSH fh={}, lock owner={}", arg.fh, arg.lock_owner)
            }
            Operation::Init { arg } => write!(
                f,
                "INIT kernel ABI={}.{}, flags={:#x}, max readahead={}",
                arg.major, arg.minor, arg.flags, arg.max_readahead
            ),
            Operation::OpenDir { arg } => write!(f, "OPENDIR flags={:#x}", arg.flags),
            Operation::ReadDir { arg } => write!(
                f,
                "READDIR fh={}, offset={}, size={}",
                arg.fh, arg.offset, arg.size
            ),
            Operation::ReleaseDir { arg } => write!(
                f,
                "RELEASEDIR fh={}, flags={:#x}, release flags={:#x}, lock owner={}",
                arg.fh, arg.flags, arg.release_flags, arg.lock_owner
            ),
            Operation::FSyncDir { arg } => write!(
                f,
                "FSYNCDIR fh={}, fsync flags={:#x}",
                arg.fh, arg.fsync_flags
            ),
            Operation::GetLk { arg } => write!(f, "GETLK fh={}, lock owner={}", arg.fh, arg.owner),
            Operation::SetLk { arg } => write!(f, "SETLK fh={}, lock owner={}", arg.fh, arg.owner),
            Operation::SetLkW { arg } => {
                write!(f, "SETLKW fh={}, lock owner={}", arg.fh, arg.owner)
            }
            Operation::Access { arg } => write!(f, "ACCESS mask={:#05o}", arg.mask),
            Operation::Create { arg, name } => write!(
                f,
                "CREATE name={:?}, mode={:#05o}, flags={:#x}",
                name, arg.mode, arg.flags,
            ),
            Operation::Interrupt { arg } => write!(f, "INTERRUPT unique={}", arg.unique),
            Operation::BMap { arg } => {
                write!(f, "BMAP blocksize={}, ids={}", arg.blocksize, arg.block)
            }
            Operation::Destroy => write!(f, "DESTROY"),

            #[cfg(feature = "abi-7-11")]
            Operation::IoCtl { arg, data } => write!(
                f,
                "IOCTL fh={}, flags {:#x}, cmd={}, arg={}",
                arg.fh, arg.flags, arg.cmd, arg.arg,
            ),
            #[cfg(feature = "abi-7-11")]
            Operation::Poll { arg } => {
                write!(f, "POLL fh={}, kh={}, flags={:#x} ", arg.fh, arg.kh, arg.flags)
            }
            #[cfg(feature = "abi-7-15")]
            Operation::NotifyReply { data } => write!(f, "NOTIFY REPLY"),
            #[cfg(feature = "abi-7-16")]
            Operation::BatchForget { arg, nodes } => {
                write!(f, "BATCH FORGOT count={}", arg.count)
            }
            #[cfg(feature = "abi-7-19")]
            Operation::FAllocate { arg } => write!(
                f,
                "FALLOCATE fh={}, offset={}, length={}, mode={:#05o}",
                arg.fh, arg.offset, arg.length, arg.mode,
            ),
            #[cfg(feature = "abi-7-21")]
            Operation::ReadDirPlus { arg } => write!(
                f,
                "READDIRPLUS fh={}, offset={}, size={}",
                arg.fh, arg.offset, arg.size,
            ),
            #[cfg(feature = "abi-7-23")]
            Operation::Rename2 { arg, oldname, newname } => write!(
                f,
                "RENAME2 name={:?}, newdir={:#018x}, newname={:?}, flags={:#x}",
                oldname, arg.newdir, newname, arg.flags,
            ),
            // #[cfg(feature = "abi-7-24")]
            // FUSE_LSEEK = 46,
            // #[cfg(feature = "abi-7-28")]
            // FUSE_COPY_FILE_RANGE = 47,

            #[cfg(target_os = "macos")]
            Operation::SetVolName { name } => write!(f, "SETVOLNAME name={:?}", name),
            #[cfg(target_os = "macos")]
            Operation::GetXTimes => write!(f, "GETXTIMES"),
            #[cfg(target_os = "macos")]
            Operation::Exchange { arg, oldname, newname } => write!(
                f,
                "EXCHANGE olddir={:#018x}, oldname={:?}, newdir={:#018x}, newname={:?}, options={:#x}",
                arg.olddir, oldname, arg.newdir, newname, arg.options,
            ),

            #[cfg(feature = "abi-7-11")]
            Operation::CuseInit { arg } => write!(
                f,
                "CUSE INIT kernel ABI={}.{}, flags={:#x}, max readahead={}",
                arg.major, arg.minor, arg.flags, arg.max_readahead,
            ),
        }
    }
}

#[derive(Debug)]
pub(crate) struct Request<'a> {
    header: &'a FuseInHeader,
    operation: Operation<'a>,
}

impl<'a> fmt::Display for Request<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "FUSE({:3}) ino={:#018x} opcode={} operation={}",
            self.header.unique, self.header.nodeid, self.header.opcode, self.operation,
        )
    }
}

impl<'a> Request<'a> {
    pub fn new(bytes: &'a [u8]) -> anyhow::Result<Self> {
        let data_len = bytes.len();
        let mut data = ByteSlice::new(bytes);
        // Parse header
        let header = data.fetch::<FuseInHeader>()?;
        // Check data size
        debug_assert!(
            data_len >= header.len as usize, // TODO: why not daten_len == header.len?
            "failed to assert {} >= {}",
            data_len,
            header.len,
        );
        // Parse/check operation arguments
        let operation = Operation::parse(header.opcode, &mut data)?;
        Ok(Self { header, operation })
    }

    /// Returns the unique identifier of this request.
    ///
    /// The FUSE kernel driver assigns a unique id to every concurrent request. This allows to
    /// distinguish between multiple concurrent requests. The unique id of a request may be
    /// reused in later requests after it has completed.
    #[inline]
    pub fn unique(&self) -> u64 {
        self.header.unique
    }

    /// Returns the node id of the inode this request is targeted to.
    #[inline]
    pub fn nodeid(&self) -> u64 {
        self.header.nodeid
    }

    /// Returns the UID that the process that triggered this request runs under.
    #[inline]
    pub fn uid(&self) -> u32 {
        self.header.uid
    }

    /// Returns the GID that the process that triggered this request runs under.
    #[inline]
    pub fn gid(&self) -> u32 {
        self.header.gid
    }

    /// Returns the PID of the process that triggered this request.
    #[inline]
    pub fn pid(&self) -> u32 {
        self.header.pid
    }

    /// Returns the byte length of this request.
    #[inline]
    pub fn len(&self) -> u32 {
        self.header.len
    }

    /// Returns the filesystem operation (and its arguments) of this request.
    #[inline]
    pub fn operation(&self) -> &Operation<'_> {
        &self.operation
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use log::debug;

    #[cfg(target_endian = "big")]
    const INIT_REQUEST: [u8; 56] = [
        0x00, 0x00, 0x00, 0x38, 0x00, 0x00, 0x00, 0x1a, // len, opcode
        0xde, 0xad, 0xbe, 0xef, 0xba, 0xad, 0xd0, 0x0d, // unique
        0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, // nodeid
        0xc0, 0x01, 0xd0, 0x0d, 0xc0, 0x01, 0xca, 0xfe, // uid, gid
        0xc0, 0xde, 0xba, 0x5e, 0x00, 0x00, 0x00, 0x00, // pid, padding
        0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x08, // major, minor
        0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, // max_readahead, flags
    ];

    #[cfg(target_endian = "little")]
    const INIT_REQUEST: [u8; 56] = [
        0x38, 0x00, 0x00, 0x00, 0x1a, 0x00, 0x00, 0x00, // len, opcode
        0x0d, 0xf0, 0xad, 0xba, 0xef, 0xbe, 0xad, 0xde, // unique
        0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, // nodeid
        0x0d, 0xd0, 0x01, 0xc0, 0xfe, 0xca, 0x01, 0xc0, // uid, gid
        0x5e, 0xba, 0xde, 0xc0, 0x00, 0x00, 0x00, 0x00, // pid, padding
        0x07, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, // major, minor
        0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // max_readahead, flags
    ];

    #[cfg(target_endian = "big")]
    const MKNOD_REQUEST: [u8; 56] = [
        0x00, 0x00, 0x00, 0x38, 0x00, 0x00, 0x00, 0x08, // len, opcode
        0xde, 0xad, 0xbe, 0xef, 0xba, 0xad, 0xd0, 0x0d, // unique
        0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, // nodeid
        0xc0, 0x01, 0xd0, 0x0d, 0xc0, 0x01, 0xca, 0xfe, // uid, gid
        0xc0, 0xde, 0xba, 0x5e, 0x00, 0x00, 0x00, 0x00, // pid, padding
        0x00, 0x00, 0x01, 0xa4, 0x00, 0x00, 0x00, 0x00, // mode, rdev
        0x66, 0x6f, 0x6f, 0x2e, 0x74, 0x78, 0x74, 0x00, // name
    ];

    #[cfg(target_endian = "little")]
    const MKNOD_REQUEST: [u8; 56] = [
        0x38, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, // len, opcode
        0x0d, 0xf0, 0xad, 0xba, 0xef, 0xbe, 0xad, 0xde, // unique
        0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, // nodeid
        0x0d, 0xd0, 0x01, 0xc0, 0xfe, 0xca, 0x01, 0xc0, // uid, gid
        0x5e, 0xba, 0xde, 0xc0, 0x00, 0x00, 0x00, 0x00, // pid, padding
        0xa4, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // mode, rdev
        0x66, 0x6f, 0x6f, 0x2e, 0x74, 0x78, 0x74, 0x00, // name
    ];

    #[test]
    fn short_read_header() {
        match Request::new(&INIT_REQUEST[..20]) {
            Err(e) => debug!(
                "no enough data to build rquest header, the error is: {:?}",
                e
            ),
            _ => panic!("Unexpected request parsing result"),
        }
    }

    #[test]
    #[should_panic(expected = "failed to assert 48 >= 56")]
    fn short_read() {
        let _req = Request::new(&INIT_REQUEST[..48]);
    }

    #[test]
    fn init() {
        let req = Request::new(&INIT_REQUEST[..]).unwrap();
        assert_eq!(req.header.len, 56);
        assert_eq!(req.header.opcode, 26);
        assert_eq!(req.unique(), 0xdead_beef_baad_f00d);
        assert_eq!(req.nodeid(), 0x1122_3344_5566_7788);
        assert_eq!(req.uid(), 0xc001_d00d);
        assert_eq!(req.gid(), 0xc001_cafe);
        assert_eq!(req.pid(), 0xc0de_ba5e);
        match req.operation() {
            Operation::Init { arg } => {
                assert_eq!(arg.major, 7);
                assert_eq!(arg.minor, 8);
                assert_eq!(arg.max_readahead, 4096);
            }
            _ => panic!("unexpected request operation"),
        }
    }

    #[test]
    fn mknod() {
        let req = Request::new(&MKNOD_REQUEST[..]).unwrap();
        assert_eq!(req.header.len, 56);
        assert_eq!(req.header.opcode, 8);
        assert_eq!(req.unique(), 0xdead_beef_baad_f00d);
        assert_eq!(req.nodeid(), 0x1122_3344_5566_7788);
        assert_eq!(req.uid(), 0xc001_d00d);
        assert_eq!(req.gid(), 0xc001_cafe);
        assert_eq!(req.pid(), 0xc0de_ba5e);
        match req.operation() {
            Operation::MkNod { arg, name } => {
                assert_eq!(arg.mode, 0o644);
                assert_eq!(*name, "foo.txt");
            }
            _ => panic!("unexpected request operation"),
        }
    }
}
