//! The implementation for FUSE request

use std::fmt;

use clippy_utilities::Cast;
use tracing::debug;

use super::context::ProtoVersion;
use super::de::{DeserializeError, Deserializer};
#[cfg(feature = "abi-7-19")]
use super::protocol::FuseFAllocateIn;
#[cfg(feature = "abi-7-23")]
use super::protocol::FuseRename2In;
use super::protocol::{
    FuseAccessIn, FuseBMapIn, FuseCopyFileRangeIn, FuseCreateIn, FuseFSyncIn, FuseFlushIn,
    FuseForgetIn, FuseGetXAttrIn, FuseInHeader, FuseInitIn, FuseInterruptIn, FuseLSeekIn,
    FuseLinkIn, FuseLockIn, FuseMkDirIn, FuseMkNodIn, FuseOpCode, FuseOpenIn, FuseReadIn,
    FuseReleaseIn, FuseRenameIn, FuseSetAttrIn, FuseSetXAttrIn, FuseWriteIn,
};
#[cfg(feature = "abi-7-16")]
use super::protocol::{FuseBatchForgetIn, FuseForgetOne};
#[cfg(feature = "abi-7-11")]
use super::protocol::{FuseIoCtlIn, FusePollIn};

/// FUSE operation
#[derive(Debug)]
pub enum Operation<'a> {
    /// FUSE_LOOKUP = 1
    Lookup {
        /// The directory name to look up
        name: &'a str,
    },
    /// FUSE_FORGET = 2
    Forget {
        /// The number of lookups to forget
        arg: &'a FuseForgetIn,
    },
    /// FUSE_GETATTR = 3
    GetAttr,
    /// FUSE_SETATTR = 4
    SetAttr {
        /// The attributes to be set
        arg: &'a FuseSetAttrIn,
    },
    /// FUSE_READLINK = 5
    ReadLink,
    /// FUSE_SYMLINK = 6
    SymLink {
        /// The link name to create
        name: &'a str,
        /// The contents of the symbolic link
        link: &'a str,
    },
    /// FUSE_MKNOD = 8
    MkNod {
        /// The FUSE mknod request
        arg: &'a FuseMkNodIn,
        /// The file name to create
        name: &'a str,
    },
    /// FUSE_MKDIR = 9
    MkDir {
        /// The FUSE mkdir request input
        arg: &'a FuseMkDirIn,
        /// The directory name to create
        name: &'a str,
    },
    /// FUSE_UNLINK = 10
    Unlink {
        /// The file name to remove
        name: &'a str,
    },
    /// FUSE_RMDIR = 11
    RmDir {
        /// The directory name to remove
        name: &'a str,
    },
    /// FUSE_RENAME = 12
    Rename {
        /// The FUSE rename request
        arg: &'a FuseRenameIn,
        /// The old name
        oldname: &'a str,
        /// The new name
        newname: &'a str,
    },
    /// FUSE_LINK = 13
    Link {
        /// The FUSE link request
        arg: &'a FuseLinkIn,
        /// The new name
        name: &'a str,
    },
    /// FUSE_OPEN = 14
    Open {
        /// The FUSE open request
        arg: &'a FuseOpenIn,
    },
    /// FUSE_READ = 15
    Read {
        /// The FUSE read request
        arg: &'a FuseReadIn,
    },
    /// FUSE_WRITE = 16
    Write {
        /// The FUSE write request
        arg: &'a FuseWriteIn,
        /// The FUSE write request data
        data: &'a [u8],
    },
    /// FUSE_STATFS = 17
    StatFs,
    /// FUSE_RELEASE = 18
    Release {
        /// The FUSE release request
        arg: &'a FuseReleaseIn,
    },
    /// FUSE_FSYNC = 20
    FSync {
        /// The FUSE fsync request
        arg: &'a FuseFSyncIn,
    },
    /// FUSE_SETXATTR = 21
    SetXAttr {
        /// The FUSE set extended attribute request
        arg: &'a FuseSetXAttrIn,
        /// The extended attribute name
        name: &'a str,
        /// The extended attribute value
        value: &'a [u8],
    },
    /// FUSE_GETXATTR = 22
    GetXAttr {
        /// The FUSE get extended attribute request
        arg: &'a FuseGetXAttrIn,
        /// The extended attribute name
        name: &'a str,
    },
    /// FUSE_LISTXATTR = 23
    ListXAttr {
        /// The FUSE list extended attribute request
        arg: &'a FuseGetXAttrIn,
    },
    /// FUSE_REMOVEXATTR = 24
    RemoveXAttr {
        /// The name of the extended attribute to remove
        name: &'a str,
    },
    /// FUSE_FLUSH = 25
    Flush {
        /// The FUSE flush request
        arg: &'a FuseFlushIn,
    },
    /// FUSE_INIT = 26
    Init {
        /// The FUSE init request
        arg: &'a FuseInitIn,
    },
    /// FUSE_OPENDIR = 27
    OpenDir {
        /// The FUSE open directory request
        arg: &'a FuseOpenIn,
    },
    /// FUSE_READDIR = 28
    ReadDir {
        /// The FUSE read directory request
        arg: &'a FuseReadIn,
    },
    /// FUSE_RELEASEDIR = 29
    ReleaseDir {
        /// The FUSE release directory request
        arg: &'a FuseReleaseIn,
    },
    /// FUSE_FSYNCDIR = 30
    FSyncDir {
        /// The FUSE fsync directory request
        arg: &'a FuseFSyncIn,
    },
    /// FUSE_GETLK = 31
    GetLk {
        /// The FUSE get lock request
        arg: &'a FuseLockIn,
    },
    /// FUSE_SETLK = 32
    SetLk {
        /// The FUSE set lock request
        arg: &'a FuseLockIn,
    },
    /// FUSE_SETLKW = 33
    SetLkW {
        /// The FUSE set lock wait request
        arg: &'a FuseLockIn,
    },
    /// FUSE_ACCESS = 34
    Access {
        /// The FUSE access request
        arg: &'a FuseAccessIn,
    },
    /// FUSE_CREATE = 35
    Create {
        /// The FUSE create request
        arg: &'a FuseCreateIn,
        /// The file name to create
        name: &'a str,
    },
    /// FUSE_INTERRUPT = 36
    Interrupt {
        /// The FUSE interrupt request
        arg: &'a FuseInterruptIn,
    },
    /// FUSE_BMAP = 37
    BMap {
        /// The FUSE bmap request
        arg: &'a FuseBMapIn,
    },
    /// FUSE_DESTROY = 38
    Destroy,
    /// FUSE_IOCTL = 39
    #[cfg(feature = "abi-7-11")]
    IoCtl {
        /// The FUSE ioctl request
        arg: &'a FuseIoCtlIn,
        /// The ioctl request data
        data: &'a [u8],
    },
    /// FUSE_POLL = 40
    #[cfg(feature = "abi-7-11")]
    Poll {
        /// The FUSE poll request
        arg: &'a FusePollIn,
    },
    /// FUSE_NOTIFY_REPLY = 41
    #[cfg(feature = "abi-7-15")]
    NotifyReply {
        /// FUSE notify reply data
        data: &'a [u8],
    },
    /// FUSE_BATCH_FORGET = 42
    #[cfg(feature = "abi-7-16")]
    BatchForget {
        /// The FUSE batch forget request
        arg: &'a FuseBatchForgetIn,
        /// The slice of nodes to forget
        nodes: &'a [FuseForgetOne],
    },
    /// FUSE_FALLOCATE = 43
    #[cfg(feature = "abi-7-19")]
    FAllocate {
        /// The FUSE fallocate request
        arg: &'a FuseFAllocateIn,
    },
    /// FUSE_READDIRPLUS = 44,
    #[cfg(feature = "abi-7-21")]
    ReadDirPlus {
        /// The FUSE read directory plus request
        arg: &'a FuseReadIn,
    },
    /// FUSE_RENAME2 = 45,
    ///
    /// Available when the protocol version is greater than 7.22.
    /// This is checked by the kernel so that DatenLord won't receive such a
    /// request.
    ///
    /// https://github.com/torvalds/linux/blob/8f6f76a6a29f36d2f3e4510d0bde5046672f6924/fs/fuse/dir.c#L1077C2-L1088C3
    #[cfg(feature = "abi-7-23")]
    Rename2 {
        /// The FUSE rename2 request
        arg: &'a FuseRename2In,
        /// The old file name
        oldname: &'a str,
        /// The new file name
        newname: &'a str,
    },
    /// FUSE_LSEEK = 46,
    // #[cfg(feature = "abi-7-24")]
    LSeek {
        /// The FUSE lseek request
        arg: &'a FuseLSeekIn,
    },
    /// FUSE_COPY_FILE_RANGE = 47,
    // #[cfg(feature = "abi-7-28")]
    CopyFileRange {
        /// The FUSE copy file range request
        arg: &'a FuseCopyFileRangeIn,
    },
    /// CUSE_INIT = 4096
    #[cfg(feature = "abi-7-11")]
    CuseInit {
        /// The CUSE init request
        arg: &'a FuseInitIn,
    },
}

impl<'a> Operation<'a> {
    /// Build FUSE operation from op-code
    #[allow(clippy::too_many_lines)]
    fn parse(
        n: u32,
        data: &mut Deserializer<'a>,
        #[allow(unused_variables)] proto_version: ProtoVersion,
    ) -> Result<Self, DeserializeError> {
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
            #[cfg(feature = "abi-7-21")]
            44 => FuseOpCode::FUSE_READDIRPLUS,
            #[cfg(feature = "abi-7-23")]
            45 => FuseOpCode::FUSE_RENAME2,
            // #[cfg(feature = "abi-7-24")]
            46 => FuseOpCode::FUSE_LSEEK,
            // #[cfg(feature = "abi-7-28")]
            47 => FuseOpCode::FUSE_COPY_FILE_RANGE,
            #[cfg(feature = "abi-7-11")]
            4096 => FuseOpCode::CUSE_INIT,

            code => return Err(DeserializeError::UnknownOpCode { code, unique: None }),
        };

        Ok(match opcode {
            FuseOpCode::FUSE_LOOKUP => Operation::Lookup {
                name: data.fetch_str()?,
            },
            FuseOpCode::FUSE_FORGET => Operation::Forget {
                arg: data.fetch_ref()?,
            },
            FuseOpCode::FUSE_GETATTR => Operation::GetAttr,
            FuseOpCode::FUSE_SETATTR => Operation::SetAttr {
                arg: data.fetch_ref()?,
            },
            FuseOpCode::FUSE_READLINK => Operation::ReadLink,
            FuseOpCode::FUSE_SYMLINK => Operation::SymLink {
                name: data.fetch_str()?,
                link: data.fetch_str()?,
            },
            FuseOpCode::FUSE_MKNOD => Operation::MkNod {
                arg: data.fetch_ref()?,
                name: data.fetch_str()?,
            },
            FuseOpCode::FUSE_MKDIR => Operation::MkDir {
                arg: data.fetch_ref()?,
                name: data.fetch_str()?,
            },
            FuseOpCode::FUSE_UNLINK => Operation::Unlink {
                name: data.fetch_str()?,
            },
            FuseOpCode::FUSE_RMDIR => Operation::RmDir {
                name: data.fetch_str()?,
            },
            FuseOpCode::FUSE_RENAME => Operation::Rename {
                arg: data.fetch_ref()?,
                oldname: data.fetch_str()?,
                newname: data.fetch_str()?,
            },
            FuseOpCode::FUSE_LINK => Operation::Link {
                arg: data.fetch_ref()?,
                name: data.fetch_str()?,
            },
            FuseOpCode::FUSE_OPEN => Operation::Open {
                arg: data.fetch_ref()?,
            },
            FuseOpCode::FUSE_READ => Operation::Read {
                arg: data.fetch_ref()?,
            },
            FuseOpCode::FUSE_WRITE => Operation::Write {
                arg: data.fetch_ref()?,
                data: data.fetch_all_bytes(),
            },
            FuseOpCode::FUSE_STATFS => Operation::StatFs,
            FuseOpCode::FUSE_RELEASE => Operation::Release {
                arg: data.fetch_ref()?,
            },
            FuseOpCode::FUSE_FSYNC => Operation::FSync {
                arg: data.fetch_ref()?,
            },
            FuseOpCode::FUSE_SETXATTR => Operation::SetXAttr {
                arg: data.fetch_ref()?,
                name: data.fetch_str()?,
                value: data.fetch_all_bytes(),
            },
            FuseOpCode::FUSE_GETXATTR => Operation::GetXAttr {
                arg: data.fetch_ref()?,
                name: data.fetch_str()?,
            },
            FuseOpCode::FUSE_LISTXATTR => Operation::ListXAttr {
                arg: data.fetch_ref()?,
            },
            FuseOpCode::FUSE_REMOVEXATTR => Operation::RemoveXAttr {
                name: data.fetch_str()?,
            },
            FuseOpCode::FUSE_FLUSH => Operation::Flush {
                arg: data.fetch_ref()?,
            },
            FuseOpCode::FUSE_INIT => Operation::Init {
                arg: data.fetch_ref()?,
            },
            FuseOpCode::FUSE_OPENDIR => Operation::OpenDir {
                arg: data.fetch_ref()?,
            },
            FuseOpCode::FUSE_READDIR => Operation::ReadDir {
                arg: data.fetch_ref()?,
            },
            FuseOpCode::FUSE_RELEASEDIR => Operation::ReleaseDir {
                arg: data.fetch_ref()?,
            },
            FuseOpCode::FUSE_FSYNCDIR => Operation::FSyncDir {
                arg: data.fetch_ref()?,
            },
            FuseOpCode::FUSE_GETLK => Operation::GetLk {
                arg: data.fetch_ref()?,
            },
            FuseOpCode::FUSE_SETLK => Operation::SetLk {
                arg: data.fetch_ref()?,
            },
            FuseOpCode::FUSE_SETLKW => Operation::SetLkW {
                arg: data.fetch_ref()?,
            },
            FuseOpCode::FUSE_ACCESS => Operation::Access {
                arg: data.fetch_ref()?,
            },
            FuseOpCode::FUSE_CREATE => Operation::Create {
                arg: data.fetch_ref()?,
                name: data.fetch_str()?,
            },
            FuseOpCode::FUSE_INTERRUPT => Operation::Interrupt {
                arg: data.fetch_ref()?,
            },
            FuseOpCode::FUSE_BMAP => Operation::BMap {
                arg: data.fetch_ref()?,
            },
            FuseOpCode::FUSE_DESTROY => Operation::Destroy,
            #[cfg(feature = "abi-7-11")]
            FuseOpCode::FUSE_IOCTL => Operation::IoCtl {
                arg: data.fetch_ref()?,
                data: data.fetch_all_bytes(),
            },
            #[cfg(feature = "abi-7-11")]
            FuseOpCode::FUSE_POLL => Operation::Poll {
                arg: data.fetch_ref()?,
            },
            #[cfg(feature = "abi-7-15")]
            FuseOpCode::FUSE_NOTIFY_REPLY => Operation::NotifyReply {
                data: data.fetch_all_bytes(),
            },
            #[cfg(feature = "abi-7-16")]
            FuseOpCode::FUSE_BATCH_FORGET => Operation::BatchForget {
                arg: data.fetch_ref()?,
                nodes: data.fetch_all_as_slice()?,
            },
            #[cfg(feature = "abi-7-19")]
            FuseOpCode::FUSE_FALLOCATE => Operation::FAllocate {
                arg: data.fetch_ref()?,
            },
            #[cfg(feature = "abi-7-21")]
            FuseOpCode::FUSE_READDIRPLUS => Operation::ReadDirPlus {
                arg: data.fetch_ref()?,
            },
            #[cfg(feature = "abi-7-23")]
            FuseOpCode::FUSE_RENAME2 => Operation::Rename2 {
                arg: data.fetch_ref()?,
                oldname: data.fetch_str()?,
                newname: data.fetch_str()?,
            },
            // #[cfg(feature = "abi-7-24")]
            FuseOpCode::FUSE_LSEEK => Operation::LSeek {
                arg: data.fetch_ref()?,
            },
            // #[cfg(feature = "abi-7-28")]
            FuseOpCode::FUSE_COPY_FILE_RANGE => Operation::CopyFileRange {
                arg: data.fetch_ref()?,
            },
            #[cfg(feature = "abi-7-11")]
            FuseOpCode::CUSE_INIT => Operation::CuseInit {
                arg: data.fetch_ref()?,
            },
        })
    }
}

impl fmt::Display for Operation<'_> {
    /// Format FUSE operation to display
    #[allow(clippy::too_many_lines)]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Operation::Lookup { name } => write!(f, "LOOKUP name={name:?}"),
            Operation::Forget { arg } => write!(f, "FORGET nlookup={}", arg.nlookup),
            Operation::GetAttr => write!(f, "GETATTR"),
            Operation::SetAttr { arg } => write!(f, "SETATTR valid={:#x}", arg.valid),
            Operation::ReadLink => write!(f, "READLINK"),
            Operation::SymLink { name, link } => {
                write!(f, "SYMLINK name={name:?}, link={link:?}")
            }
            Operation::MkNod { arg, name } => write!(
                f,
                "MKNOD name={:?}, mode={:#05o}, rdev={}",
                name, arg.mode, arg.rdev
            ),
            Operation::MkDir { arg, name } => {
                write!(f, "MKDIR name={:?}, mode={:#05o}", name, arg.mode)
            }
            Operation::Unlink { name } => write!(f, "UNLINK name={name:?}"),
            Operation::RmDir { name } => write!(f, "RMDIR name={name:?}"),
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
            Operation::RemoveXAttr { name } => write!(f, "REMOVEXATTR name={name:?}"),
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
                "IOCTL fh={}, flags {:#x}, cmd={}, arg={}, data={:?}",
                arg.fh, arg.flags, arg.cmd, arg.arg, data,
            ),
            #[cfg(feature = "abi-7-11")]
            Operation::Poll { arg } => {
                write!(
                    f,
                    "POLL fh={}, kh={}, flags={:#x} ",
                    arg.fh, arg.kh, arg.flags
                )
            }
            #[cfg(feature = "abi-7-15")]
            Operation::NotifyReply { data } => write!(f, "NOTIFY REPLY data={data:?}"),
            #[cfg(feature = "abi-7-16")]
            Operation::BatchForget { arg, nodes } => {
                write!(f, "BATCH FORGOT count={}, nodes={:?}", arg.count, nodes)
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
            Operation::Rename2 {
                arg,
                oldname,
                newname,
            } => write!(
                f,
                "RENAME2 name={:?}, newdir={:#018x}, newname={:?}, flags={:#x}",
                oldname, arg.newdir, newname, arg.flags,
            ),
            // #[cfg(feature = "abi-7-24")]
            Operation::LSeek { arg } => write!(
                f,
                "LSEEK fh={}, offset={}, whence={}",
                arg.fh, arg.offset, arg.whence,
            ),
            // #[cfg(feature = "abi-7-28")]
            Operation::CopyFileRange { arg } => write!(
                f,
                "COPYFILERANGE src fh={}, dst fh={}, flags={:#?}",
                arg.fh_in, arg.fh_out, arg.flags,
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

/// FUSE request
#[derive(Debug)]
pub struct Request<'a> {
    /// FUSE request header
    header: &'a FuseInHeader,
    /// FUSE request operation
    operation: Operation<'a>,
}

impl fmt::Display for Request<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "fuse={} ino={} operation={}",
            self.header.unique, self.header.nodeid, self.operation,
        )
    }
}

impl<'a> Request<'a> {
    /// Build FUSE request
    pub fn new(bytes: &'a [u8], proto_version: ProtoVersion) -> Result<Self, DeserializeError> {
        let data_len = bytes.len();
        let mut de = Deserializer::new(bytes);
        // Parse header
        let header = de.fetch_ref::<FuseInHeader>()?;
        // Check data size
        debug_assert!(
            data_len >= header.len.cast(), // TODO: why not daten_len == header.len?
            "failed to assert {} >= {}",
            data_len,
            header.len,
        );
        // Parse/check operation arguments
        let operation = Operation::parse(header.opcode, &mut de, proto_version).map_err(|e| {
            if let DeserializeError::UnknownOpCode { code, .. } = e {
                DeserializeError::UnknownOpCode {
                    code,
                    unique: Some(header.unique),
                }
            } else {
                e
            }
        })?;
        if de.remaining_len() > 0 {
            debug!(
                "request bytes is not completely consumed: \
                    bytes.len() = {}, header = {:?}, de.remaining_len() = {}, de = {:?}",
                bytes.len(),
                header,
                de.remaining_len(),
                de
            );
        }

        Ok(Self { header, operation })
    }

    /// Returns the unique identifier of this request.
    ///
    /// The FUSE kernel driver assigns a unique id to every concurrent request.
    /// This allows to distinguish between multiple concurrent requests. The
    /// unique id of a request may be reused in later requests after it has
    /// completed.
    #[inline]
    #[must_use]
    pub const fn unique(&self) -> u64 {
        self.header.unique
    }

    /// Returns the node ID of the inode this request is targeted to.
    #[inline]
    #[must_use]
    pub const fn nodeid(&self) -> u64 {
        self.header.nodeid
    }

    /// Returns the UID that the process that triggered this request runs under.
    #[allow(dead_code)]
    #[inline]
    #[must_use]
    pub const fn uid(&self) -> u32 {
        self.header.uid
    }

    /// Returns the GID that the process that triggered this request runs under.
    #[allow(dead_code)]
    #[inline]
    #[must_use]
    pub const fn gid(&self) -> u32 {
        self.header.gid
    }

    /// Returns the PID of the process that triggered this request.
    #[allow(dead_code)]
    #[inline]
    #[must_use]
    pub const fn pid(&self) -> u32 {
        self.header.pid
    }

    /// Returns the byte length of this request.
    #[allow(dead_code)]
    #[inline]
    #[must_use]
    pub const fn len(&self) -> u32 {
        self.header.len
    }

    /// Returns if the byte length of this request is 0.
    #[allow(dead_code)]
    #[inline]
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.header.len == 0
    }

    /// Returns the filesystem operation (and its arguments) of this request.
    #[inline]
    #[must_use]
    pub const fn operation(&self) -> &Operation<'_> {
        &self.operation
    }
}

#[cfg(test)]
mod test {
    use tracing::debug;

    use super::super::de::DeserializeError;
    use super::*;

    // `FuseInHeader` is aligned to 8 bytes.
    // `Align8` is enough for structs used here.
    //
    // `[u8;N]` is aligned to 1 byte.
    // Requests which are not well-aligned will cause an alignment error (potential
    // UB). So we have runtime checks in `ByteSlice::fetch` and
    // `ByteSlice::fetch_all_as_slice`.

    /// Define test data in big endian and little endian in the meantime.
    macro_rules! define_data {
        ($name:ident; $($type:ident: $val:literal),+ $(,)?) => {
            #[cfg(target_endian = "big")]
            const $name: aligned_utils::stack::Align8<[
                u8;
                macro_utils::calculate_size! {
                    BE;
                    $($type: $val),+
                }
            ]> = aligned_utils::stack::Align8(
                macro_utils::generate_bytes! {
                    BE;
                    $($type: $val),+
                }
            );

            #[cfg(target_endian = "little")]
            const $name: aligned_utils::stack::Align8<[
                u8;
                macro_utils::calculate_size! {
                    LE;
                    $($type: $val),+
                }
            ]> = aligned_utils::stack::Align8(
                macro_utils::generate_bytes! {
                    LE;
                    $($type: $val),+
                }
            );
        }
    }

    /// Define test data with default FUSE request header fields.
    ///
    /// The length of header is 40.
    macro_rules! define_payload {
        ($name:ident; len: $len:literal; opcode: $opcode:literal; $($type:ident: $val:literal),* $(,)?) => {
            define_data! {
                $name;
                u32: $len,                   // len
                u32: $opcode,                // opcode
                u64: 0xdead_beef_baad_f00d,  // unique
                u64: 0x1122_3344_5566_7788,  // nodeid
                u32: 0xc001_d00d,            // uid
                u32: 0xc001_cafe,            // gid
                u32: 0xc0de_ba5e,            // pid
                u32: 0,                      // padding
                $($type: $val),*             // payload
            }
        }
    }

    /// assume that kernel protocol version is 7.12
    const PROTO_VERSION: ProtoVersion = ProtoVersion {
        major: 7,
        minor: 12,
    };

    #[test]
    fn short_read_header() {
        let idx = 20;
        let bytes = INIT_REQUEST
            .get(..idx)
            .unwrap_or_else(|| panic!("failed to get the first {idx} elements from INIT_REQUEST"));

        #[allow(clippy::expect_used)]
        let err =
            Request::new(bytes, PROTO_VERSION).expect_err("Unexpected request parsing result");
        assert_eq!(err, DeserializeError::NotEnough);
    }

    #[test]
    #[should_panic(expected = "failed to assert 48 >= 56")]
    fn short_read() {
        let idx = 48;
        let req = Request::new(
            INIT_REQUEST
                .get(..idx)
                .unwrap_or_else(|| panic!("failed to get first {idx} elements from INIT_REQUEST")),
            PROTO_VERSION,
        )
        .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        debug!("short read request={:?}", req);
    }

    fn check_header(req: &Request<'_>) {
        assert_eq!(req.unique(), 0xdead_beef_baad_f00d);
        assert_eq!(req.nodeid(), 0x1122_3344_5566_7788);
        assert_eq!(req.uid(), 0xc001_d00d);
        assert_eq!(req.gid(), 0xc001_cafe);
        assert_eq!(req.pid(), 0xc0de_ba5e);
    }

    define_payload! {
        LOOKUP_REQUEST;
        len: 48;
        opcode: 1;
        str: b"foo.txt\0",  // name
    }

    #[test]
    fn lookup() {
        let req = Request::new(&LOOKUP_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(LOOKUP_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 1);
        check_header(&req);
        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::Lookup { name } => {
                assert_eq!(name, "foo.txt");
            }
            _ => panic!("unexpected request operation"),
        }
    }

    define_payload! {
        FORGET_REQUEST;
        len: 48;
        opcode: 2;
        u64: 5,  // nlookup
    }

    #[test]
    fn forget() {
        let req = Request::new(&FORGET_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(FORGET_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 2);
        check_header(&req);
        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::Forget { arg } => {
                assert_eq!(arg.nlookup, 5);
            }
            _ => panic!("unexpected request operation"),
        }
    }

    define_payload! {
        GETATTR_REQUEST;
        len: 40;
        opcode: 3;
    }

    #[test]
    fn getattr() {
        let req = Request::new(&GETATTR_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(GETATTR_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 3);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::GetAttr => {}
            _ => panic!("unexpected request operation"),
        }
    }

    define_payload! {
        SETATTR_REQUEST;
        len: 128;
        opcode: 4;
        u32: 5,      // valid
        u32: 0,      // padding
        u64: 0x10,   // fh
        u64: 0x20,   // size
        u64: 0x10,   // lock_owner, or unused1 before 7.9
        u64: 0x1234, // atime
        u64: 0x5678, // mtime
        u64: 0x9abc, // ctime, or unused2 before 7.23
        u32: 0x1122, // atimensec
        u32: 0x3344, // mtimensec
        u32: 0x5566, // ctimensec, or unused3 before 7.23
        u32: 0o0755, // mode
        u32: 0,      // unused4
        u32: 1001,   // uid
        u32: 1001,   // gid
        u32: 0,      // unused5
    }

    #[test]
    fn setattr() {
        use super::super::protocol::{FATTR_GID, FATTR_MODE};

        let req = Request::new(&SETATTR_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(SETATTR_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 4);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::SetAttr { arg } => {
                assert_eq!(arg.valid, FATTR_MODE | FATTR_GID);
                assert_eq!(arg.fh, 0x10);
                assert_eq!(arg.size, 0x20);
                #[cfg(feature = "abi-7-9")]
                assert_eq!(arg.lock_owner, 0x10);
                assert_eq!(arg.atime, 0x1234);
                assert_eq!(arg.mtime, 0x5678);
                #[cfg(feature = "abi-7-23")]
                assert_eq!(arg.ctime, 0x9abc);
                assert_eq!(arg.atimensec, 0x1122);
                assert_eq!(arg.mtimensec, 0x3344);
                #[cfg(feature = "abi-7-23")]
                assert_eq!(arg.ctimensec, 0x5566);
                assert_eq!(arg.mode, 0o0755);
                assert_eq!(arg.uid, 1001);
                assert_eq!(arg.gid, 1001);
            }
            _ => panic!("unexpected request operation"),
        }
    }

    define_payload! {
        READLINK_REQUEST;
        len: 40;
        opcode: 5;
    }

    #[test]
    fn readlink() {
        let req = Request::new(&READLINK_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(READLINK_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 5);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::ReadLink => {}
            _ => panic!("unexpected request operation"),
        }
    }

    define_payload! {
        SYMLINK_REQUEST;
        len: 56;
        opcode: 6;
        str: b"foo.txt\0",  // name
        str: b"bar.txt\0",  // link
    }

    #[test]
    fn symlink() {
        let req = Request::new(&SYMLINK_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(SYMLINK_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 6);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::SymLink { name, link } => {
                assert_eq!(name, "foo.txt");
                assert_eq!(link, "bar.txt");
            }
            _ => panic!("unexpected request operation"),
        }
    }

    #[cfg(not(feature = "abi-7-12"))]
    define_payload! {
        MKNOD_REQUEST;
        len: 56;
        opcode: 8;
        u32: 0o0644,                 // mode
        u32: 0,                      // rdev
        str: b"foo.txt\0",           // name
    }

    #[cfg(feature = "abi-7-12")]
    define_payload! {
        MKNOD_REQUEST;
        len: 64;
        opcode: 8;
        u32: 0o0644,                 // mode
        u32: 0,                      // rdev
        u32: 2,                      // umask
        u32: 0,                      // padding
        str: b"foo.txt\0",           // name
    }

    #[test]
    fn mknod() {
        let req = Request::new(&MKNOD_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(req.header.opcode, 8);
        assert_eq!(MKNOD_REQUEST.len(), req.len().cast::<usize>());
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::MkNod { arg, name } => {
                assert_eq!(arg.mode, 0o0644);
                assert_eq!(arg.rdev, 0);
                #[cfg(feature = "abi-7-12")]
                {
                    assert_eq!(arg.umask, 0o002);
                }
                assert_eq!(name, "foo.txt");
            }
            _ => panic!("unexpected request operation"),
        }
    }

    define_payload! {
        MKDIR_REQUEST;
        len: 56;
        opcode: 9;
        u32: 0o0755,        // mode
        u32: 0o0022,        // umask, or padding before 7.12
        str: b"foo.txt\0",  // name
    }

    #[test]
    fn mkdir() {
        let req = Request::new(&MKDIR_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(MKDIR_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 9);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::MkDir { arg, name } => {
                assert_eq!(arg.mode, 0o0755);
                #[cfg(feature = "abi-7-12")]
                assert_eq!(arg.umask, 0o0022);
                assert_eq!(name, "foo.txt");
            }
            _ => panic!("unexpected request operation"),
        }
    }

    define_payload! {
        UNLINK_REQUEST;
        len: 48;
        opcode: 10;
        str: b"foo.txt\0",  // name
    }

    #[test]
    fn unlink() {
        let req = Request::new(&UNLINK_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(UNLINK_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 10);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::Unlink { name } => {
                assert_eq!(name, "foo.txt");
            }
            _ => panic!("unexpected request operation"),
        }
    }

    define_payload! {
        RMDIR_REQUEST;
        len: 48;
        opcode: 11;
        str: b"foo.txt\0",  // name
    }

    #[test]
    fn rmdir() {
        let req = Request::new(&RMDIR_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(RMDIR_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 11);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::RmDir { name } => {
                assert_eq!(name, "foo.txt");
            }
            _ => panic!("unexpected request operation"),
        }
    }

    define_payload! {
        RENAME_REQUEST;
        len: 64;
        opcode: 12;
        u64: 1,             // newdir
        str: b"foo.txt\0",  // oldname
        str: b"bar.txt\0",  // newname
    }

    #[test]
    fn rename() {
        let req = Request::new(&RENAME_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(RENAME_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 12);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::Rename {
                arg,
                oldname,
                newname,
            } => {
                assert_eq!(arg.newdir, 1);
                assert_eq!(oldname, "foo.txt");
                assert_eq!(newname, "bar.txt");
            }
            _ => panic!("unexpected request operation"),
        }
    }

    define_payload! {
        LINK_REQUEST;
        len: 56;
        opcode: 13;
        u64: 1,             // oldnodeid
        str: b"bar.txt\0",  // name
    }

    #[test]
    fn link() {
        let req = Request::new(&LINK_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(LINK_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 13);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::Link { arg, name } => {
                assert_eq!(arg.oldnodeid, 1);
                assert_eq!(name, "bar.txt");
            }
            _ => panic!("unexpected request operation"),
        }
    }

    define_payload! {
        OPEN_REQUEST;
        len: 48;
        opcode: 14;
        u32: 2,          // flags
        u32: 0o0755,     // mode, or unused from 7.12
    }

    #[test]
    fn open() {
        use super::super::protocol::FOPEN_KEEP_CACHE;

        let req = Request::new(&OPEN_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(OPEN_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 14);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::Open { arg } => {
                assert_eq!(arg.flags, FOPEN_KEEP_CACHE);
                #[cfg(not(feature = "abi-7-12"))]
                assert_eq!(arg.mode, 0o0755);
            }
            _ => panic!("unexpected request operation"),
        }
    }

    #[cfg(not(feature = "abi-7-9"))]
    define_payload! {
        READ_REQUEST;
        len: 64;
        opcode: 15;
        u64: 0x10,  // fh
        u64: 0x0a,  // offset
        u32: 0x10,  // size
        u32: 0,     // padding
    }

    #[cfg(feature = "abi-7-9")]
    define_payload! {
        READ_REQUEST;
        len: 80;
        opcode: 15;
        u64: 0x10,   // fh
        u64: 0x0a,   // offset
        u32: 0x10,   // size
        u32: 0,      // read_flags
        u64: 0x1234, // lock_owner
        u32: 2,      // flags
        u32: 0,      // padding
    }

    #[test]
    fn read() {
        #[cfg(feature = "abi-7-9")]
        use super::super::protocol::FOPEN_KEEP_CACHE;

        let req = Request::new(&READ_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(READ_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 15);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::Read { arg } => {
                assert_eq!(arg.fh, 0x10);
                assert_eq!(arg.offset, 0x0a);
                assert_eq!(arg.size, 0x10);
                #[cfg(feature = "abi-7-9")]
                {
                    assert_eq!(arg.read_flags, 0);
                    assert_eq!(arg.lock_owner, 0x1234);
                    assert_eq!(arg.flags, FOPEN_KEEP_CACHE);
                }
            }
            _ => panic!("unexpected request operation"),
        }
    }

    #[cfg(not(feature = "abi-7-9"))]
    define_payload! {
        WRITE_REQUEST;
        len: 72;
        opcode: 16;
        u64: 0x10,         // fh
        u64: 0x0a,         // offset
        u32: 0x10,         // size
        u32: 0,            // write_flags
        str: b"foo, bar",  // data
    }

    #[cfg(feature = "abi-7-9")]
    define_payload! {
        WRITE_REQUEST;
        len: 88;
        opcode: 16;
        u64: 0x10,         // fh
        u64: 0x0a,         // offset
        u32: 0x10,         // size
        u32: 0b11,         // write_flags
        u64: 0x1234,       // lock_owner
        u32: 2,            // flags
        u32: 0,            // padding
        str: b"foo, bar",  // data
    }

    #[test]
    fn write() {
        #[cfg(feature = "abi-7-9")]
        use super::super::protocol::{FOPEN_KEEP_CACHE, FUSE_WRITE_CACHE, FUSE_WRITE_LOCKOWNER};
        let req = Request::new(&WRITE_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(WRITE_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 16);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::Write { arg, data } => {
                assert_eq!(arg.fh, 0x10);
                assert_eq!(arg.offset, 0x0a);
                assert_eq!(arg.size, 0x10);
                #[cfg(not(feature = "abi-7-9"))]
                assert_eq!(arg.write_flags, 0);
                #[cfg(feature = "abi-7-9")]
                {
                    assert_eq!(arg.write_flags, FUSE_WRITE_CACHE | FUSE_WRITE_LOCKOWNER);
                    assert_eq!(arg.lock_owner, 0x1234);
                    assert_eq!(arg.flags, FOPEN_KEEP_CACHE);
                }
                assert_eq!(data, b"foo, bar");
            }
            _ => panic!("unexpected request operation"),
        }
    }

    define_payload! {
        STATFS_REQUEST;
        len: 40;
        opcode: 17;
    }

    #[test]
    fn statfs() {
        let req = Request::new(&STATFS_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(STATFS_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 17);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::StatFs => {}
            _ => panic!("unexpected request operation"),
        }
    }

    define_payload! {
        RELEASE_REQUEST;
        len: 64;
        opcode: 18;
        u64: 0x10,    // fh
        u32: 2,       // flags
        u32: 0,       // release_flags
        u64: 0x1234,  // lock_owner
    }

    #[test]
    fn release() {
        use super::super::protocol::FOPEN_KEEP_CACHE;

        let req = Request::new(&RELEASE_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(RELEASE_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 18);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::Release { arg } => {
                assert_eq!(arg.fh, 0x10);
                assert_eq!(arg.flags, FOPEN_KEEP_CACHE);
                assert_eq!(arg.release_flags, 0);
                assert_eq!(arg.lock_owner, 0x1234);
            }
            _ => panic!("unexpected request operation"),
        }
    }

    define_payload! {
        FSYNC_REQUEST;
        len: 56;
        opcode: 20;
        u64: 0x10,  // fh
        u32: 0,     // fsync_flags
        u32: 0,     // padding
    }

    #[test]
    fn fsync() {
        let req = Request::new(&FSYNC_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(FSYNC_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 20);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::FSync { arg } => {
                assert_eq!(arg.fh, 0x10);
                assert_eq!(arg.fsync_flags, 0);
            }
            _ => panic!("unexpected request operation"),
        }
    }

    define_payload! {
        SETXATTR_REQUEST;
        len: 64;
        opcode: 21;
        u32: 8,             // size
        u32: 0,             // flags
        str: b"foo.bar\0",  // name
        str: b"foo, bar",   // value
    }

    #[test]
    fn setxattr() {
        let req = Request::new(&SETXATTR_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(SETXATTR_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 21);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::SetXAttr { arg, name, value } => {
                assert_eq!(arg.size, 8);
                assert_eq!(arg.flags, 0);
                assert_eq!(name, "foo.bar");
                assert_eq!(value, b"foo, bar");
            }
            _ => panic!("unexpected request operation"),
        }
    }

    define_payload! {
        GETXATTR_REQUEST;
        len: 56;
        opcode: 22;
        u32: 0x80,          // size
        u32: 0,             // padding
        str: b"foo.bar\0",  // name
    }

    #[test]
    fn getxattr() {
        let req = Request::new(&GETXATTR_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(GETXATTR_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 22);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::GetXAttr { arg, name } => {
                assert_eq!(arg.size, 0x80);
                assert_eq!(name, "foo.bar");
            }
            _ => panic!("unexpected request operation"),
        }
    }

    define_payload! {
        LISTXATTR_REQUEST;
        len: 48;
        opcode: 23;
        u32: 0x80,  // size
        u32: 0,     // padding
    }

    #[test]
    fn listxattr() {
        let req = Request::new(&LISTXATTR_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(LISTXATTR_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 23);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::ListXAttr { arg } => {
                assert_eq!(arg.size, 0x80);
            }
            _ => panic!("unexpected request operation"),
        }
    }

    define_payload! {
        REMOVEXATTR_REQUEST;
        len: 48;
        opcode: 24;
        str: b"foo.bar\0",  // name
    }

    #[test]
    fn removexattr() {
        let req = Request::new(&REMOVEXATTR_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(REMOVEXATTR_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 24);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::RemoveXAttr { name } => {
                assert_eq!(name, "foo.bar");
            }
            _ => panic!("unexpected request operation"),
        }
    }

    define_payload! {
        FLUSH_REQUEST;
        len: 64;
        opcode: 25;
        u64: 0x10,    // fh
        u32: 0,       // unused
        u32: 0,       // padding
        u64: 0x1234,  // lock_owner
    }

    #[test]
    fn flush() {
        let req = Request::new(&FLUSH_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(FLUSH_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 25);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::Flush { arg } => {
                assert_eq!(arg.fh, 0x10);
                assert_eq!(arg.lock_owner, 0x1234);
            }
            _ => panic!("unexpected request operation"),
        }
    }

    define_payload! {
        INIT_REQUEST;
        len: 56;
        opcode: 26;
        u32: 7,                      // major
        u32: 8,                      // minor
        u32: 0x1000,                 // max_readahead
        u32: 1,                      // flags
    }

    #[test]
    fn init() {
        use super::super::protocol::FUSE_ASYNC_READ;

        let req = Request::new(&INIT_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(INIT_REQUEST.len(), req.header.len.cast::<usize>());
        assert_eq!(req.header.opcode, 26);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::Init { arg } => {
                assert_eq!(arg.major, 7);
                assert_eq!(arg.minor, 8);
                assert_eq!(arg.max_readahead, 4096);
                assert_eq!(arg.flags, FUSE_ASYNC_READ);
            }
            _ => panic!("unexpected request operation"),
        }
    }

    define_payload! {
        OPENDIR_REQUEST;
        len: 48;
        opcode: 27;
        u32: 2,                      // flags
        u32: 0o0755,                 // mode, or unused from 7.12

    }

    #[test]
    fn opendir() {
        use super::super::protocol::FOPEN_KEEP_CACHE;

        let req = Request::new(&OPENDIR_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(OPENDIR_REQUEST.len(), req.header.len.cast::<usize>());
        assert_eq!(req.header.opcode, 27);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::OpenDir { arg } => {
                assert_eq!(arg.flags, FOPEN_KEEP_CACHE);
                #[cfg(not(feature = "abi-7-12"))]
                assert_eq!(arg.mode, 0o0755);
            }
            _ => panic!("unexpected request operation"),
        }
    }

    #[cfg(not(feature = "abi-7-9"))]
    define_payload! {
        READDIR_REQUEST;
        len: 64;
        opcode: 28;
        u64: 0x10,  // fh
        u64: 0x0a,  // offset
        u32: 0x10,  // size
        u32: 0,     // padding
    }

    #[cfg(feature = "abi-7-9")]
    define_payload! {
        READDIR_REQUEST;
        len: 80;
        opcode: 28;
        u64: 0x10,   // fh
        u64: 0x0a,   // offset
        u32: 0x10,   // size
        u32: 2,      // read_flags
        u64: 0x1234, // lock_owner
        u32: 2,      // flags
        u32: 0,      // padding
    }

    #[test]
    fn readdir() {
        #[cfg(feature = "abi-7-9")]
        use super::super::protocol::{FOPEN_KEEP_CACHE, FUSE_READ_LOCKOWNER};

        let req = Request::new(&READDIR_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(READDIR_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 28);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::ReadDir { arg } => {
                assert_eq!(arg.fh, 0x10);
                assert_eq!(arg.offset, 0x0a);
                assert_eq!(arg.size, 0x10);
                #[cfg(feature = "abi-7-9")]
                {
                    assert_eq!(arg.read_flags, FUSE_READ_LOCKOWNER);
                    assert_eq!(arg.lock_owner, 0x1234);
                    assert_eq!(arg.flags, FOPEN_KEEP_CACHE);
                }
            }
            _ => panic!("unexpected request operation"),
        }
    }

    define_payload! {
        RELEASEDIR_REQUEST;
        len: 64;
        opcode: 29;
        u64: 0x10,    // fh
        u32: 2,       // flags
        u32: 0,       // release_flags
        u64: 0x1234,  // lock_owner
    }

    #[test]
    fn releasedir() {
        use super::super::protocol::FOPEN_KEEP_CACHE;

        let req = Request::new(&RELEASEDIR_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(RELEASEDIR_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 29);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::ReleaseDir { arg } => {
                assert_eq!(arg.fh, 0x10);
                assert_eq!(arg.flags, FOPEN_KEEP_CACHE);
                assert_eq!(arg.release_flags, 0);
                assert_eq!(arg.lock_owner, 0x1234);
            }
            _ => panic!("unexpected request operation"),
        }
    }

    define_payload! {
        FSYNCDIR_REQUEST;
        len: 56;
        opcode: 30;
        u64: 0x10,  // fh
        u32: 0,     // fsync_flags
        u32: 0,     // padding
    }

    #[test]
    fn fsyncdir() {
        let req = Request::new(&FSYNCDIR_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(FSYNCDIR_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 30);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::FSyncDir { arg } => {
                assert_eq!(arg.fh, 0x10);
                assert_eq!(arg.fsync_flags, 0);
            }
            _ => panic!("unexpected request operation"),
        }
    }

    #[cfg(not(feature = "abi-7-9"))]
    define_payload! {
        GETLK_REQUEST;
        len: 80;
        opcode: 31;
        u64: 0x10,  // fh
        u64: 0x11,  // owner
        u64: 0x00,  // lk.start
        u64: 0x20,  // lk.end
        u32: 1,     // lk.typ
        u32: 0xff,  // lk.pid
    }

    #[cfg(feature = "abi-7-9")]
    define_payload! {
        GETLK_REQUEST;
        len: 88;
        opcode: 31;
        u64: 0x10,  // fh
        u64: 0x11,  // owner
        u64: 0x00,  // lk.start
        u64: 0x20,  // lk.end
        u32: 1,     // lk.typ
        u32: 0xff,  // lk.pid
        u32: 1,     // lk_flags
        u32: 0,     // padding
    }

    #[test]
    fn getlk() {
        #[cfg(feature = "abi-7-9")]
        use super::super::protocol::FUSE_LK_FLOCK;

        let req = Request::new(&GETLK_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(GETLK_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 31);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::GetLk { arg } => {
                assert_eq!(arg.fh, 0x10);
                assert_eq!(arg.owner, 0x11);
                assert_eq!(arg.lk.start, 0x00);
                assert_eq!(arg.lk.end, 0x20);
                assert_eq!(arg.lk.typ, 1);
                assert_eq!(arg.lk.pid, 0xff);
                #[cfg(feature = "abi-7-9")]
                assert_eq!(arg.lk_flags, FUSE_LK_FLOCK);
            }
            _ => panic!("unexpected request operation"),
        }
    }

    #[cfg(not(feature = "abi-7-9"))]
    define_payload! {
        SETLK_REQUEST;
        len: 80;
        opcode: 32;
        u64: 0x10,  // fh
        u64: 0x11,  // owner
        u64: 0x00,  // lk.start
        u64: 0x20,  // lk.end
        u32: 1,     // lk.typ
        u32: 0xff,  // lk.pid
    }

    #[cfg(feature = "abi-7-9")]
    define_payload! {
        SETLK_REQUEST;
        len: 88;
        opcode: 32;
        u64: 0x10,  // fh
        u64: 0x11,  // owner
        u64: 0x00,  // lk.start
        u64: 0x20,  // lk.end
        u32: 1,     // lk.typ
        u32: 0xff,  // lk.pid
        u32: 1,     // lk_flags
        u32: 0,     // padding
    }

    #[test]
    fn setlk() {
        #[cfg(feature = "abi-7-9")]
        use super::super::protocol::FUSE_LK_FLOCK;

        let req = Request::new(&SETLK_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(SETLK_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 32);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::SetLk { arg } => {
                assert_eq!(arg.fh, 0x10);
                assert_eq!(arg.owner, 0x11);
                assert_eq!(arg.lk.start, 0x00);
                assert_eq!(arg.lk.end, 0x20);
                assert_eq!(arg.lk.typ, 1);
                assert_eq!(arg.lk.pid, 0xff);
                #[cfg(feature = "abi-7-9")]
                assert_eq!(arg.lk_flags, FUSE_LK_FLOCK);
            }
            _ => panic!("unexpected request operation"),
        }
    }

    #[cfg(not(feature = "abi-7-9"))]
    define_payload! {
        SETLKW_REQUEST;
        len: 80;
        opcode: 33;
        u64: 0x10,  // fh
        u64: 0x11,  // owner
        u64: 0x00,  // lk.start
        u64: 0x20,  // lk.end
        u32: 1,     // lk.typ
        u32: 0xff,  // lk.pid
    }

    #[cfg(feature = "abi-7-9")]
    define_payload! {
        SETLKW_REQUEST;
        len: 88;
        opcode: 33;
        u64: 0x10,  // fh
        u64: 0x11,  // owner
        u64: 0x00,  // lk.start
        u64: 0x20,  // lk.end
        u32: 1,     // lk.typ
        u32: 0xff,  // lk.pid
        u32: 1,     // lk_flags
        u32: 0,     // padding
    }

    #[test]
    fn setlkw() {
        #[cfg(feature = "abi-7-9")]
        use super::super::protocol::FUSE_LK_FLOCK;

        let req = Request::new(&SETLKW_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(SETLKW_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 33);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::SetLkW { arg } => {
                assert_eq!(arg.fh, 0x10);
                assert_eq!(arg.owner, 0x11);
                assert_eq!(arg.lk.start, 0x00);
                assert_eq!(arg.lk.end, 0x20);
                assert_eq!(arg.lk.typ, 1);
                assert_eq!(arg.lk.pid, 0xff);
                #[cfg(feature = "abi-7-9")]
                assert_eq!(arg.lk_flags, FUSE_LK_FLOCK);
            }
            _ => panic!("unexpected request operation"),
        }
    }

    define_payload! {
        ACCESS_REQUEST;
        len: 48;
        opcode: 34;
        u32: 0o0022,  // mask
        u32: 0,       // padding
    }

    #[test]
    fn access() {
        let req = Request::new(&ACCESS_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(ACCESS_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 34);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::Access { arg } => {
                assert_eq!(arg.mask, 0o0022);
            }
            _ => panic!("unexpected request operation"),
        }
    }

    #[cfg(not(feature = "abi-7-12"))]
    define_payload! {
        CREATE_REQUEST;
        len: 56;
        opcode: 35;
        u32: 0,             // flags
        u32: 0o0755,        // mode
        str: b"foo.txt\0",   // name
    }

    #[cfg(feature = "abi-7-12")]
    define_payload! {
        CREATE_REQUEST;
        len: 64;
        opcode: 35;
        u32: 0,             // flags
        u32: 0o0755,        // mode
        u32: 0o0022,        // umask
        u32: 0,             // padding
        str: b"foo.txt\0",   // name
    }

    #[test]
    fn create() {
        let req = Request::new(&CREATE_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(CREATE_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 35);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::Create { arg, name } => {
                assert_eq!(arg.flags, 0);
                assert_eq!(arg.mode, 0o0755);
                #[cfg(feature = "abi-7-12")]
                assert_eq!(arg.umask, 0o0022);
                assert_eq!(name, "foo.txt");
            }
            _ => panic!("unexpected request operation"),
        }
    }

    define_payload! {
        INTERRUPT_REQUEST;
        len: 48;
        opcode: 36;
        u64: 0x1234_5678,  // unique
    }

    #[test]
    fn interrupt() {
        let req = Request::new(&INTERRUPT_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(INTERRUPT_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 36);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::Interrupt { arg } => {
                assert_eq!(arg.unique, 0x1234_5678);
            }
            _ => panic!("unexpected request operation"),
        }
    }

    define_payload! {
        BMAP_REQUEST;
        len: 56;
        opcode: 37;
        u64: 0x1234_5678,  // block
        u32: 0xff,         // blocksize
        u32: 0,            // padding
    }

    #[test]
    fn bmap() {
        let req = Request::new(&BMAP_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(BMAP_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 37);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::BMap { arg } => {
                assert_eq!(arg.block, 0x1234_5678);
                assert_eq!(arg.blocksize, 0xff);
            }
            _ => panic!("unexpected request operation"),
        }
    }

    define_payload! {
        DESTROY_REQUEST;
        len: 40;
        opcode: 38;
    }

    #[test]
    fn destroy() {
        let req = Request::new(&DESTROY_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(DESTROY_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 38);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::Destroy => {}
            _ => panic!("unexpected request operation"),
        }
    }

    #[cfg(feature = "abi-7-11")]
    define_payload! {
        IOCTL_REQUEST;
        len: 80;
        opcode: 39;
        u64: 0x10,                  // fh
        u32: 4,                     // flags
        u32: 0,                     // cmd
        u64: 0x1122_3344_5566_7788, // arg
        u32: 0xa0,                  // in_size
        u32: 0xb0,                  // out_size
        str: b"foobar2k",           // data
    }

    #[test]
    #[cfg(feature = "abi-7-11")]
    fn ioctl() {
        use super::super::protocol::FUSE_IOCTL_RETRY;

        let req = Request::new(&IOCTL_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(IOCTL_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 39);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::IoCtl { arg, data } => {
                assert_eq!(arg.fh, 0x10);
                assert_eq!(arg.flags, FUSE_IOCTL_RETRY);
                assert_eq!(arg.cmd, 0);
                assert_eq!(arg.arg, 0x1122_3344_5566_7788);
                assert_eq!(arg.in_size, 0xa0);
                assert_eq!(arg.out_size, 0xb0);
                assert_eq!(data, b"foobar2k");
            }
            _ => panic!("unexpected request operation"),
        }
    }

    #[cfg(feature = "abi-7-11")]
    define_payload! {
        POLL_REQUEST;
        len: 64;
        opcode: 40;
        u64: 0x10,                  // fh
        u64: 0x20,                  // kh
        u32: 1,                     // flags
        u32: 0xfa,                  // events, or padding before 7.21
    }

    #[test]
    #[cfg(feature = "abi-7-11")]
    fn poll() {
        use super::super::protocol::FUSE_POLL_SCHEDULE_NOTIFY;

        let req = Request::new(&POLL_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(POLL_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 40);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::Poll { arg } => {
                assert_eq!(arg.fh, 0x10);
                assert_eq!(arg.kh, 0x20);
                assert_eq!(arg.flags, FUSE_POLL_SCHEDULE_NOTIFY);
                assert_eq!(arg.events, 0xfa);
            }
            _ => panic!("unexpected request operation"),
        }
    }

    #[cfg(feature = "abi-7-15")]
    define_payload! {
        NOTIFY_REPLY_REQUEST;
        len: 48;
        opcode: 41;
        str: b"foobar2k",  // data
    }

    #[test]
    #[cfg(feature = "abi-7-15")]
    fn notify_reply() {
        let req = Request::new(&NOTIFY_REPLY_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(NOTIFY_REPLY_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 41);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::NotifyReply { data } => {
                assert_eq!(data, b"foobar2k");
            }
            _ => panic!("unexpected request operation"),
        }
    }

    #[cfg(feature = "abi-7-16")]
    define_payload! {
        BATCH_FORGET_REQUEST;
        len: 80;
        opcode: 42;
        u32: 2,       // count
        u32: 0,       // dummy
        u64: 1,       // nodes[0].nodeid
        u64: 5,       // nodes[0].nlookup
        u64: 2,       // nodes[1].nodeid
        u64: 10,      // nodes[1].nlookup
    }

    #[test]
    #[cfg(feature = "abi-7-16")]
    fn batch_forget() {
        let req = Request::new(&BATCH_FORGET_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(BATCH_FORGET_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 42);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm, clippy::indexing_slicing)]
        match *req.operation() {
            Operation::BatchForget { arg, nodes } => {
                assert_eq!(arg.count, 2);
                assert!(nodes.len() >= 2);
                assert_eq!(nodes[0].nodeid, 1);
                assert_eq!(nodes[0].nlookup, 5);
                assert_eq!(nodes[1].nodeid, 2);
                assert_eq!(nodes[1].nlookup, 10);
            }
            _ => panic!("unexpected request operation"),
        }
    }

    #[cfg(feature = "abi-7-19")]
    define_payload! {
        FALLOCATE_REQUEST;
        len: 72;
        opcode: 43;
        u64: 0x10,      // fh
        u64: 0xff,      // offset
        u64: 0xff00,    // length
        u32: 0o0755,    // mode
        u32: 0,         // padding
    }

    #[test]
    #[cfg(feature = "abi-7-19")]
    fn fallocate() {
        let req = Request::new(&FALLOCATE_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(FALLOCATE_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 43);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::FAllocate { arg } => {
                assert_eq!(arg.fh, 0x10);
                assert_eq!(arg.offset, 0xff);
                assert_eq!(arg.length, 0xff00);
                assert_eq!(arg.mode, 0o0755);
            }
            _ => panic!("unexpected request operation"),
        }
    }

    #[cfg(feature = "abi-7-21")]
    define_payload! {
        READDIRPLUS_REQUEST;
        len: 80;
        opcode: 44;
        u64: 0x10,   // fh
        u64: 0x0a,   // offset
        u32: 0x10,   // size
        u32: 2,      // read_flags
        u64: 0x1234, // lock_owner
        u32: 2,      // flags
        u32: 0,      // padding
    }

    #[test]
    #[cfg(feature = "abi-7-21")]
    fn readdirplus() {
        use super::super::protocol::{FOPEN_KEEP_CACHE, FUSE_READ_LOCKOWNER};

        let req = Request::new(&READDIRPLUS_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(READDIRPLUS_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 44);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::ReadDirPlus { arg } => {
                assert_eq!(arg.fh, 0x10);
                assert_eq!(arg.offset, 0x0a);
                assert_eq!(arg.size, 0x10);
                assert_eq!(arg.read_flags, FUSE_READ_LOCKOWNER);
                assert_eq!(arg.lock_owner, 0x1234);
                assert_eq!(arg.flags, FOPEN_KEEP_CACHE);
            }
            _ => panic!("unexpected request operation"),
        }
    }

    #[cfg(feature = "abi-7-23")]
    define_payload! {
        RENAME2_REQUEST;
        len: 72;
        opcode: 45;
        u64: 0x10,          // newdir
        u32: 2,             // flags
        u32: 0,             // padding
        str: b"foo.txt\0",  // oldname
        str: b"bar.txt\0",  // newname
    }

    #[test]
    #[cfg(feature = "abi-7-23")]
    fn rename2() {
        use libc::RENAME_EXCHANGE;

        let req = Request::new(&RENAME2_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(RENAME2_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 45);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::Rename2 {
                arg,
                oldname,
                newname,
            } => {
                assert_eq!(arg.newdir, 0x10);
                assert_eq!(arg.flags, RENAME_EXCHANGE);
                assert_eq!(oldname, "foo.txt");
                assert_eq!(newname, "bar.txt");
            }
            _ => panic!("unexpected request operation"),
        }
    }

    // TODO: Check ABI above 7.24, and mark them.

    define_payload! {
        LSEEK_REQUEST;
        len: 64;
        opcode: 46;
        u64: 0x10,          // fh
        u64: 0xff,          // offset
        u32: 0x1f,          // whence
        u32: 0,             // padding
    }

    #[test]
    fn lseek() {
        let req = Request::new(&LSEEK_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(LSEEK_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 46);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::LSeek { arg } => {
                assert_eq!(arg.fh, 0x10);
                assert_eq!(arg.offset, 0xff);
                assert_eq!(arg.whence, 0x1f);
            }
            _ => panic!("unexpected request operation"),
        }
    }

    define_payload! {
        COPY_FILE_RANGE_REQUEST;
        len: 96;
        opcode: 47;
        u64: 0x10,          // fh_in
        u64: 0xff,          // off_in
        u64: 0x20,          // nodeid_out
        u64: 0x11,          // fh_out
        u64: 0xff,          // off_out
        u64: 0x10ff,        // len
        u64: 0,             // flags
    }

    #[test]
    fn copy_file_range() {
        let req = Request::new(&COPY_FILE_RANGE_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(COPY_FILE_RANGE_REQUEST.len(), req.len().cast::<usize>());
        assert_eq!(req.header.opcode, 47);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::CopyFileRange { arg } => {
                assert_eq!(arg.fh_in, 0x10);
                assert_eq!(arg.off_in, 0xff);
                assert_eq!(arg.nodeid_out, 0x20);
                assert_eq!(arg.fh_out, 0x11);
                assert_eq!(arg.off_out, 0xff);
                assert_eq!(arg.len, 0x10ff);
                assert_eq!(arg.flags, 0);
            }
            _ => panic!("unexpected request operation"),
        }
    }

    #[cfg(feature = "abi-7-11")]
    define_payload! {
        CUSE_INIT_REQUEST;
        len: 56;
        opcode: 4096;
        u32: 7,                      // major
        u32: 11,                     // minor
        u32: 0x1000,                 // max_readahead
        u32: 0,                      // flags
    }

    #[test]
    #[cfg(feature = "abi-7-11")]
    fn cuse_init() {
        let req = Request::new(&CUSE_INIT_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(CUSE_INIT_REQUEST.len(), req.header.len.cast::<usize>());
        assert_eq!(req.header.opcode, 4096);
        check_header(&req);

        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::CuseInit { arg } => {
                assert_eq!(arg.major, 7);
                assert_eq!(arg.minor, 11);
                assert_eq!(arg.max_readahead, 4096);
            }
            _ => panic!("unexpected request operation"),
        }
    }
}
