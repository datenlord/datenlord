//! The implementation for FUSE request

use std::fmt;

use clippy_utilities::Cast;
use tracing::warn;

use super::context::ProtoVersion;
use super::de::Deserializer;
#[cfg(target_os = "macos")]
use super::protocol::FuseExchangeIn;
#[cfg(feature = "abi-7-23")]
use super::protocol::FuseRename2In;
use super::protocol::{
    FuseAccessIn, FuseBMapIn, FuseBatchForgetIn, FuseCopyFileRangeIn, FuseCreateIn,
    FuseFAllocateIn, FuseFSyncIn, FuseFlushIn, FuseForgetIn, FuseForgetOne, FuseGetXAttrIn,
    FuseInHeader, FuseInitIn, FuseInterruptIn, FuseIoCtlIn, FuseLSeekIn, FuseLinkIn, FuseLockIn,
    FuseMkDirIn, FuseMkNodIn, FuseOpCode, FuseOpenIn, FusePollIn, FuseReadIn, FuseReleaseIn,
    FuseRenameIn, FuseSetAttrIn, FuseSetXAttrIn, FuseWriteIn,
};

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
    // #[cfg(feature = "abi-7-11")]
    IoCtl {
        /// The FUSE ioctl request
        arg: &'a FuseIoCtlIn,
        /// The ioctl request data
        data: &'a [u8],
    },
    /// FUSE_POLL = 40
    // #[cfg(feature = "abi-7-11")]
    Poll {
        /// The FUSE poll request
        arg: &'a FusePollIn,
    },
    /// FUSE_NOTIFY_REPLY = 41
    // #[cfg(feature = "abi-7-15")]
    NotifyReply {
        /// FUSE notify reply data
        data: &'a [u8],
    },
    /// FUSE_BATCH_FORGET = 42
    // #[cfg(feature = "abi-7-16")]
    BatchForget {
        /// The FUSE batch forget request
        arg: &'a FuseBatchForgetIn,
        /// The slice of nodes to forget
        nodes: &'a [FuseForgetOne],
    },
    /// FUSE_FALLOCATE = 43
    // #[cfg(feature = "abi-7-19")]
    FAllocate {
        /// The FUSE fallocate request
        arg: &'a FuseFAllocateIn,
    },
    /// FUSE_READDIRPLUS = 44,
    // #[cfg(feature = "abi-7-21")]
    ReadDirPlus {
        /// The FUSE read directory plus request
        arg: &'a FuseReadIn,
    },
    /// FUSE_RENAME2 = 45,
    ///
    /// Available when the protocol version is greater than 7.22.
    /// This is checked by the kernel so that DatenLord won't receive such a request.
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
    /// FUSE_SETVOLNAME = 61
    #[cfg(target_os = "macos")]
    SetVolName {
        /// Volume name to set
        name: &'a str,
    },
    /// FUSE_GETXTIMES = 62
    #[cfg(target_os = "macos")]
    GetXTimes,
    /// FUSE_EXCHANGE = 63
    #[cfg(target_os = "macos")]
    Exchange {
        /// The FUSE exchange request
        arg: &'a FuseExchangeIn,
        /// The old file name
        oldname: &'a str,
        /// The new file name
        newname: &'a str,
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
    ) -> anyhow::Result<Self> {
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
            // #[cfg(feature = "abi-7-11")]
            39 => FuseOpCode::FUSE_IOCTL,
            // #[cfg(feature = "abi-7-11")]
            40 => FuseOpCode::FUSE_POLL,
            // #[cfg(feature = "abi-7-15")]
            41 => FuseOpCode::FUSE_NOTIFY_REPLY,
            // #[cfg(feature = "abi-7-16")]
            42 => FuseOpCode::FUSE_BATCH_FORGET,
            // #[cfg(feature = "abi-7-19")]
            43 => FuseOpCode::FUSE_FALLOCATE,
            // #[cfg(feature = "abi-7-21")]
            44 => FuseOpCode::FUSE_READDIRPLUS,
            #[cfg(feature = "abi-7-23")]
            // https://github.com/torvalds/linux/blob/8f6f76a6a29f36d2f3e4510d0bde5046672f6924/fs/fuse/dir.c#L1077C2-L1088C3
            45 => FuseOpCode::FUSE_RENAME2,
            // #[cfg(feature = "abi-7-24")]
            46 => FuseOpCode::FUSE_LSEEK,
            // #[cfg(feature = "abi-7-28")]
            47 => FuseOpCode::FUSE_COPY_FILE_RANGE,

            #[cfg(target_os = "macos")]
            61 => FuseOpCode::FUSE_SETVOLNAME,
            #[cfg(target_os = "macos")]
            62 => FuseOpCode::FUSE_GETXTIMES,
            #[cfg(target_os = "macos")]
            63 => FuseOpCode::FUSE_EXCHANGE,

            #[cfg(feature = "abi-7-11")]
            4096 => FuseOpCode::CUSE_INIT,

            _ => panic!("unknown FUSE OpCode={n}"),
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
            // #[cfg(feature = "abi-7-11")]
            FuseOpCode::FUSE_IOCTL => Operation::IoCtl {
                arg: data.fetch_ref()?,
                data: data.fetch_all_bytes(),
            },
            // #[cfg(feature = "abi-7-11")]
            FuseOpCode::FUSE_POLL => Operation::Poll {
                arg: data.fetch_ref()?,
            },
            // #[cfg(feature = "abi-7-15")]
            FuseOpCode::FUSE_NOTIFY_REPLY => Operation::NotifyReply {
                data: data.fetch_all_bytes(),
            },
            // #[cfg(feature = "abi-7-16")]
            FuseOpCode::FUSE_BATCH_FORGET => Operation::BatchForget {
                arg: data.fetch_ref()?,
                nodes: data.fetch_all_as_slice()?,
            },
            // #[cfg(feature = "abi-7-19")]
            FuseOpCode::FUSE_FALLOCATE => Operation::FAllocate {
                arg: data.fetch_ref()?,
            },
            // #[cfg(feature = "abi-7-21")]
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

            #[cfg(target_os = "macos")]
            FuseOpCode::FUSE_SETVOLNAME => Operation::SetVolName {
                name: data.fetch_str()?,
            },
            #[cfg(target_os = "macos")]
            FuseOpCode::FUSE_GETXTIMES => Operation::GetXTimes,
            #[cfg(target_os = "macos")]
            FuseOpCode::FUSE_EXCHANGE => Operation::Exchange {
                arg: data.fetch_ref()?,
                oldname: data.fetch_str()?,
                newname: data.fetch_str()?,
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

            // #[cfg(feature = "abi-7-11")]
            Operation::IoCtl { arg, data } => write!(
                f,
                "IOCTL fh={}, flags {:#x}, cmd={}, arg={}, data={:?}",
                arg.fh, arg.flags, arg.cmd, arg.arg, data,
            ),
            // #[cfg(feature = "abi-7-11")]
            Operation::Poll { arg } => {
                write!(f, "POLL fh={}, kh={}, flags={:#x} ", arg.fh, arg.kh, arg.flags)
            }
            // #[cfg(feature = "abi-7-15")]
            Operation::NotifyReply { data } => write!(f, "NOTIFY REPLY data={data:?}"),
            // #[cfg(feature = "abi-7-16")]
            Operation::BatchForget { arg, nodes } => {
                write!(f, "BATCH FORGOT count={}, nodes={:?}", arg.count, nodes)
            }
            // #[cfg(feature = "abi-7-19")]
            Operation::FAllocate { arg } => write!(
                f,
                "FALLOCATE fh={}, offset={}, length={}, mode={:#05o}",
                arg.fh, arg.offset, arg.length, arg.mode,
            ),
            // #[cfg(feature = "abi-7-21")]
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
            "FUSE({:3}) ino={:#018x} opcode={} operation={}",
            self.header.unique, self.header.nodeid, self.header.opcode, self.operation,
        )
    }
}

impl<'a> Request<'a> {
    /// Build FUSE request
    pub fn new(bytes: &'a [u8], proto_version: ProtoVersion) -> anyhow::Result<Self> {
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
        let operation = Operation::parse(header.opcode, &mut de, proto_version)?;
        if de.remaining_len() > 0 {
            warn!(
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
    use aligned_utils::stack::Align8;
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

    #[cfg(target_endian = "big")]
    const INIT_REQUEST: Align8<[u8; 56]> = Align8([
        0x00, 0x00, 0x00, 0x38, 0x00, 0x00, 0x00, 0x1a, // len, opcode
        0xde, 0xad, 0xbe, 0xef, 0xba, 0xad, 0xd0, 0x0d, // unique
        0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, // nodeid
        0xc0, 0x01, 0xd0, 0x0d, 0xc0, 0x01, 0xca, 0xfe, // uid, gid
        0xc0, 0xde, 0xba, 0x5e, 0x00, 0x00, 0x00, 0x00, // pid, padding
        0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x08, // major, minor
        0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, // max_readahead, flags
    ]);

    #[cfg(target_endian = "little")]
    const INIT_REQUEST: Align8<[u8; 56]> = Align8([
        0x38, 0x00, 0x00, 0x00, 0x1a, 0x00, 0x00, 0x00, // len, opcode
        0x0d, 0xf0, 0xad, 0xba, 0xef, 0xbe, 0xad, 0xde, // unique
        0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, // nodeid
        0x0d, 0xd0, 0x01, 0xc0, 0xfe, 0xca, 0x01, 0xc0, // uid, gid
        0x5e, 0xba, 0xde, 0xc0, 0x00, 0x00, 0x00, 0x00, // pid, padding
        0x07, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, // major, minor
        0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // max_readahead, flags
    ]);

    #[cfg(all(target_endian = "big", not(feature = "abi-7-12")))]
    const MKNOD_REQUEST: Align8<[u8; 56]> = Align8([
        0x00, 0x00, 0x00, 0x38, 0x00, 0x00, 0x00, 0x08, // len, opcode
        0xde, 0xad, 0xbe, 0xef, 0xba, 0xad, 0xd0, 0x0d, // unique
        0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, // nodeid
        0xc0, 0x01, 0xd0, 0x0d, 0xc0, 0x01, 0xca, 0xfe, // uid, gid
        0xc0, 0xde, 0xba, 0x5e, 0x00, 0x00, 0x00, 0x00, // pid, padding
        0x00, 0x00, 0x01, 0xa4, 0x00, 0x00, 0x00, 0x00, // mode, rdev
        0x66, 0x6f, 0x6f, 0x2e, 0x74, 0x78, 0x74, 0x00, // name
    ]);

    /// `MKNOD_REQUEST` for protocol version less then 7.12
    #[cfg(all(target_endian = "little", not(feature = "abi-7-12")))]
    const MKNOD_REQUEST: Align8<[u8; 56]> = Align8([
        0x38, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, // len, opcode
        0x0d, 0xf0, 0xad, 0xba, 0xef, 0xbe, 0xad, 0xde, // unique
        0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, // nodeid
        0x0d, 0xd0, 0x01, 0xc0, 0xfe, 0xca, 0x01, 0xc0, // uid, gid
        0x5e, 0xba, 0xde, 0xc0, 0x00, 0x00, 0x00, 0x00, // pid, padding
        0xa4, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // mode, rdev
        0x66, 0x6f, 0x6f, 0x2e, 0x74, 0x78, 0x74, 0x00, // name
    ]);

    #[cfg(all(target_endian = "big", feature = "abi-7-12"))]
    const MKNOD_REQUEST: Align8<[u8; 64]> = Align8([
        0x00, 0x00, 0x00, 0x38, 0x00, 0x00, 0x00, 0x08, // len, opcode
        0xde, 0xad, 0xbe, 0xef, 0xba, 0xad, 0xd0, 0x0d, // unique
        0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, // nodeid
        0xc0, 0x01, 0xd0, 0x0d, 0xc0, 0x01, 0xca, 0xfe, // uid, gid
        0xc0, 0xde, 0xba, 0x5e, 0x00, 0x00, 0x00, 0x00, // pid, padding
        0x00, 0x00, 0x01, 0xa4, 0x00, 0x00, 0x00, 0x00, // mode, rdev
        0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, // umsak, padding
        0x66, 0x6f, 0x6f, 0x2e, 0x74, 0x78, 0x74, 0x00, // name
    ]);

    /// `MKNOD_REQUEST` for protocol version greater then 7.11
    #[cfg(all(target_endian = "little", feature = "abi-7-12"))]
    const MKNOD_REQUEST: Align8<[u8; 64]> = Align8([
        0x38, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, // len, opcode
        0x0d, 0xf0, 0xad, 0xba, 0xef, 0xbe, 0xad, 0xde, // unique
        0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, // nodeid
        0x0d, 0xd0, 0x01, 0xc0, 0xfe, 0xca, 0x01, 0xc0, // uid, gid
        0x5e, 0xba, 0xde, 0xc0, 0x00, 0x00, 0x00, 0x00, // pid, padding
        0xa4, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // mode, rdev
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // umsak, padding
        0x66, 0x6f, 0x6f, 0x2e, 0x74, 0x78, 0x74, 0x00, // name
    ]);

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
        let mut chain = err.chain();
        assert_eq!(
            chain
                .next()
                .unwrap_or_else(|| panic!("failed to get next error"))
                .downcast_ref::<DeserializeError>()
                .unwrap_or_else(|| panic!("error type not match")),
            &DeserializeError::NotEnough
        );
        assert!(chain.next().is_none());
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

    #[test]
    fn init() {
        let req = Request::new(&INIT_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(req.header.len, 56);
        assert_eq!(req.header.opcode, 26);
        assert_eq!(req.unique(), 0xdead_beef_baad_f00d);
        assert_eq!(req.nodeid(), 0x1122_3344_5566_7788);
        assert_eq!(req.uid(), 0xc001_d00d);
        assert_eq!(req.gid(), 0xc001_cafe);
        assert_eq!(req.pid(), 0xc0de_ba5e);
        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
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
        let req = Request::new(&MKNOD_REQUEST[..], PROTO_VERSION)
            .unwrap_or_else(|err| panic!("failed to build FUSE request, the error is: {err}"));
        assert_eq!(req.header.len, 56);
        assert_eq!(req.header.opcode, 8);
        assert_eq!(req.unique(), 0xdead_beef_baad_f00d);
        assert_eq!(req.nodeid(), 0x1122_3344_5566_7788);
        assert_eq!(req.uid(), 0xc001_d00d);
        assert_eq!(req.gid(), 0xc001_cafe);
        assert_eq!(req.pid(), 0xc0de_ba5e);
        #[allow(clippy::wildcard_enum_match_arm)]
        match *req.operation() {
            Operation::MkNod { arg, name } => {
                assert_eq!(arg.mode, 0o644);
                #[cfg(feature = "abi-7-12")]
                {
                    assert_eq!(arg.umask, 0o002);
                }
                assert_eq!(name, "foo.txt");
            }
            _ => panic!("unexpected request operation"),
        }
    }
}
