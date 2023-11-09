//! Internal unsafe marker for FUSE ABI types

use std::{mem, slice};

/// FUSE ABI types.
///
/// It is safe to transmute a `&[u8]` to `&T` where `T: FuseAbiData + Sized`.
///
/// [`FuseAbiData`] can not be implemented for ZSTs.
///
/// [`FuseAbiData`] can be implemented for DSTs but there is no way to construct
/// a custom DST reference.
pub unsafe trait FuseAbiData {}

/// # Safety
/// T muse not be changed during the lifetime of `&[u8]`
#[allow(dead_code)] // TODO
#[inline]
pub unsafe fn as_bytes_unchecked<T: Sized>(raw: &T) -> &[u8] {
    let ty_size = mem::size_of::<T>();
    let base: *const u8 = <*const T>::cast(raw);
    slice::from_raw_parts(base, ty_size)
}

/// Transmutes `&T` to `&[u8]` where `T: FuseAbiData + Sized`
#[allow(dead_code)] // TODO
#[inline]
pub fn as_abi_bytes<T: FuseAbiData + Sized>(raw: &T) -> &[u8] {
    unsafe { as_bytes_unchecked(raw) }
}

/// Impl `FuseAbiData` trait
macro_rules! mark_abi_type {
    ($ty:ty) => {
        unsafe impl FuseAbiData for $ty {}
    };
}

/// Impl `FuseAbiData` for sized types
macro_rules! mark_sized_types {
    (@kernel size_check: $name: ident, $($ty:ident,)+) => {
        $(
            mark_abi_type!(super::protocol::$ty);
        )+

        #[test]
        fn $name() {
            $(
                assert!(mem::size_of::<super::protocol::$ty>() > 0); // ZST makes no sense
            )+
            $(
                assert!(mem::size_of::<super::protocol::$ty>() <= 256); // detect large types
            )+
        }
    };

    (@primitive $($ty:ty,)+) => {
        $(
            mark_abi_type!($ty);
        )+
    }
}

mark_sized_types!(@primitive
    u8,
    u16,
    u32,
    u64,
    usize,
    i8,
    i16,
    i32,
    i64,
    isize,
);

mark_sized_types!(@kernel size_check: check_common,
    FuseAttr,
    FuseKStatFs,
    FuseFileLock,
    FuseEntryOut,
    FuseForgetIn,
    FuseAttrOut,
    FuseMkNodIn,
    FuseMkDirIn,
    FuseRenameIn,
    FuseLinkIn,
    FuseSetAttrIn,
    FuseOpenIn,
    FuseCreateIn,
    FuseOpenOut,
    FuseReleaseIn,
    FuseFlushIn,
    FuseReadIn,
    FuseWriteIn,
    FuseWriteOut,
    FuseStatFsOut,
    FuseFSyncIn,
    FuseSetXAttrIn,
    FuseGetXAttrIn,
    FuseGetXAttrOut,
    FuseLockIn,
    FuseLockOut,
    FuseAccessIn,
    FuseInitIn,
    FuseInitOut,
    FuseInterruptIn,
    FuseBMapIn,
    FuseBMapOut,
    FuseInHeader,
    FuseOutHeader,
    FuseDirEnt,
);

// TODO: remove this after implement corresponding FUSE operations
// ---
// FuseIoCtlIn,
// FusePollIn,
// FuseBatchForgetIn,
// FuseForgetOne,
// ---

#[cfg(feature = "abi-7-9")]
mark_sized_types! {@kernel size_check: check_abi_7_9,
    FuseGetAttrIn,
}

#[cfg(feature = "abi-7-11")]
mark_sized_types! {@kernel size_check: check_abi_7_11,
    CuseInitIn,
    CuseInitOut,
    FuseIoCtlIn,
    FuseIoCtlOut,
    FusePollIn,
    FusePollOut,
    FuseNotifyPollWakeUpOut,
}

#[cfg(feature = "abi-7-12")]
mark_sized_types! {@kernel size_check: check_abi_7_12,
    FuseNotifyInvalEntryOut,
    FuseNotifyInvalINodeOut,
}

#[cfg(feature = "abi-7-15")]
mark_sized_types! {@kernel size_check: check_abi_7_15,
    FuseNotifyRetrieveOut,
    FuseNotifyRetrieveIn,
    FuseNotifyStoreOut,
}

#[cfg(feature = "abi-7-16")]
mark_sized_types! {@kernel size_check: check_abi_7_16,
    FuseForgetOne,
    FuseBatchForgetIn,
    FuseIoCtlIoVec,
}

#[cfg(feature = "abi-7-18")]
mark_sized_types! {@kernel size_check: check_abi_7_18,
    FuseNotifyDeleteOut,
}

#[cfg(feature = "abi-7-19")]
mark_sized_types! {@kernel size_check: check_abi_7_19,
    FuseFAllocateIn,
}

#[cfg(feature = "abi-7-21")]
mark_sized_types! {@kernel size_check: check_abi_7_21,
    FuseDirEntPlus,
}

#[cfg(feature = "abi-7-23")]
mark_sized_types! {@kernel size_check: check_abi_7_23,
    FuseRename2In,
}

// #[cfg(feature = "abi-7-24")]
mark_sized_types! {@kernel size_check: check_abi_7_24,
    FuseLSeekIn,
    FuseLSeekOut,
}

// #[cfg(feature = "abi-7-28")]
mark_sized_types! {@kernel size_check: check_abi_7_28,
    FuseCopyFileRangeIn,
}
