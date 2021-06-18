//! Request between caches

use super::super::dir::DirEntry;
use super::types::{self, SerialDirEntry, SerialFileAttr};
use log::info;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub(crate) enum DistRequest {
    TurnOff,
    Invalidate(OpArgs),
    CheckAvailable(OpArgs),
    Read(OpArgs),
    GetFileAttr(String),
    PushFileAttr((String, SerialFileAttr)),
    LoadDir(String),
    UpdateDir(UpdateDirArgs),
    RemoveDirEntry(RemoveDirEntryArgs),
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct UpdateDirArgs {
    pub(crate) parent_path: String,
    pub(crate) child_name: String,
    pub(crate) entry: SerialDirEntry,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct RemoveDirEntryArgs {
    pub(crate) parent_path: String,
    pub(crate) child_name: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) enum Index {
    Point(usize),
    Range(usize, usize),
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct OpArgs {
    pub(crate) file_name: Vec<u8>,
    pub(crate) index: Vec<Index>,
}

pub(crate) fn turnoff() -> Vec<u8> {
    info!("dist turnoff");
    bincode::serialize(&DistRequest::TurnOff).unwrap_or_else(|e| {
        panic!(
            "fail to serialize `Turnoff` distributed cache operation, {}",
            e
        )
    })
}

pub(crate) fn invalidate(file_name: Vec<u8>, index: Vec<Index>) -> Vec<u8> {
    info!("dist invalidate");
    bincode::serialize(&DistRequest::Invalidate(OpArgs { file_name, index })).unwrap_or_else(|e| {
        panic!(
            "fail to serialize `Invalidate` distributed cache operationi, {}",
            e
        )
    })
}

pub(crate) fn check_available(file_name: Vec<u8>, index: Vec<Index>) -> Vec<u8> {
    bincode::serialize(&DistRequest::CheckAvailable(OpArgs { file_name, index })).unwrap_or_else(
        |e| {
            panic!(
                "fail to serialize `CheckAvailable` distributed cache operation, {}",
                e
            )
        },
    )
}

pub(crate) fn read(file_name: Vec<u8>, index: Vec<Index>) -> Vec<u8> {
    bincode::serialize(&DistRequest::Read(OpArgs { file_name, index })).unwrap_or_else(|e| {
        panic!(
            "fail to serialize `Read` distributed cache operation, {}",
            e
        )
    })
}

pub(crate) fn deserialize_cache(bin: &[u8]) -> DistRequest {
    bincode::deserialize(bin)
        .unwrap_or_else(|e| panic!("fail to deserialize distributed cache operation, {}", e))
}

pub(crate) fn load_dir(path: &str) -> Vec<u8> {
    bincode::serialize(&DistRequest::LoadDir(path.to_owned())).unwrap_or_else(|e| {
        panic!(
            "fail to serialize `LoadDir` distributed meta operation, {}",
            e
        )
    })
}

pub(crate) fn update_dir(parent: &str, child: &str, entry: &DirEntry) -> Vec<u8> {
    let args = UpdateDirArgs {
        parent_path: parent.to_owned(),
        child_name: child.to_owned(),
        entry: types::dir_entry_to_serial(entry),
    };

    bincode::serialize(&DistRequest::UpdateDir(args)).unwrap_or_else(|e| {
        panic!(
            "fail to serialize `UpdateDir` distributed meta operation, {}",
            e
        )
    })
}

pub(crate) fn remove_dir_entry(parent: &str, child: &str) -> Vec<u8> {
    let args = RemoveDirEntryArgs {
        parent_path: parent.to_owned(),
        child_name: child.to_owned(),
    };

    bincode::serialize(&DistRequest::RemoveDirEntry(args)).unwrap_or_else(|e| {
        panic!(
            "fail to serialize `RemoveDirEntry` distributed meta operation, {}",
            e
        )
    })
}

pub(crate) fn get_file_attr(path: &str) -> Vec<u8> {
    bincode::serialize(&DistRequest::GetFileAttr(path.to_owned())).unwrap_or_else(|e| {
        panic!(
            "fail to serialize `GetFileAttr` distributed meta operation, {}",
            e
        )
    })
}

pub(crate) fn push_file_attr(path: &str, attr: SerialFileAttr) -> Vec<u8> {
    bincode::serialize(&DistRequest::PushFileAttr((path.to_owned(), attr))).unwrap_or_else(|e| {
        panic!(
            "fail to serialize `PushFileAttr` distributed meta operation, {}",
            e
        )
    })
}
