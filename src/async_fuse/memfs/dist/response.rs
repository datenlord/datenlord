//! Response between caches

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::super::dir::DirEntry;
use super::super::fs_util::FileAttr;
use super::super::serial::{self, SerialDirEntry, SerialFileAttr};
use super::request::Index;

/// Cache operation response
#[allow(variant_size_differences)]
#[derive(Serialize, Deserialize, Debug)]
pub enum CacheResponse {
    /// Turn off server response
    TurnOff(bool),
    /// Invalidate cache response
    Invalidate(bool),
}

/// Serialize dir entry map
fn serialize_direntry_map(map: &BTreeMap<String, DirEntry>) -> Vec<u8> {
    let mut target: BTreeMap<String, SerialDirEntry> = BTreeMap::new();

    // Checked before
    for (name, entry) in map {
        target.insert(name.clone(), serial::dir_entry_to_serial(entry));
    }

    let target = Some(target);

    bincode::serialize(&target)
        .unwrap_or_else(|e| panic!("fail to serialize `LoadDir` response, {e}"))
}

/// Deserialize dir entry map
fn deserialize_direntry_map(bin: &[u8]) -> Option<BTreeMap<String, DirEntry>> {
    let map_opt: Option<BTreeMap<String, SerialDirEntry>> = bincode::deserialize(bin)
        .unwrap_or_else(|e| panic!("fail to deserialize DirEntry Map, {e}"));

    if let Some(map) = map_opt {
        let mut target: BTreeMap<String, DirEntry> = BTreeMap::new();

        // Checked before
        for (ref name, ref entry) in map {
            target.insert(name.clone(), serial::serial_to_dir_entry(entry));
        }

        Some(target)
    } else {
        None
    }
}

/// Serialize `TurnOff` response
#[must_use]
pub fn turnoff() -> Vec<u8> {
    bincode::serialize(&CacheResponse::TurnOff(true))
        .unwrap_or_else(|e| panic!("fail to serialize `Turnoff` response, {e}"))
}

/// Serialize `Invalidate` response
#[must_use]
pub fn invalidate() -> Vec<u8> {
    bincode::serialize(&CacheResponse::Invalidate(true))
        .unwrap_or_else(|e| panic!("fail to serialize `Invalidate` response, {e}"))
}

/// Serialize `CheckAvailable` response
#[must_use]
pub fn check_available(index: &Option<Vec<Index>>) -> Vec<u8> {
    bincode::serialize(&index)
        .unwrap_or_else(|e| panic!("fail to serialize `CheckAvailable` response, {e}"))
}

/// Deserialize `CheckAvailable` response
#[must_use]
pub fn deserialize_check_available(bin: &[u8]) -> Option<Vec<Index>> {
    bincode::deserialize(bin)
        .unwrap_or_else(|e| panic!("fail to deserialize `CheckAvailable` response, {e}"))
}

/// Serialize `LoadDir` response for empty dir
#[must_use]
pub fn load_dir_none() -> Vec<u8> {
    let target: Option<BTreeMap<String, SerialDirEntry>> = None;
    bincode::serialize(&target)
        .unwrap_or_else(|e| panic!("fail to serialize `LoadDir` response, {e}"))
}

/// Serialize `LoadDir` response
#[must_use]
pub fn load_dir(map: &BTreeMap<String, DirEntry>) -> Vec<u8> {
    serialize_direntry_map(map)
}

/// Deserialize `LoadDir` response
#[must_use]
pub fn deserialize_load_dir(bin: &[u8]) -> Option<BTreeMap<String, DirEntry>> {
    deserialize_direntry_map(bin)
}

/// Serialize `UpdateDir` response
#[must_use]
pub fn update_dir() -> Vec<u8> {
    bincode::serialize(&true)
        .unwrap_or_else(|e| panic!("fail to serialize `UpdateDir` response, {e}"))
}

/// Serialize `GetAttr` response for empty attr
#[must_use]
pub fn get_attr_none() -> Vec<u8> {
    let serial_attr: Option<SerialFileAttr> = None;
    bincode::serialize(&serial_attr)
        .unwrap_or_else(|e| panic!("fail to serialize `GetAttr` response, {e}"))
}

/// Serialize `GetAttr` response
#[must_use]
pub fn get_attr(attr: &FileAttr) -> Vec<u8> {
    let serial_attr = Some(serial::file_attr_to_serial(attr));
    bincode::serialize(&serial_attr)
        .unwrap_or_else(|e| panic!("fail to serialize `GetAttr` response, {e}"))
}

/// Serialize `PushFileAttr` response
#[must_use]
pub fn push_attr() -> Vec<u8> {
    bincode::serialize(&true)
        .unwrap_or_else(|e| panic!("fail to serialize `PushFileAttr` response, {e}"))
}

/// Serialize `Rename` response
#[must_use]
pub fn rename() -> Vec<u8> {
    bincode::serialize(&true).unwrap_or_else(|e| panic!("fail to serialize `Rename` response, {e}"))
}

/// Serialize `Remove` response
#[must_use]
pub fn remove() -> Vec<u8> {
    bincode::serialize(&true).unwrap_or_else(|e| panic!("fail to serialize `Remove` response, {e}"))
}

/// Deserialize get attr response
#[must_use]
pub fn deserialize_get_attr(bin: &[u8]) -> Option<FileAttr> {
    let attr_opt: Option<SerialFileAttr> = bincode::deserialize(bin)
        .unwrap_or_else(|e| panic!("fail to deserialize DirEntry Map, {e}"));

    attr_opt.map(|attr| serial::serial_to_file_attr(&attr))
}
