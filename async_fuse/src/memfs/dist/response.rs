//! Response between caches

use super::super::dir::DirEntry;
use super::super::fs_util::FileAttr;
use super::request::Index;
use super::types::{self, SerialDirEntry, SerialFileAttr};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[allow(variant_size_differences)]
#[derive(Serialize, Deserialize, Debug)]
pub(crate) enum CacheResponse {
    TurnOff(bool),
    Invalidate(bool),
}

fn serialize_direntry_map(map: &BTreeMap<String, DirEntry>) -> Vec<u8> {
    let mut target: BTreeMap<String, SerialDirEntry> = BTreeMap::new();

    // Checked before
    for (name, entry) in map {
        target.insert(name.to_owned(), types::dir_entry_to_serial(entry));
    }

    let target = Some(target);

    bincode::serialize(&target)
        .unwrap_or_else(|e| panic!("fail to serialize `LoadDir` response, {}", e))
}

fn deserialize_direntry_map(bin: &[u8]) -> Option<BTreeMap<String, DirEntry>> {
    let map: Option<BTreeMap<String, SerialDirEntry>> = bincode::deserialize(bin)
        .unwrap_or_else(|e| panic!("fail to deserialize DirEntry Map, {}", e));

    if let None = map {
        return None;
    }

    let mut target: BTreeMap<String, DirEntry> = BTreeMap::new();

    // Checked before
    for (ref name, ref entry) in map.unwrap() {
        target.insert(name.to_owned(), types::serial_to_dir_entry(entry));
    }

    Some(target)
}

pub(crate) fn turnoff() -> Vec<u8> {
    bincode::serialize(&CacheResponse::TurnOff(true))
        .unwrap_or_else(|e| panic!("fail to serialize `Turnoff` response, {}", e))
}

pub(crate) fn invalidate() -> Vec<u8> {
    bincode::serialize(&CacheResponse::Invalidate(true))
        .unwrap_or_else(|e| panic!("fail to serialize `Invalidate` response, {}", e))
}

pub(crate) fn check_available(index: Option<Vec<Index>>) -> Vec<u8> {
    bincode::serialize(&index)
        .unwrap_or_else(|e| panic!("fail to serialize `CheckAvailable` response, {}", e))
}

pub(crate) fn deserialize_check_available(bin: &[u8]) -> Option<Vec<Index>> {
    bincode::deserialize(bin)
        .unwrap_or_else(|e| panic!("fail to deserialize `CheckAvailable` response, {}", e))
}

pub(crate) fn load_dir_none() -> Vec<u8> {
    let target: Option<BTreeMap<String, SerialDirEntry>> = None;
    bincode::serialize(&target)
        .unwrap_or_else(|e| panic!("fail to serialize `LoadDir` response, {}", e))
}

pub(crate) fn load_dir(map: &BTreeMap<String, DirEntry>) -> Vec<u8> {
    serialize_direntry_map(map)
}

pub(crate) fn deserialize_load_dir(bin: &[u8]) -> Option<BTreeMap<String, DirEntry>> {
    deserialize_direntry_map(bin)
}

pub(crate) fn update_dir() -> Vec<u8> {
    bincode::serialize(&true)
        .unwrap_or_else(|e| panic!("fail to serialize `UpdateDir` response, {}", e))
}

pub(crate) fn get_attr_none() -> Vec<u8> {
    let serial_attr: Option<SerialFileAttr> = None;
    bincode::serialize(&serial_attr)
        .unwrap_or_else(|e| panic!("fail to serialize `GetAtr` response, {}", e))
}

pub(crate) fn get_attr(attr: &FileAttr) -> Vec<u8> {
    let serial_attr = Some(types::file_attr_to_serial(attr));
    bincode::serialize(&serial_attr)
        .unwrap_or_else(|e| panic!("fail to serialize `GetAtr` response, {}", e))
}

pub(crate) fn push_attr() -> Vec<u8> {
    bincode::serialize(&true)
        .unwrap_or_else(|e| panic!("fail to serialize `PushFileAttr` response, {}", e))
}

pub(crate) fn deserialize_get_attr(bin: &[u8]) -> Option<FileAttr> {
    let attr: Option<SerialFileAttr> = bincode::deserialize(bin)
        .unwrap_or_else(|e| panic!("fail to deserialize DirEntry Map, {}", e));

    if let None = attr {
        return None;
    }

    // Checked before
    Some(types::serial_to_file_attr(&attr.unwrap()))
}
