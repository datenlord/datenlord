use serde::{Deserialize, Serialize};

use crate::fs::datenlordfs::direntry::DirEntry;
use crate::fs::datenlordfs::s3_node::S3Node;
use crate::fs::datenlordfs::serial::SerialNode;
use crate::fs::datenlordfs::S3MetaData;

/// The `ValueType` is used to provide support for metadata.
///
/// The variants `DirEntry`, `INum` and `Attr` are not used currently,
/// but preserved for the future.
#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub enum ValueType {
    /// SerialNode
    Node(SerialNode),
    /// SerialDirEntry
    DirEntry(DirEntry),
    /// Next id allocate range begin
    NextIdAllocateRangeBegin(u64),
    /// Raw value
    Raw(Vec<u8>),
    /// String value
    String(String),
}

impl ValueType {
    #[allow(dead_code, clippy::wildcard_enum_match_arm)] // Allow wildcard because there should be only one enum branch matches one specific type.
    /// Turn the `ValueType` into `SerialNode` then into `S3Node`.
    /// # Panics
    /// Panics if `ValueType` is not `ValueType::Node`.
    pub fn into_s3_node(self, meta: &S3MetaData) -> S3Node {
        match self {
            ValueType::Node(node) => S3Node::from_serial_node(node, meta),
            _ => {
                panic!("expect ValueType::Node but get {self:?}");
            }
        }
    }

    /// Turn the `ValueType` into `NextIdAllocateRangeBegin`.
    /// # Panics
    /// Panics if `ValueType` is not `ValueType::NextIdAllocateRangeBegin`.
    #[allow(clippy::wildcard_enum_match_arm)] // Allow wildcard because there should be only one enum branch matches one specific type.
    #[must_use]
    pub fn into_next_id_allocate_range_begin(self) -> u64 {
        match self {
            ValueType::NextIdAllocateRangeBegin(begin) => begin,
            _ => panic!("expect ValueType::NextIdAllocateRangeBegin but get {self:?}"),
        }
    }

    /// Turn the `ValueType` into `String`
    /// # Panics
    /// Panics if `ValueType` is not `ValueType::String`.
    #[allow(clippy::wildcard_enum_match_arm)] // Allow wildcard because there should be only one enum branch matches one specific type.
    #[must_use]
    pub fn into_string(self) -> String {
        match self {
            ValueType::String(string) => string,
            _ => panic!("expect ValueType::String but get {self:?}"),
        }
    }

    /// Turn the `ValueType` into `Raw`
    /// # Panics
    /// Panics if `ValueType` is not `ValueType::Raw`.
    #[allow(clippy::wildcard_enum_match_arm)] // Allow wildcard because there should be only one enum branch matches one specific type.
    #[must_use]
    pub fn into_raw(self) -> Vec<u8> {
        match self {
            ValueType::Raw(raw) => raw,
            _ => panic!("expect ValueType::Raw but get {self:?}"),
        }
    }

    /// Turn the `ValueType` into `DirEntry`
    /// # Panics
    /// Panics if `ValueType` is not `ValueType::DirEntry`.
    #[allow(clippy::wildcard_enum_match_arm)] // Allow wildcard because there should be only one enum branch matches one specific type.
    #[must_use]
    pub fn into_dir_entry(self) -> DirEntry {
        match self {
            ValueType::DirEntry(direntry) => direntry,
            _ => panic!("expect ValueType::DirEntry but get {self:?}"),
        }
    }
}
