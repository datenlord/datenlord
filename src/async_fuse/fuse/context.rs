//! FUSE request context

use std::fmt;

/// Protocol version
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProtoVersion {
    /// major version
    pub major: u32,
    /// minor version
    pub minor: u32,
}

impl ProtoVersion {
    /// Unspecified version
    #[allow(dead_code)]
    pub const UNSPECIFIED: Self = Self { major: 0, minor: 0 };
}

impl fmt::Display for ProtoVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.major == 0 && self.minor == 0 {
            write!(f, "UNSPECIFIED")
        } else {
            write!(f, "{}.{}", self.major, self.minor)
        }
    }
}
