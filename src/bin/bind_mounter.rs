//! The helper command to bind mount for non-root user

use std::ffi::OsStr;
use std::path::Path;

use clap::Parser;
use datenlord::common::error::DatenLordError::ArgumentInvalid;
use datenlord::common::error::{Context, DatenLordResult};
use datenlord::common::logger::{init_logger, LogRole};
use nix::mount::{self, MsFlags};
use tracing::debug;

/// Argument name of mount from directory
const FROM_DIR_ARG_NAME: &str = "from";
/// Argument name of mount to directory
const TO_DIR_ARG_NAME: &str = "to";
/// Argument name of mount filesystem type
const FS_TYPE_ARG_NAME: &str = "fstype";
/// Argument name of read only mount
const READ_ONLY_ARG_NAME: &str = "readonly";
/// Argument name of re-mount
const REMOUNT_ARG_NAME: &str = "remount";
/// Argument name of mount flags
const MOUNT_OPTIONS_ARG_NAME: &str = "options";
/// Argument name of un-mount
const UMOUNT_ARG_NAME: &str = "umount";

#[derive(Debug, Parser)]
#[clap(author,version,about,long_about=None)]
pub struct BindMounterConfig {
    #[clap(short = 'u', long = "umount", value_name = "DIRECTORY")]
    umount: Option<String>,
    #[clap(short = 'f', long = "from", value_name = "FROM DIRECTORY")]
    from_dir: Option<String>,
    #[clap(short = 't', long = "to", value_name = "TO DIRECTORY")]
    to_dir: Option<String>,
    #[clap(short = 's', long = "fstype", value_name = "FS TYPE")]
    fstype: Option<String>,
    #[clap(short = '0', long = "options", value_name = "OPTION,OPTION...")]
    options: Option<String>,
    #[clap(short = 'r', long = "readonly", value_name = "TRUE|FALSE")]
    readonly: bool,
    #[clap(short = 'm', long = "remount", value_name = "TRUE|FALSE")]
    remount: bool,
}

fn main() -> DatenLordResult<()> {
    init_logger(LogRole::BindMounter);
    debug!(
        "bind_mounter started with args: {:?}",
        std::env::args().collect::<Vec<_>>()
    );
    let config = BindMounterConfig::parse();

    let (umount_path, do_umount) = match config.umount {
        Some(ref s) => (Path::new(s), true),
        None => (Path::new(""), false),
    };

    let from_path = match config.from_dir {
        Some(ref s) => Path::new(s),
        None => {
            if do_umount {
                Path::new("")
            } else {
                return Err(ArgumentInvalid {
                    context: vec!["missing mount source directory".to_string()],
                });
            }
        }
    };

    let to_path = match config.to_dir {
        Some(ref s) => Path::new(s),
        None => {
            if do_umount {
                Path::new("")
            } else {
                return Err(ArgumentInvalid {
                    context: vec!["missing mount target directory".to_string()],
                });
            }
        }
    };
    let fs_type = match config.fstype {
        Some(ref s) => s.to_owned(),
        None => "".to_owned(),
    };
    let mount_options = match config.options {
        Some(ref s) => s.to_owned(),
        None => "".to_owned(),
    };
    let read_only = config.readonly;
    let remount = config.remount;

    debug!(
        "{}={:?}, {}={:?}, {}={:?}, {}={}, {}={}, {}={}, {}={}",
        UMOUNT_ARG_NAME,
        umount_path,
        FROM_DIR_ARG_NAME,
        from_path,
        TO_DIR_ARG_NAME,
        to_path,
        FS_TYPE_ARG_NAME,
        fs_type,
        MOUNT_OPTIONS_ARG_NAME,
        mount_options,
        READ_ONLY_ARG_NAME,
        read_only,
        REMOUNT_ARG_NAME,
        remount,
    );

    let mut mnt_flags = MsFlags::MS_BIND;
    if read_only {
        mnt_flags |= MsFlags::MS_RDONLY;
    }
    if remount {
        mnt_flags |= MsFlags::MS_REMOUNT;
    }

    if do_umount {
        mount::umount(umount_path)
            .with_context(|| format!("failed to umount {:?}", umount_path))?;
    } else {
        mount::mount::<Path, Path, OsStr, OsStr>(
            Some(from_path),
            to_path,
            if fs_type.is_empty() {
                None
            } else {
                Some(OsStr::new(&fs_type))
            },
            mnt_flags,
            if mount_options.is_empty() {
                None
            } else {
                Some(OsStr::new(&mount_options))
            },
        )
        .with_context(|| format!("failed to mount {:?} to {:?}", from_path, to_path))?;
    }

    Ok(())
}

#[cfg(test)]
#[allow(clippy::bool_assert_comparison)] // For test, it brings more readability.
mod tests {
    use super::*;

    #[test]
    fn test_bind_mounter_config() {
        let args = vec![
            "bind_mounter",
            "--from",
            "/tmp/from",
            "--to",
            "/tmp/to",
            "--fstype",
            "ext4",
            "--options",
            "rw",
            "--readonly",
            "--remount",
        ];
        let config = BindMounterConfig::parse_from(args);
        assert_eq!(config.from_dir.unwrap(), "/tmp/from");
        assert_eq!(config.to_dir.unwrap(), "/tmp/to");
        assert_eq!(config.fstype.unwrap(), "ext4");
        assert_eq!(config.options.unwrap(), "rw");
        assert_eq!(config.readonly, true);
        assert_eq!(config.remount, true);
    }

    #[test]
    fn test_bind_mounter_config_umount() {
        let args = vec!["bind_mounter", "--umount", "/tmp/umount"];
        let config = BindMounterConfig::parse_from(args);
        assert_eq!(config.umount.unwrap(), "/tmp/umount");
    }

    #[test]
    fn test_short_config() {
        let args = vec![
            "bind_mounter",
            "-f",
            "/tmp/from",
            "-t",
            "/tmp/to",
            "-s",
            "ext4",
            "-0",
            "rw",
            "-r",
            "-m",
        ];
        let config = BindMounterConfig::parse_from(args);
        assert_eq!(config.from_dir.unwrap(), "/tmp/from");
        assert_eq!(config.to_dir.unwrap(), "/tmp/to");
        assert_eq!(config.fstype.unwrap(), "ext4");
        assert_eq!(config.options.unwrap(), "rw");
        assert_eq!(config.readonly, true);
        assert_eq!(config.remount, true);
    }
}
