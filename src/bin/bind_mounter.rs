//! The helper command to bind mount for non-root user

use clap::{Arg, Command};
use log::debug;
use nix::mount::{self, MsFlags};
use std::ffi::OsStr;
use std::path::Path;

use datenlord::common::error::{Context, DatenLordError::ArgumentInvalid, DatenLordResult};

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

fn main() -> DatenLordResult<()> {
    env_logger::init();

    let matches = Command::new("BindMounter")
        .about("Helper command to bind mount for non-root user")
        .arg(
            Arg::new(UMOUNT_ARG_NAME)
                .short('u')
                .long(UMOUNT_ARG_NAME)
                .value_name("DIRECTORY")
                .takes_value(true)
                .help("Set umount directory, default empty"),
        )
        .arg(
            Arg::new(FROM_DIR_ARG_NAME)
                .short('f')
                .long(FROM_DIR_ARG_NAME)
                .value_name("FROM DIRECTORY")
                .takes_value(true)
                .help("Set read only true or false, default false"),
        )
        .arg(
            Arg::new(TO_DIR_ARG_NAME)
                .short('t')
                .long(TO_DIR_ARG_NAME)
                .value_name("TO DIRECTORY")
                .takes_value(true)
                .help("Set read only true or false, default false"),
        )
        .arg(
            Arg::new(FS_TYPE_ARG_NAME)
                .short('s')
                .long(FS_TYPE_ARG_NAME)
                .value_name("FS TYPE")
                .takes_value(true)
                .help("Set mount filesystem, default empty"),
        )
        .arg(
            Arg::new(MOUNT_OPTIONS_ARG_NAME)
                .short('o')
                .long(MOUNT_OPTIONS_ARG_NAME)
                .value_name("OPTION,OPTION...")
                .takes_value(true)
                .help("Set mount flags, default empty"),
        )
        .arg(
            Arg::new(READ_ONLY_ARG_NAME)
                .short('r')
                .long(READ_ONLY_ARG_NAME)
                .value_name("TRUE|FALSE")
                .takes_value(false)
                .help("Set read only true or false, default false"),
        )
        .arg(
            Arg::new(REMOUNT_ARG_NAME)
                .short('m')
                .long(REMOUNT_ARG_NAME)
                .value_name("TRUE|FALSE")
                .takes_value(false)
                .help(
                    "Set re-mount true or false, \
                        default false",
                ),
        )
        .get_matches();

    let (umount_path, do_umount) = match matches.value_of(UMOUNT_ARG_NAME) {
        Some(s) => (Path::new(s), true),
        None => (Path::new(""), false),
    };
    let from_path = match matches.value_of(FROM_DIR_ARG_NAME) {
        Some(s) => Path::new(s),
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
    let to_path = match matches.value_of(TO_DIR_ARG_NAME) {
        Some(s) => Path::new(s),
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
    let fs_type = match matches.value_of(FS_TYPE_ARG_NAME) {
        Some(s) => s.to_owned(),
        None => "".to_owned(),
    };
    let mount_options = match matches.value_of(MOUNT_OPTIONS_ARG_NAME) {
        Some(s) => s.to_owned(),
        None => "".to_owned(),
    };
    let read_only = matches.is_present(READ_ONLY_ARG_NAME);
    let remount = matches.is_present(REMOUNT_ARG_NAME);

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
