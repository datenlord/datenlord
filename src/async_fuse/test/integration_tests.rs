use std::fs::File;
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;
use std::{fs, io, iter};

use anyhow::Context;
use clippy_utilities::OverflowArithmetic;
use nix::fcntl::{self, OFlag};
use nix::sys::stat::Mode;
use nix::unistd::{self, Whence};
use tracing::info;

use super::test_util;

pub const BENCH_MOUNT_DIR: &str = "/tmp/datenlord_bench_dir";
pub const S3_BENCH_MOUNT_DIR: &str = "./s3_fuse_bench";
pub const DEFAULT_MOUNT_DIR: &str = "/tmp/datenlord_test_dir";
pub const S3_DEFAULT_MOUNT_DIR: &str = "/tmp/datenlord_test_dir";
pub const FILE_CONTENT: &str = "0123456789ABCDEF";

#[cfg(test)]
fn test_create_file(mount_dir: &Path) -> anyhow::Result<()> {
    use smol::fs::unix::MetadataExt;
    info!("test create file");
    let file_path = Path::new(mount_dir).join("test_create_file_user.txt");
    let file_mode = Mode::from_bits_truncate(0o644);
    let file_fd = fcntl::open(&file_path, OFlag::O_CREAT, file_mode)?;
    unistd::close(file_fd)?;
    info!("try get file metadata");
    let file_metadata = fs::metadata(&file_path)?;
    assert_eq!(file_metadata.mode() & 0o777, 0o644);
    // check the file owner
    let file_owner = file_metadata.uid();
    let create_user_id = unistd::getuid();
    assert_eq!(file_owner, create_user_id.as_raw());
    // check the file group
    let file_group = file_metadata.gid();
    let create_group_id = unistd::getgid();
    assert_eq!(file_group, create_group_id.as_raw());
    // check nlink == 1
    assert_eq!(file_metadata.nlink(), 1);
    fs::remove_file(&file_path)?; // immediate deletion
    Ok(())
}

#[cfg(test)]
fn test_name_too_long(mount_dir: &Path) -> anyhow::Result<()> {
    use nix::fcntl::open;
    info!("test_name_too_long");

    let file_name = "a".repeat(256);
    // try to create file, dir, symlink with name too long
    // expect to fail with ENAMETOOLONG
    let file_path = Path::new(mount_dir).join(file_name);
    let file_mode = Mode::from_bits_truncate(0o644);

    info!("try to create a file with name too long");
    let result = open(&file_path, OFlag::O_CREAT, file_mode);
    match result {
        Ok(_) => panic!("File creation should have failed with ENAMETOOLONG"),
        Err(nix::Error::ENAMETOOLONG) => {} // expected this error
        Err(e) => return Err(e.into()),
    }

    info!("try to create a dir with name too long");
    let result = fs::create_dir_all(&file_path);
    match result {
        Ok(()) => panic!("Directory creation should have failed with ENAMETOOLONG"),
        Err(ref e) if e.raw_os_error() == Some(libc::ENAMETOOLONG) => {} // expected this error
        Err(e) => return Err(e.into()),
    }

    info!("try to create a symlink with name too long");
    let result = std::os::unix::fs::symlink(mount_dir, &file_path);
    match result {
        Ok(()) => panic!("Symlink creation should have failed with ENAMETOOLONG"),
        Err(ref e) if e.raw_os_error() == Some(libc::ENAMETOOLONG) => {} // expected this error
        Err(e) => return Err(e.into()),
    }

    Ok(())
}

#[cfg(test)]
fn test_file_manipulation_rust_way(mount_dir: &Path) -> anyhow::Result<()> {
    info!("file manipulation Rust style");
    let file_path = Path::new(mount_dir).join("tmp.txt");
    fs::write(&file_path, FILE_CONTENT)?;
    let bytes = fs::read(&file_path)?;
    let content = String::from_utf8(bytes)?;
    fs::remove_file(&file_path)?; // immediate deletion

    assert_eq!(content, FILE_CONTENT);
    assert!(
        !file_path.exists(),
        "the file {file_path:?} should have been removed",
    );
    Ok(())
}

#[cfg(test)]
fn test_directory_manipulation_rust_way(mount_dir: &Path) -> anyhow::Result<()> {
    info!("Directory manipulation Rust style");

    // Fixed directory name
    let dir_name = "test_dir";
    let dir_path = Path::new(mount_dir).join(dir_name);

    // Create directory
    fs::create_dir_all(&dir_path)?;

    // Fixed file name
    let file_name = "tmp.txt";
    let file_path = dir_path.join(file_name);

    // Write to file
    fs::write(&file_path, FILE_CONTENT)?;

    // Read from file
    let bytes = fs::read(&file_path)?;
    let content = String::from_utf8(bytes)?;

    // Remove file and directory
    fs::remove_file(&file_path)?;
    fs::remove_dir(&dir_path)?;

    // Assertions
    assert_eq!(content, FILE_CONTENT);
    assert!(!file_path.exists(), "the file should have been removed");
    assert!(!dir_path.exists(), "the directory should have been removed");

    Ok(())
}

#[cfg(test)]
fn test_deferred_deletion(mount_dir: &Path) -> anyhow::Result<()> {
    info!("file deletion deferred");
    let file_path = Path::new(mount_dir).join("test_file.txt");
    let oflags = OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_RDWR;
    let file_mode = Mode::from_bits_truncate(0o644);
    let fd = fcntl::open(&file_path, oflags, file_mode)?;
    unistd::unlink(&file_path)?; // deferred deletion

    let repeat_times = 3;
    for _ in 0..repeat_times {
        let write_size = unistd::write(fd, FILE_CONTENT.as_bytes())?;
        assert_eq!(
            write_size,
            FILE_CONTENT.len(),
            "the file write size {} is not the same as the expected size {}",
            write_size,
            FILE_CONTENT.len(),
        );
    }
    unistd::fsync(fd)?;

    let mut buffer: Vec<u8> = iter::repeat(0_u8)
        .take(FILE_CONTENT.len().overflow_mul(repeat_times))
        .collect();
    unistd::lseek(fd, 0, Whence::SeekSet)?;
    let read_size = unistd::read(fd, &mut buffer)?;
    let content = String::from_utf8(buffer)?;
    unistd::close(fd)?;

    assert_eq!(
        read_size,
        FILE_CONTENT.len().overflow_mul(repeat_times),
        "the file read size {} is not the same as the expected size {}",
        read_size,
        FILE_CONTENT.len().overflow_mul(repeat_times),
    );
    let str_content = FILE_CONTENT.repeat(repeat_times);
    assert_eq!(
        content, str_content,
        "the file read result is not the same as the expected content",
    );

    assert!(
        !file_path.exists(),
        "the file {file_path:?} should have been removed",
    );
    Ok(())
}

#[cfg(test)]
fn test_rename_file(mount_dir: &Path) -> anyhow::Result<()> {
    info!("rename file");
    let from_dir = Path::new(mount_dir).join("from_dir");
    if from_dir.exists() {
        fs::remove_dir_all(&from_dir)?;
    }
    fs::create_dir_all(&from_dir)?;

    let to_dir = Path::new(&mount_dir).join("to_dir");
    if to_dir.exists() {
        fs::remove_dir_all(&to_dir)?;
    }
    fs::create_dir_all(&to_dir)?;

    let old_file = from_dir.join("old.txt");
    fs::write(&old_file, FILE_CONTENT)?;

    let new_file = to_dir.join("new.txt");

    fs::rename(&old_file, &new_file)?;

    let bytes = fs::read(&new_file)?;
    let content = String::from_utf8(bytes)?;
    assert_eq!(
        content, FILE_CONTENT,
        "the file read result is not the same as the expected content",
    );

    assert!(
        !old_file.exists(),
        "the old file {old_file:?} should have been removed",
    );
    assert!(new_file.exists(), "the new file {new_file:?} should exist",);

    // Clean up
    fs::remove_dir_all(&from_dir)?;
    fs::remove_dir_all(&to_dir)?;
    Ok(())
}

#[cfg(test)]
fn test_rename_file_replace(mount_dir: &Path) -> anyhow::Result<()> {
    info!("rename file no replace");
    let oflags = OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_RDWR;
    let file_mode = Mode::from_bits_truncate(0o644);

    let old_file = Path::new(mount_dir).join("old.txt");
    let old_fd = fcntl::open(&old_file, oflags, file_mode)?;
    let old_file_write_size = unistd::write(old_fd, FILE_CONTENT.as_bytes())?;
    assert_eq!(
        old_file_write_size,
        FILE_CONTENT.len(),
        "the file write size {} is not the same as the expected size {}",
        old_file_write_size,
        FILE_CONTENT.len(),
    );

    let new_file = Path::new(&mount_dir).join("new.txt");
    let new_fd = fcntl::open(&new_file, oflags, file_mode)?;
    let new_file_write_size = unistd::write(new_fd, FILE_CONTENT.as_bytes())?;
    assert_eq!(
        new_file_write_size,
        FILE_CONTENT.len(),
        "the file write size {} is not the same as the expected size {}",
        new_file_write_size,
        FILE_CONTENT.len(),
    );

    fs::rename(&old_file, &new_file).context("rename replace should not fail")?;

    let mut buffer: Vec<u8> = iter::repeat(0_u8).take(FILE_CONTENT.len()).collect();
    unistd::lseek(old_fd, 0, Whence::SeekSet)?;
    let old_file_read_size = unistd::read(old_fd, &mut buffer)?;
    let content = String::from_utf8(buffer)?;
    unistd::close(old_fd)?;
    assert_eq!(
        old_file_read_size,
        FILE_CONTENT.len(),
        "the file read size {} is not the same as the expected size {}",
        old_file_read_size,
        FILE_CONTENT.len(),
    );
    assert_eq!(
        content, FILE_CONTENT,
        "the file read result is not the same as the expected content",
    );

    let mut buffer: Vec<u8> = iter::repeat(0_u8).take(FILE_CONTENT.len()).collect();
    unistd::lseek(new_fd, 0, Whence::SeekSet)?;
    let new_file_read_size = unistd::read(new_fd, &mut buffer)?;
    let content = String::from_utf8(buffer)?;
    unistd::close(new_fd)?;
    assert_eq!(
        new_file_read_size,
        FILE_CONTENT.len(),
        "the file read size {} is not the same as the expected size {}",
        new_file_read_size,
        FILE_CONTENT.len(),
    );
    assert_eq!(
        content, FILE_CONTENT,
        "the file read result is not the same as the expected content",
    );

    assert!(
        !old_file.exists(),
        "the old file {new_file:?} should not exist",
    );
    assert!(new_file.exists(), "the new file {new_file:?} should exist",);

    // Clean up
    fs::remove_file(&new_file)?;
    Ok(())
}

#[cfg(test)]
fn test_rename_non_existent_source(mount_dir: &Path) -> anyhow::Result<()> {
    let non_existent_file = mount_dir.join("non_existent_file.txt");
    let destination_file = mount_dir.join("destination_file.txt");

    // Attempt to rename the non-existent file to the new location
    match fs::rename(non_existent_file, destination_file) {
        Ok(()) => Err(anyhow::anyhow!(
            "Renaming a non-existent file unexpectedly succeeded"
        )),
        Err(e) => {
            // Check if the error kind is NotFound, which is expected in this case
            if e.kind() == std::io::ErrorKind::NotFound {
                Ok(()) // Test passes
            } else {
                Err(anyhow::anyhow!(
                    "Expected NotFound error, but got a different error: {:?}",
                    e
                ))
            }
        }
    }
}

#[cfg(test)]
#[cfg(feature = "abi-7-23")]
fn test_rename_no_replace_flag(mount_dir: &Path) -> anyhow::Result<()> {
    use nix::fcntl::{renameat2, RenameFlags};

    let source_file_str = "source_file_no_replace_flag.txt";
    let destination_file_str = "destination_file_no_replace_flag.txt";

    let source_file = mount_dir.join(source_file_str);
    let destination_file = mount_dir.join(destination_file_str);

    // Create a source file and a destination file to set up the test environment
    std::fs::write(source_file.clone(), "source content")?;
    std::fs::write(destination_file.clone(), "destination content")?;

    // Attempt to rename the source file to the destination location with
    // RENAME_NOREPLACE flag
    let result = renameat2(
        None,
        source_file_str,
        None,
        destination_file_str,
        RenameFlags::RENAME_NOREPLACE,
    );

    match result {
        Ok(()) => Err(anyhow::anyhow!(
            "Renaming with RENAME_NOREPLACE flag unexpectedly succeeded"
        )),
        Err(e) => {
            if e == nix::errno::Errno::EEXIST {
                // Remove the source file and destination file
                std::fs::remove_file(source_file)?;
                std::fs::remove_file(destination_file)?;
                Ok(()) // Test passes
            } else {
                Err(anyhow::anyhow!(
                    "Expected EEXIST error, but got a different error: {:?}",
                    e
                ))
            }
        }
    }
}

#[cfg(test)]
fn test_rename_to_non_existent_destination_directory(mount_dir: &Path) -> anyhow::Result<()> {
    let source_file = mount_dir.join("source_file.txt");
    let non_existent_dir = mount_dir.join("non_existent_dir");
    let destination_file = non_existent_dir.join("destination_file.txt");

    // Create a source file for testing.
    std::fs::write(&source_file, "Some content")?;

    // Attempt to rename the file to a non-existent directory
    match fs::rename(&source_file, destination_file) {
        Ok(()) => Err(anyhow::anyhow!(
            "Renaming to a non-existent destination directory unexpectedly succeeded"
        )),
        Err(e) => {
            // Check if the error kind is NotFound, which is expected in this case
            if e.kind() == std::io::ErrorKind::NotFound {
                Ok(()) // Test passes
            } else {
                Err(anyhow::anyhow!(
                    "Expected NotFound error for non-existent directory, but got a different error: {:?}",
                    e
                ))
            }
        }
    }
}

#[cfg(test)]
#[cfg(feature = "abi-7-23")]
fn test_rename_exchange(mount_dir: &Path) -> anyhow::Result<()> {
    use nix::fcntl::RenameFlags;
    use nix::sys::stat;

    info!("rename file exchange");

    let base_dir = Path::new(mount_dir).join("exchange");
    if base_dir.exists() {
        fs::remove_dir_all(&base_dir)?;
    }
    fs::create_dir_all(&base_dir)?;

    let oflags_dir = OFlag::O_DIRECTORY;
    let rename_flag = RenameFlags::RENAME_EXCHANGE;

    // Test exchange under the same parent
    {
        let base_fd = fcntl::open(&base_dir, oflags_dir, Mode::from_bits_truncate(0o755))?;
        let file_left = base_dir.join("a.txt");
        let file_right = base_dir.join("dir");
        let file_b_txt = file_right.join("b.txt");

        fs::create_dir_all(&file_right)?;
        fs::write(&file_left, "a")?;
        fs::write(file_b_txt, "b")?;

        // Tree:
        // exchange
        // |- a.txt
        // |
        // |- dir
        //     |- b.txt

        let stat_old_base = stat::fstat(base_fd);
        let stat_old_left = stat::stat(&file_left)?;
        let stat_old_right = stat::stat(&file_right)?;

        fcntl::renameat2(
            Some(base_fd),
            &file_left,
            Some(base_fd),
            &file_right,
            rename_flag,
        )?;

        // Tree:
        // exchange
        // |- dir (former a.txt)
        // |
        // |- a.txt (former dir)
        //     |- b.txt

        let stat_new_base = stat::fstat(base_fd);
        let stat_new_left = stat::stat(&file_left)?;
        let stat_new_right = stat::stat(&file_right)?;

        assert_eq!(stat_old_left, stat_new_right);
        assert_eq!(stat_old_right, stat_new_left);
        assert_ne!(stat_old_base, stat_new_base); // Timestamp of the parent is changed.

        let file_b_txt = file_left.join("b.txt"); // file_left is a dir now
        let content_a = fs::read_to_string(&file_right)?;
        let content_b = fs::read_to_string(file_b_txt)?;

        assert_eq!(content_a, "a");
        assert_eq!(content_b, "b");

        // This call will do nothing.
        fcntl::renameat2(
            Some(base_fd),
            &file_left,
            Some(base_fd),
            &file_left,
            rename_flag,
        )?;
        let stat_new_base_2 = stat::fstat(base_fd);
        assert_eq!(stat_new_base, stat_new_base_2);

        unistd::close(base_fd)?;
    }

    // Test exchange under different parents
    {
        let base_left = base_dir.join("left");
        let base_right = base_dir.join("right");
        let file_left = base_left.join("left.txt");
        let file_right = base_right.join("right.txt");

        fs::create_dir_all(&base_left)?;
        fs::create_dir_all(&base_right)?;
        fs::write(&file_left, "left")?;
        fs::write(&file_right, "right")?;

        let base_fd_left = fcntl::open(&base_left, oflags_dir, Mode::from_bits_truncate(0o755))?;
        let base_fd_right = fcntl::open(&base_right, oflags_dir, Mode::from_bits_truncate(0o755))?;

        let stat_old_left = stat::stat(&file_left)?;
        let stat_old_right = stat::stat(&file_right)?;

        fcntl::renameat2(
            Some(base_fd_left),
            &file_left,
            Some(base_fd_right),
            &file_right,
            rename_flag,
        )?;

        let stat_new_left = stat::stat(&file_left)?;
        let stat_new_right = stat::stat(&file_right)?;

        assert_eq!(stat_old_left, stat_new_right);
        assert_eq!(stat_old_right, stat_new_left);

        let content_a = fs::read_to_string(&file_left)?;
        let content_b = fs::read_to_string(&file_right)?;

        assert_eq!(content_a, "right");
        assert_eq!(content_b, "left");

        unistd::close(base_fd_left)?;
        unistd::close(base_fd_right)?;
    }

    // Clean up
    fs::remove_dir_all(&base_dir)?;

    Ok(())
}
#[cfg(test)]
fn test_rename_dir(mount_dir: &Path) -> anyhow::Result<()> {
    info!("rename directory");
    let from_dir = Path::new(mount_dir).join("from_dir");
    if from_dir.exists() {
        fs::remove_dir_all(&from_dir)?;
    }
    fs::create_dir_all(&from_dir)?;

    let to_dir = Path::new(&mount_dir).join("to_dir");
    if to_dir.exists() {
        fs::remove_dir_all(&to_dir)?;
    }
    fs::create_dir_all(&to_dir)?;

    let old_sub_dir = from_dir.join("old_sub");
    fs::create_dir_all(&old_sub_dir)?;
    let new_sub_dir = to_dir.join("new_sub");
    fs::rename(&old_sub_dir, &new_sub_dir)?;

    assert!(
        !old_sub_dir.exists(),
        "the old directory {old_sub_dir:?} should have been removed"
    );
    assert!(
        new_sub_dir.exists(),
        "the new directory {new_sub_dir:?} should exist",
    );

    // Clean up
    fs::remove_dir_all(&from_dir)?;
    fs::remove_dir_all(&to_dir)?;
    Ok(())
}

#[cfg(test)]
fn test_symlink_dir(mount_dir: &Path) -> anyhow::Result<()> {
    info!("create and read symlink to directory");

    let current_dir = std::env::current_dir()?;
    std::env::set_current_dir(Path::new(mount_dir))?;

    let src_dir = Path::new("src_dir");
    if src_dir.exists() {
        fs::remove_dir_all(src_dir)?;
    }
    fs::create_dir_all(src_dir).context(format!("failed to create directory={src_dir:?}"))?;

    let src_file_name = "src.txt";
    let src_path = Path::new(&src_dir).join(src_file_name);

    let dst_dir = Path::new("dst_dir");
    unistd::symlinkat(src_dir, None, dst_dir).context("create symlink failed")?;
    let target_path = fs::read_link(dst_dir).context("read symlink failed ")?;
    assert_eq!(src_dir, target_path, "symlink target path not match");

    let dst_path = Path::new("./dst_dir").join(src_file_name);
    fs::write(&dst_path, FILE_CONTENT).context(format!("failed to write to file={dst_path:?}"))?;
    let content = fs::read_to_string(src_path).context("read symlink target file failed")?;
    assert_eq!(
        content, FILE_CONTENT,
        "symlink target file content not match"
    );

    let md = fs::symlink_metadata(dst_dir).context("read symlink metadata failed")?;
    assert!(
        md.file_type().is_symlink(),
        "file type should be symlink other than {:?}",
        md.file_type(),
    );

    let entries = fs::read_dir(dst_dir)
        .context("ready symlink target directory failed")?
        .filter_map(|e| {
            if let Ok(entry) = e {
                Some(entry.file_name())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(entries.len(), 1, "the directory entry number not match");
    assert_eq!(
        std::ffi::OsStr::new(src_file_name),
        entries
            .get(0)
            .unwrap_or_else(|| panic!("failed to get the first entry")),
        "directory entry name not match",
    );

    info!("about to remove src_dir");
    fs::remove_dir_all(src_dir)?; // immediate deletion
    info!("about to remove dst_dir");
    fs::remove_dir_all(dst_dir)?; // immediate deletion
    assert!(!src_dir.exists());
    assert!(!dst_dir.exists());
    std::env::set_current_dir(current_dir)?;
    Ok(())
}

#[cfg(test)]
fn test_symlink_file(mount_dir: &Path) -> anyhow::Result<()> {
    info!("create and read symlink to file");

    let current_dir = std::env::current_dir()?;
    std::env::set_current_dir(Path::new(mount_dir))?;

    let src_path = Path::new("src.txt");
    fs::write(src_path, FILE_CONTENT)?;

    let dst_path = Path::new("dst.txt");
    unistd::symlinkat(src_path, None, dst_path).context("create symlink failed")?;
    let target_path = fs::read_link(dst_path).context("read symlink failed ")?;
    assert_eq!(src_path, target_path, "symlink target path not match");

    let content = fs::read_to_string(dst_path).context("read symlink target file failed")?;
    assert_eq!(
        content, FILE_CONTENT,
        "symlink target file content not match"
    );

    let md = fs::symlink_metadata(dst_path).context("read symlink metadata failed")?;
    assert!(
        md.file_type().is_symlink(),
        "file type should be symlink other than {:?}",
        md.file_type(),
    );
    fs::remove_file(src_path)?; // immediate deletion
    fs::remove_file(dst_path)?; // immediate deletion
    assert!(!src_path.exists());
    assert!(!dst_path.exists());
    std::env::set_current_dir(current_dir)?;
    Ok(())
}

/// Test bind mount a FUSE directory to a tmpfs directory
/// this test case need root privilege
#[cfg(target_os = "linux")]
#[cfg(test)]
fn test_bind_mount(fuse_mount_dir: &Path) -> anyhow::Result<()> {
    use nix::mount::MsFlags;

    pub fn cleanup_dir(directory: &Path) -> anyhow::Result<()> {
        let umount_res = nix::mount::umount2(directory, nix::mount::MntFlags::MNT_FORCE);
        if umount_res.is_err() {
            info!("cleanup_dir() failed to un-mount {directory:?}");
        }
        fs::remove_dir_all(directory)
            .context(format!("cleanup_dir() failed to remove {directory:?}"))?;
        Ok(())
    }

    if unistd::geteuid().is_root() {
        info!("test bind mount with root user");
    } else {
        // Skip bind mount test for non-root user
        return Ok(());
    }
    let from_dir = Path::new(fuse_mount_dir).join("bind_from_dir");
    if from_dir.exists() {
        cleanup_dir(&from_dir).context(format!("failed to cleanup {from_dir:?}"))?;
    }
    fs::create_dir_all(&from_dir).context(format!("failed to create {from_dir:?}"))?;

    let target_dir = Path::new("/tmp/bind_target_dir");
    if target_dir.exists() {
        cleanup_dir(target_dir).context(format!("failed to cleanup {target_dir:?}"))?;
    }
    fs::create_dir_all(target_dir).context(format!("failed to create {from_dir:?}"))?;

    nix::mount::mount::<Path, Path, Path, Path>(
        Some(&from_dir),
        target_dir,
        None, // fstype
        MsFlags::MS_BIND | MsFlags::MS_NOSUID | MsFlags::MS_NODEV | MsFlags::MS_NOEXEC,
        None, // mount option data
    )
    .context(format!(
        "failed to bind mount {from_dir:?} to {target_dir:?}",
    ))?;

    let file_path = Path::new(&target_dir).join("tmp.txt");
    fs::write(&file_path, FILE_CONTENT)?;
    let content = fs::read_to_string(&file_path)?;
    fs::remove_file(&file_path)?; // immediate deletion
    assert_eq!(content, FILE_CONTENT, "file content not match");

    nix::mount::umount(target_dir).context(format!("failed to un-mount {target_dir:?}"))?;

    fs::remove_dir_all(&from_dir).context(format!("failed to remove {from_dir:?}"))?;
    fs::remove_dir_all(target_dir).context(format!("failed to remove {target_dir:?}"))?;
    Ok(())
}

#[cfg(test)]
fn test_delete_file(mount_dir: &Path) -> anyhow::Result<()> {
    info!("test delete file");
    let file_path = Path::new(mount_dir).join("test_delete_file.txt");
    let file_mode = Mode::from_bits_truncate(0o644);
    let file_fd = fcntl::open(&file_path, OFlag::O_CREAT, file_mode)?;
    unistd::close(file_fd)?;

    fs::remove_file(&file_path)?;

    let result = fs::metadata(&file_path);
    match result {
        Ok(_) => panic!("File deletion failed"),
        Err(ref e) if e.kind() == io::ErrorKind::NotFound => {} // expected this error
        Err(e) => return Err(e.into()),
    }

    Ok(())
}

#[cfg(test)]
fn test_open_file_permission(mount_dir: &Path) -> anyhow::Result<()> {
    info!("test open file permission");
    let file_path = Path::new(mount_dir).join("test_open_file_permission.txt");

    let mut file = File::options()
        .create_new(true)
        .read(true)
        .write(true)
        .mode(0o444)
        .open(file_path.clone())?;

    file.write_all(FILE_CONTENT.as_ref())?;

    let content = fs::read_to_string(&file_path)?;
    assert_eq!(content, FILE_CONTENT);

    fs::remove_file(&file_path)?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::assertions_on_result_states)] // assert!(result.is_err()) is more readable for test
fn test_write_read_only_file(mount_dir: &Path) -> anyhow::Result<()> {
    info!("test write read only file");
    let file_path = Path::new(mount_dir).join("test_write_read_only_file.txt");

    // Create a read-only file
    File::create(&file_path)?;
    // Set the file permission to read-only
    fs::set_permissions(&file_path, fs::Permissions::from_mode(0o444))?; // -r--r--r--

    // Check the file permission
    let file_metadata = fs::metadata(&file_path)?;
    assert_eq!(file_metadata.permissions().mode() & 0o777, 0o444);

    // Try to open the file with read-only permission
    File::options().read(true).open(file_path.clone())?;
    // Try to open the file with write permission
    assert!(File::options().write(true).open(file_path.clone()).is_err());
    // Try to open the file with read-write permission
    assert!(File::options()
        .read(true)
        .write(true)
        .open(file_path.clone())
        .is_err());
    fs::remove_file(&file_path)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_all() -> anyhow::Result<()> {
    run_test().await
}

async fn run_test() -> anyhow::Result<()> {
    _run_test(DEFAULT_MOUNT_DIR, false).await
}

#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn run_s3_test() -> anyhow::Result<()> {
    _run_test(S3_DEFAULT_MOUNT_DIR, true).await
}

async fn _run_test(mount_dir_str: &str, is_s3: bool) -> anyhow::Result<()> {
    info!("begin integration test");
    let mount_dir = Path::new(mount_dir_str);
    let th = test_util::setup(mount_dir, is_s3).await?;

    test_delete_file(mount_dir).context("test_delete_file() failed")?;
    test_file_manipulation_rust_way(mount_dir)
        .context("test_file_manipulation_rust_way() failed")?;
    test_directory_manipulation_rust_way(mount_dir)
        .context("test_directory_manipulation_rust_way() failed")?;
    test_create_file(mount_dir).context("test_create_file() failed")?;
    test_name_too_long(mount_dir).context("test_name_too_long() failed")?;
    test_symlink_dir(mount_dir).context("test_symlink_dir() failed")?;
    test_symlink_file(mount_dir).context("test_symlink_file() failed")?;
    #[cfg(target_os = "linux")]
    test_bind_mount(mount_dir).context("test_bind_mount() failed")?;
    test_deferred_deletion(mount_dir).context("test_deferred_deletion() failed")?;
    test_rename_non_existent_source(mount_dir)
        .context("test_rename_non_existent_source() failed")?;
    #[cfg(feature = "abi-7-23")]
    test_rename_no_replace_flag(mount_dir).context("test_rename_no_replace_flag() failed")?;
    test_rename_to_non_existent_destination_directory(mount_dir)
        .context("test_rename_to_non_existent_destination_directory() failed")?;
    test_rename_file_replace(mount_dir).context("test_rename_file_replace() failed")?;
    #[cfg(feature = "abi-7-23")]
    test_rename_exchange(mount_dir).context("test_rename_exchange() failed")?;
    test_rename_file(mount_dir).context("test_rename_file() failed")?;
    test_rename_dir(mount_dir).context("test_rename_dir() failed")?;
    test_create_file(mount_dir).context("test_create_file() failed")?;
    test_open_file_permission(mount_dir).context("test_open_file_permission() failed")?;
    test_write_read_only_file(mount_dir).context("test_write_read_only_file() failed")?;

    test_util::teardown(mount_dir, th).await?;

    Ok(())
}

// TODO: check the logic of this benchmark and make it could be run in CI
#[allow(dead_code)]
async fn run_bench() -> anyhow::Result<()> {
    _run_bench(BENCH_MOUNT_DIR, true).await
}

#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn run_s3_bench() -> anyhow::Result<()> {
    _run_bench(S3_BENCH_MOUNT_DIR, true).await
}

async fn _run_bench(mount_dir_str: &str, is_s3: bool) -> anyhow::Result<()> {
    let mount_dir = Path::new(mount_dir_str);
    let th = test_util::setup(mount_dir, is_s3).await?;

    let fio_handle = std::process::Command::new("fio")
        .arg("./scripts/perf/fio-jobs.ini")
        .env("BENCH_DIR", mount_dir)
        .output()
        .context("fio command failed to start, maybe install fio first")?;
    let fio_res = if fio_handle.status.success() {
        fs::write("bench.log", &fio_handle.stdout).context("failed to write bench log")?;
        Ok(())
    } else {
        let stderr = String::from_utf8_lossy(&fio_handle.stderr);
        info!("fio failed to run, the error is: {}", &stderr);
        Err(anyhow::anyhow!(
            "fio failed to run, the error is: {}",
            &stderr,
        ))
    };

    test_util::teardown(mount_dir, th).await?;
    fio_res
}
