use anyhow::Context;
use log::info;
use nix::dir::Dir;
use nix::fcntl::{self, OFlag};
use nix::sys::stat::Mode;
use nix::unistd::{self, Whence};
use std::collections::HashSet;
use std::fs;
use std::iter;
use std::path::Path;
use utilities::OverflowArithmetic;

use super::test_util;

pub const BENCH_MOUNT_DIR: &str = "../fuse_bench";
pub const DEFAULT_MOUNT_DIR: &str = "../fuse_test";
pub const FILE_CONTENT: &str = "0123456789ABCDEF";

#[allow(dead_code)]
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
        "the file {:?} should have been removed",
        file_path,
    );
    Ok(())
}

#[allow(dead_code)]
fn test_file_manipulation_nix_way(mount_dir: &Path) -> anyhow::Result<()> {
    info!("file manipulation C style");
    let file_path = Path::new(mount_dir).join("tmp.test");
    let oflags = OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_RDWR;
    let fd = fcntl::open(&file_path, oflags, Mode::empty())?;

    let write_size = unistd::write(fd, FILE_CONTENT.as_bytes())?;
    assert_eq!(
        write_size,
        FILE_CONTENT.len(),
        "the file write size {} is not the same as the expected size {}",
        write_size,
        FILE_CONTENT.len(),
    );

    unistd::lseek(fd, 0, Whence::SeekSet)?;
    let mut buffer: Vec<u8> = iter::repeat(0_u8).take(FILE_CONTENT.len()).collect();
    let read_size = unistd::read(fd, &mut *buffer)?;
    assert_eq!(
        read_size,
        FILE_CONTENT.len(),
        "the file read size {} is not the same as the expected size {}",
        read_size,
        FILE_CONTENT.len(),
    );
    let content = String::from_utf8(buffer)?;
    assert_eq!(
        content, FILE_CONTENT,
        "the file read result is not the same as the expected content",
    );
    unistd::close(fd)?;

    unistd::unlink(&file_path)?; // immediate deletion
    assert!(
        !file_path.exists(),
        "the file {:?} should have been removed",
        file_path,
    );
    Ok(())
}

#[allow(dead_code)]
fn test_dir_manipulation_nix_way(mount_dir: &Path) -> anyhow::Result<()> {
    info!("directory manipulation C style");
    let dir_path = Path::new(mount_dir).join("test_dir");
    let dir_mode = Mode::from_bits_truncate(0o755);
    if dir_path.exists() {
        fs::remove_dir_all(&dir_path)?;
    }
    unistd::mkdir(&dir_path, dir_mode)?;

    let repeat_times = 100;
    let mut sub_dirs = HashSet::new();
    for i in 0..repeat_times {
        let sub_dir_name = format!("test_sub_dir_{}", i);
        let sub_dir_path = dir_path.join(&sub_dir_name);
        unistd::mkdir(&sub_dir_path, dir_mode)?;
        sub_dirs.insert(sub_dir_name);
    }

    let oflags = OFlag::O_RDONLY;
    let mut dir_fd = Dir::open(&dir_path, oflags, Mode::empty())?;
    let count = dir_fd
        .iter()
        .filter_map(nix::Result::ok)
        .filter_map(|e| {
            let bytes = e.file_name().to_bytes();
            if bytes.starts_with(&[b'.']) {
                // skip hidden entries, '.' and '..'
                None
            } else {
                let byte_vec = Vec::from(bytes);
                let str_name = String::from_utf8(byte_vec).ok()?;
                assert!(
                    sub_dirs.contains(&str_name),
                    "the sub directory name {} should be in the hashmap {:?}",
                    str_name,
                    sub_dirs,
                );
                Some(str_name)
            }
        })
        .count();
    assert_eq!(
        count,
        sub_dirs.len(),
        "the number of directory items {} is not as the expected number {}",
        count,
        sub_dirs.len()
    );

    // Clean up
    fs::remove_dir_all(&dir_path)?;
    Ok(())
}

#[allow(dead_code)]
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
    let read_size = unistd::read(fd, &mut *buffer)?;
    let content = String::from_utf8(buffer)?;
    unistd::close(fd)?;

    assert_eq!(
        read_size,
        FILE_CONTENT.len().overflow_mul(repeat_times),
        "the file read size {} is not the same as the expected size {}",
        read_size,
        FILE_CONTENT.len().overflow_mul(repeat_times),
    );
    let str_content: String = iter::repeat(FILE_CONTENT).take(repeat_times).collect();
    assert_eq!(
        content, str_content,
        "the file read result is not the same as the expected content",
    );

    assert!(
        !file_path.exists(),
        "the file {:?} should have been removed",
        file_path,
    );
    Ok(())
}

#[allow(dead_code)]
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
        "the old file {:?} should have been removed",
        old_file,
    );
    assert!(
        new_file.exists(),
        "the new file {:?} should exist",
        new_file,
    );

    // Clean up
    fs::remove_dir_all(&from_dir)?;
    fs::remove_dir_all(&to_dir)?;
    Ok(())
}

#[allow(dead_code)]
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

    // fs::rename(&old_file, &new_file).expect_err("rename no replace should fail");
    fs::rename(&old_file, &new_file).context("rename replace should not fail")?;

    let mut buffer: Vec<u8> = iter::repeat(0_u8).take(FILE_CONTENT.len()).collect();
    unistd::lseek(old_fd, 0, Whence::SeekSet)?;
    let old_file_read_size = unistd::read(old_fd, &mut *buffer)?;
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
    let new_file_read_size = unistd::read(new_fd, &mut *buffer)?;
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
        "the old file {:?} should not exist",
        new_file,
    );
    assert!(
        new_file.exists(),
        "the new file {:?} should exist",
        new_file,
    );

    // Clean up
    // fs::remove_file(&old_file)?;
    fs::remove_file(&new_file)?;
    Ok(())
}

#[allow(dead_code)]
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
        "the old direcotry {:?} should have been removed",
        old_sub_dir
    );
    assert!(
        new_sub_dir.exists(),
        "the new direcotry {:?} should exist",
        new_sub_dir,
    );

    // Clean up
    fs::remove_dir_all(&from_dir)?;
    fs::remove_dir_all(&to_dir)?;
    Ok(())
}

#[allow(dead_code)]
fn test_symlink_dir(mount_dir: &Path) -> anyhow::Result<()> {
    info!("create and read symlink to directory");
    let src_dir = Path::new(mount_dir).join("src_dir");
    if src_dir.exists() {
        fs::remove_dir_all(&src_dir)?;
    }
    fs::create_dir_all(&src_dir).context(format!("failed to create directory={:?}", src_dir))?;

    let src_file_name = "src.txt";
    let src_path = Path::new(&src_dir).join(src_file_name);

    let dst_dir = Path::new(mount_dir).join("dst_dir");
    unistd::symlinkat(&src_dir, None, &dst_dir).context("create symlink failed")?;
    // std::os::unix::fs::symlink(&src_path, &dst_path).context("create symlink failed")?;
    let target_path = std::fs::read_link(&dst_dir).context("read symlink failed ")?;
    assert_eq!(src_dir, target_path, "symlink target path not match");

    let dst_path = Path::new(&dst_dir).join(src_file_name);
    fs::write(&dst_path, FILE_CONTENT)
        .context(format!("failed to write to file={:?}", dst_path))?;
    let content = fs::read_to_string(&src_path).context("read symlink target file failed")?;
    assert_eq!(
        content, FILE_CONTENT,
        "symlink target file content not match"
    );

    let md = std::fs::symlink_metadata(&dst_dir).context("read symlink metadata failed")?;
    assert!(
        md.file_type().is_symlink(),
        "file type should be symlink other than {:?}",
        md.file_type(),
    );

    let entries = fs::read_dir(&dst_dir)
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

    fs::remove_dir_all(&src_dir)?; // immediate deletion
    fs::remove_dir_all(&dst_dir)?; // immediate deletion
    assert!(!src_dir.exists());
    assert!(!dst_dir.exists());
    Ok(())
}

#[allow(dead_code)]
fn test_symlink_file(mount_dir: &Path) -> anyhow::Result<()> {
    info!("create and read symlink to file");

    let src_path = Path::new(mount_dir).join("src.txt");
    fs::write(&src_path, FILE_CONTENT)?;

    let dst_path = Path::new(mount_dir).join("dst.txt");
    unistd::symlinkat(&src_path, None, &dst_path).context("create symlink failed")?;
    // std::os::unix::fs::symlink(&src_path, &dst_path).context("create symlink failed")?;
    let target_path = std::fs::read_link(&dst_path).context("read symlink failed ")?;
    assert_eq!(src_path, target_path, "symlink target path not match");

    let content = fs::read_to_string(&dst_path).context("read symlink target file failed")?;
    assert_eq!(
        content, FILE_CONTENT,
        "symlink target file content not match"
    );

    // let oflags = OFlag::O_RDWR;
    // let fd = fcntl::open(&dst_path, oflags, Mode::empty())
    //     .context("open symlink target file failed ")?;
    // unistd::close(fd).context("failed to close symlink target file")?;

    let md = std::fs::symlink_metadata(&dst_path).context("read symlink metadata failed")?;
    assert!(
        md.file_type().is_symlink(),
        "file type should be symlink other than {:?}",
        md.file_type(),
    );
    fs::remove_file(&src_path)?; // immediate deletion
    fs::remove_file(&dst_path)?; // immediate deletion
    assert!(!src_path.exists());
    assert!(!dst_path.exists());
    Ok(())
}

/// Test bind mount a FUSE directory to a tmpfs directory
/// this test case need root privilege
#[cfg(target_os = "linux")]
#[allow(dead_code)]
fn test_bind_mount(fuse_mount_dir: &Path) -> anyhow::Result<()> {
    use nix::mount::MsFlags;

    pub fn cleanup_dir(directory: &Path) -> anyhow::Result<()> {
        let umount_res = nix::mount::umount2(directory, nix::mount::MntFlags::MNT_FORCE);
        if umount_res.is_err() {
            info!("cleanup_dir() failed to un-mount {:?}", directory);
        }
        fs::remove_dir_all(directory)
            .context(format!("cleanup_dir() failed to remove {:?}", directory))?;
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
        cleanup_dir(&from_dir).context(format!("failed to cleanup {:?}", from_dir))?;
    }
    fs::create_dir_all(&from_dir).context(format!("failed to create {:?}", from_dir))?;

    let target_dir = Path::new("/tmp/bind_target_dir");
    if target_dir.exists() {
        cleanup_dir(target_dir).context(format!("failed to cleanup {:?}", target_dir))?;
    }
    fs::create_dir_all(&target_dir).context(format!("failed to create {:?}", from_dir))?;

    nix::mount::mount::<Path, Path, Path, Path>(
        Some(&from_dir),
        target_dir,
        None, // fstype
        MsFlags::MS_BIND | MsFlags::MS_NOSUID | MsFlags::MS_NODEV | MsFlags::MS_NOEXEC,
        None, // mount option data
    )
    .context(format!(
        "failed to bind mount {:?} to {:?}",
        from_dir, target_dir,
    ))?;

    let file_path = Path::new(&target_dir).join("tmp.txt");
    fs::write(&file_path, FILE_CONTENT)?;
    let content = fs::read_to_string(&file_path)?;
    fs::remove_file(&file_path)?; // immediate deletion
    assert_eq!(content, FILE_CONTENT, "file content not match");

    nix::mount::umount(target_dir).context(format!("failed to un-mount {:?}", target_dir))?;

    // cleanup_dir(&from_dir)?;
    // cleanup_dir(target_dir)?
    fs::remove_dir_all(&from_dir).context(format!("failed to remove {:?}", from_dir))?;
    fs::remove_dir_all(target_dir).context(format!("failed to remove {:?}", target_dir))?;
    Ok(())
}

#[test]
fn run_test() -> anyhow::Result<()> {
    let mount_dir = Path::new(DEFAULT_MOUNT_DIR);
    let th = test_util::setup(mount_dir)?;
    info!("begin integration test");

    test_symlink_dir(mount_dir).context("test_symlink_dir() failed")?;
    test_symlink_file(mount_dir).context("test_symlink_file() failed")?;
    #[cfg(target_os = "linux")]
    test_bind_mount(mount_dir).context("test_bind_mount() failed")?;
    test_file_manipulation_rust_way(mount_dir)
        .context("test_file_manipulation_rust_way() failed")?;
    test_file_manipulation_nix_way(mount_dir).context("test_file_manipulation_nix_way() failed")?;
    test_dir_manipulation_nix_way(mount_dir).context("test_dir_manipulation_nix_way() failed")?;
    test_deferred_deletion(mount_dir).context("test_deferred_deletion() failed")?;
    test_rename_file_replace(mount_dir).context("test_rename_file_replace() failed")?;
    test_rename_file(mount_dir).context("test_rename_file() failed")?;
    test_rename_dir(mount_dir).context("test_rename_dir() failed")?;

    test_util::teardown(mount_dir, th)?;
    Ok(())
}

#[test]
fn run_bench() -> anyhow::Result<()> {
    let mount_dir = Path::new(BENCH_MOUNT_DIR);
    let th = test_util::setup(mount_dir)?;

    let fio_handle = std::process::Command::new("fio")
        .arg("../scripts/fio_jobs.ini")
        .env("BENCH_DIR", BENCH_MOUNT_DIR)
        .output()
        .context("fio command failed to start, maybe install fio first")?;
    let fio_res = if fio_handle.status.success() {
        std::fs::write("bench.log", &fio_handle.stdout).context("failed to write bench log")?;
        Ok(())
    } else {
        let stderr = String::from_utf8_lossy(&fio_handle.stderr);
        info!("fio failed to run, the error is: {}", &stderr);
        Err(anyhow::anyhow!(
            "fio failed to run, the error is: {}",
            &stderr,
        ))
    };

    test_util::teardown(mount_dir, th)?;
    fio_res
}
