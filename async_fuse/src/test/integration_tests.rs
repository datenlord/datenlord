use log::info; // debug, warn
use nix::dir::Dir;
use nix::fcntl::{self, OFlag};
use nix::sys::stat::Mode;
use nix::unistd::{self, Whence};
use std::collections::HashSet;
use std::fs;
use std::iter;
use std::path::Path;

use super::test_util::{self, DEFAULT_MOUNT_DIR, FILE_CONTENT};

fn test_file_manipulation_rust_way(mount_dir: &Path) -> anyhow::Result<()> {
    info!("file manipulation Rust style");
    let file_path = Path::new(&mount_dir).join("tmp.txt");
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

fn test_file_manipulation_nix_way(mount_dir: &Path) -> anyhow::Result<()> {
    info!("file manipulation C style");
    let file_path = Path::new(&mount_dir).join("tmp.test");
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
    let mut buffer: Vec<u8> = iter::repeat(0u8).take(FILE_CONTENT.len()).collect();
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

fn test_dir_manipulation_nix_way(mount_dir: &Path) -> anyhow::Result<()> {
    info!("directory manipulation C style");
    let dir_path = Path::new(&mount_dir).join("test_dir");
    let dir_mode = Mode::from_bits_truncate(0o755);
    if dir_path.exists() {
        fs::remove_dir_all(&dir_path)?;
    }
    unistd::mkdir(&dir_path, dir_mode)?;

    let repeat_times = 3;
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
        .filter(nix::Result::is_ok)
        .map(|e| -> nix::dir::Entry {
            e.unwrap() // safe to use unwrap() here
        })
        .filter(|e| {
            let bytes = e.file_name().to_bytes();
            !bytes.starts_with(&[b'.']) // skip hidden entries, '.' and '..'
        })
        .map(|e| -> anyhow::Result<()> {
            let bytes = e.file_name().to_bytes(); // safe to use unwrap() here
            let byte_vec = Vec::from(bytes);
            let str_name = String::from_utf8(byte_vec)?;
            assert!(
                sub_dirs.contains(&str_name),
                "the sub directory name {} should be in the hashmap {:?}",
                str_name,
                sub_dirs,
            );
            Ok(())
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

fn test_deferred_deletion(mount_dir: &Path) -> anyhow::Result<()> {
    info!("file deletion deferred");
    let file_path = Path::new(&mount_dir).join("test_file.txt");
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

    let mut buffer: Vec<u8> = iter::repeat(0u8)
        .take(FILE_CONTENT.len() * repeat_times)
        .collect();
    unistd::lseek(fd, 0, Whence::SeekSet)?;
    let read_size = unistd::read(fd, &mut *buffer)?;
    let content = String::from_utf8(buffer)?;
    unistd::close(fd)?;

    assert_eq!(
        read_size,
        FILE_CONTENT.len() * repeat_times,
        "the file read size {} is not the same as the expected size {}",
        read_size,
        FILE_CONTENT.len() * repeat_times,
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

fn test_rename_file(mount_dir: &Path) -> anyhow::Result<()> {
    info!("rename file");
    let from_dir = Path::new(&mount_dir).join("from_dir");
    if from_dir.exists() {
        fs::remove_dir_all(&from_dir)?;
    }
    fs::create_dir(&from_dir)?;

    let to_dir = Path::new(&mount_dir).join("to_dir");
    if to_dir.exists() {
        fs::remove_dir_all(&to_dir)?;
    }
    fs::create_dir(&to_dir)?;

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

fn test_rename_file_no_replace(mount_dir: &Path) -> anyhow::Result<()> {
    info!("rename file no replace");
    let oflags = OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_RDWR;
    let file_mode = Mode::from_bits_truncate(0o644);

    let old_file = Path::new(&mount_dir).join("old.txt");
    let old_fd = fcntl::open(&old_file, oflags, file_mode)?;
    let write_size = unistd::write(old_fd, FILE_CONTENT.as_bytes())?;
    assert_eq!(
        write_size,
        FILE_CONTENT.len(),
        "the file write size {} is not the same as the expected size {}",
        write_size,
        FILE_CONTENT.len(),
    );

    let new_file = Path::new(&mount_dir).join("new.txt");
    let new_fd = fcntl::open(&new_file, oflags, file_mode)?;
    let write_size = unistd::write(new_fd, FILE_CONTENT.as_bytes())?;
    assert_eq!(
        write_size,
        FILE_CONTENT.len(),
        "the file write size {} is not the same as the expected size {}",
        write_size,
        FILE_CONTENT.len(),
    );

    fs::rename(&old_file, &new_file).expect_err(&"rename no replace should fail".to_string());

    let mut buffer: Vec<u8> = iter::repeat(0u8).take(FILE_CONTENT.len()).collect();
    unistd::lseek(old_fd, 0, Whence::SeekSet)?;
    let read_size = unistd::read(old_fd, &mut *buffer)?;
    let content = String::from_utf8(buffer)?;
    unistd::close(old_fd)?;
    assert_eq!(
        read_size,
        FILE_CONTENT.len(),
        "the file read size {} is not the same as the expected size {}",
        read_size,
        FILE_CONTENT.len(),
    );
    assert_eq!(
        content, FILE_CONTENT,
        "the file read result is not the same as the expected content",
    );

    let mut buffer: Vec<u8> = iter::repeat(0u8).take(FILE_CONTENT.len()).collect();
    unistd::lseek(new_fd, 0, Whence::SeekSet)?;
    let read_size = unistd::read(new_fd, &mut *buffer)?;
    let content = String::from_utf8(buffer)?;
    unistd::close(new_fd)?;
    assert_eq!(
        read_size,
        FILE_CONTENT.len(),
        "the file read size {} is not the same as the expected size {}",
        read_size,
        FILE_CONTENT.len(),
    );
    assert_eq!(
        content, FILE_CONTENT,
        "the file read result is not the same as the expected content",
    );

    assert!(
        old_file.exists(),
        "the old file {:?} should exist",
        new_file,
    );
    assert!(
        new_file.exists(),
        "the new file {:?} should exist",
        new_file,
    );

    // Clean up
    fs::remove_file(&old_file)?;
    fs::remove_file(&new_file)?;
    Ok(())
}

fn test_rename_dir(mount_dir: &Path) -> anyhow::Result<()> {
    info!("rename directory");
    let from_dir = Path::new(&mount_dir).join("from_dir");
    if from_dir.exists() {
        fs::remove_dir_all(&from_dir)?;
    }
    fs::create_dir(&from_dir)?;

    let to_dir = Path::new(&mount_dir).join("to_dir");
    if to_dir.exists() {
        fs::remove_dir_all(&to_dir)?;
    }
    fs::create_dir(&to_dir)?;

    let old_sub_dir = from_dir.join("old_sub");
    fs::create_dir(&old_sub_dir)?;
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

#[test]
fn run_test() -> anyhow::Result<()> {
    let mount_dir = Path::new(DEFAULT_MOUNT_DIR);

    let th = test_util::setup(&mount_dir)?;

    info!("begin integration test");
    test_file_manipulation_rust_way(&mount_dir)?;
    test_file_manipulation_nix_way(&mount_dir)?;
    test_dir_manipulation_nix_way(&mount_dir)?;
    test_deferred_deletion(&mount_dir)?;
    // TODO: enable thiese test after implemented rename()
    // test_rename_file_no_replace(&mount_dir)?;
    // test_rename_file(&mount_dir)?;
    // test_rename_dir(&mount_dir)?;

    test_util::teardown(&mount_dir, th)?;
    Ok(())
}
