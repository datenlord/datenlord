use log::info; // debug, error, warn
use nix::dir::Dir;
use nix::fcntl::{self, OFlag};
use nix::sys::stat::Mode;
use nix::unistd::{self, Whence};
use std::collections::HashSet;
use std::env;
use std::ffi::OsString;
use std::fs;
use std::iter;
use std::path::Path;

mod test_util;
use test_util::DEFAULT_MOUNT_DIR;
use test_util::FILE_CONTENT;

fn test_file_manipulation_rust_way(mount_dir: &Path) {
    info!("file manipulation Rust style");
    let file_path = Path::new(&mount_dir).join("tmp.txt");
    fs::write(&file_path, FILE_CONTENT).unwrap();
    let bytes = fs::read(&file_path).unwrap();
    let content = String::from_utf8(bytes).unwrap();
    fs::remove_file(&file_path).unwrap(); // immediate deletion

    assert_eq!(content, FILE_CONTENT);
    assert!(!file_path.exists());
}

fn test_file_manipulation_nix_way(mount_dir: &Path) {
    info!("file manipulation C style");
    let file_path = Path::new(&mount_dir).join("tmp.test");
    let oflags = OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_RDWR;
    let fd = fcntl::open(&file_path, oflags, Mode::empty()).unwrap();

    let write_size = unistd::write(fd, FILE_CONTENT.as_bytes()).unwrap();
    assert_eq!(FILE_CONTENT.len(), write_size);

    unistd::lseek(fd, 0, Whence::SeekSet).unwrap();
    let mut buffer: Vec<u8> = iter::repeat(0u8).take(FILE_CONTENT.len()).collect();
    let read_size = unistd::read(fd, &mut *buffer).unwrap();
    assert_eq!(FILE_CONTENT.len(), read_size);
    let content = String::from_utf8(buffer).unwrap();
    assert_eq!(content, FILE_CONTENT);
    unistd::close(fd).unwrap();

    unistd::unlink(&file_path).unwrap(); // immediate deletion
    assert!(!file_path.exists());
}

fn test_dir_manipulation_nix_way(mount_dir: &Path) {
    info!("directory manipulation C style");
    let dir_path = Path::new(&mount_dir).join("test_dir");
    let dir_mode = Mode::from_bits_truncate(0o755);
    if dir_path.exists() {
        fs::remove_dir_all(&dir_path).unwrap();
    }
    unistd::mkdir(&dir_path, dir_mode).unwrap();

    let repeat_times = 3;
    let mut sub_dirs = HashSet::new();
    for i in 0..repeat_times {
        let sub_dir_name = format!("test_sub_dir_{}", i);
        let sub_dir_path = dir_path.join(&sub_dir_name);
        unistd::mkdir(&sub_dir_path, dir_mode).unwrap();
        sub_dirs.insert(sub_dir_name);
    }

    let oflags = OFlag::O_RDONLY;
    let mut dir_fd = Dir::open(&dir_path, oflags, Mode::empty()).unwrap();
    let count = dir_fd
        .iter()
        .filter(|e| e.is_ok())
        .map(|e| e.unwrap()) // safe to use unwrap() here
        .filter(|e| {
            let bytes = e.file_name().to_bytes();
            !bytes.starts_with(&[b'.']) // skip hidden entries, '.' and '..'
        })
        .inspect(|e| {
            let bytes = e.file_name().to_bytes();
            let byte_vec = Vec::from(bytes);
            let str_name = String::from_utf8(byte_vec).unwrap();
            assert!(sub_dirs.contains(&str_name));
        })
        .count();
    assert_eq!(count, sub_dirs.len());

    fs::remove_dir_all(&dir_path).unwrap();
}

fn test_deferred_deletion(mount_dir: &Path) {
    info!("file deletion deferred");
    let file_path = Path::new(&mount_dir).join("test_file.txt");
    let oflags = OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_RDWR;
    let file_mode = Mode::from_bits_truncate(0o644);
    let fd = fcntl::open(&file_path, oflags, file_mode).unwrap();
    unistd::unlink(&file_path).unwrap(); // deferred deletion

    let repeat_times = 3;
    for _ in 0..repeat_times {
        let write_size = unistd::write(fd, FILE_CONTENT.as_bytes()).unwrap();
        assert_eq!(FILE_CONTENT.len(), write_size);
    }
    unistd::fsync(fd).unwrap();

    let mut buffer: Vec<u8> = iter::repeat(0u8)
        .take(FILE_CONTENT.len() * repeat_times)
        .collect();
    unistd::lseek(fd, 0, Whence::SeekSet).unwrap();
    let read_size = unistd::read(fd, &mut *buffer).unwrap();
    let content = String::from_utf8(buffer).unwrap();
    unistd::close(fd).unwrap();

    assert_eq!(FILE_CONTENT.len() * repeat_times, read_size);
    let str_content: String = iter::repeat(FILE_CONTENT).take(repeat_times).collect();
    assert_eq!(str_content, content);

    assert!(!file_path.exists());
}

fn test_rename_file(mount_dir: &Path) {
    info!("rename file");
    let from_dir = Path::new(&mount_dir).join("from_dir");
    if from_dir.exists() {
        fs::remove_dir_all(&from_dir).unwrap();
    }
    fs::create_dir(&from_dir).unwrap();

    let to_dir = Path::new(&mount_dir).join("to_dir");
    if to_dir.exists() {
        fs::remove_dir_all(&to_dir).unwrap();
    }
    fs::create_dir(&to_dir).unwrap();

    let old_file = from_dir.join("old.txt");
    fs::write(&old_file, FILE_CONTENT).unwrap();

    let new_file = to_dir.join("new.txt");

    fs::rename(&old_file, &new_file).unwrap();

    let bytes = fs::read(&new_file).unwrap();
    let content = String::from_utf8(bytes).unwrap();
    assert_eq!(content, FILE_CONTENT);

    assert!(!old_file.exists());
    assert!(new_file.exists());

    fs::remove_dir_all(&from_dir).unwrap();
    assert!(!from_dir.exists());
    fs::remove_dir_all(&to_dir).unwrap();
    assert!(!to_dir.exists());
}

fn test_rename_file_no_replace(mount_dir: &Path) {
    info!("rename file no replace");
    let oflags = OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_RDWR;
    let file_mode = Mode::from_bits_truncate(0o644);

    let old_file = Path::new(&mount_dir).join("old.txt");
    let old_fd = fcntl::open(&old_file, oflags, file_mode).unwrap();
    let write_size = unistd::write(old_fd, FILE_CONTENT.as_bytes()).unwrap();
    assert_eq!(FILE_CONTENT.len(), write_size);

    let new_file = Path::new(&mount_dir).join("new.txt");
    let new_fd = fcntl::open(&new_file, oflags, file_mode).unwrap();
    let write_size = unistd::write(new_fd, FILE_CONTENT.as_bytes()).unwrap();
    assert_eq!(FILE_CONTENT.len(), write_size);

    fs::rename(&old_file, &new_file).expect_err(&format!("rename no replace should fail"));

    let mut buffer: Vec<u8> = iter::repeat(0u8).take(FILE_CONTENT.len()).collect();
    unistd::lseek(old_fd, 0, Whence::SeekSet).unwrap();
    let read_size = unistd::read(old_fd, &mut *buffer).unwrap();
    let content = String::from_utf8(buffer).unwrap();
    unistd::close(old_fd).unwrap();
    assert_eq!(FILE_CONTENT.len(), read_size);
    assert_eq!(FILE_CONTENT, content);

    let mut buffer: Vec<u8> = iter::repeat(0u8).take(FILE_CONTENT.len()).collect();
    unistd::lseek(new_fd, 0, Whence::SeekSet).unwrap();
    let read_size = unistd::read(new_fd, &mut *buffer).unwrap();
    let content = String::from_utf8(buffer).unwrap();
    unistd::close(new_fd).unwrap();
    assert_eq!(FILE_CONTENT.len(), read_size);
    assert_eq!(FILE_CONTENT, content);

    assert!(old_file.exists());
    assert!(new_file.exists());

    fs::remove_file(&old_file).unwrap();
    assert!(!old_file.exists());
    fs::remove_file(&new_file).unwrap();
    assert!(!new_file.exists());
}

fn test_rename_dir(mount_dir: &Path) {
    info!("rename directory");
    let from_dir = Path::new(&mount_dir).join("from_dir");
    if from_dir.exists() {
        fs::remove_dir_all(&from_dir).unwrap();
    }
    fs::create_dir(&from_dir).unwrap();

    let to_dir = Path::new(&mount_dir).join("to_dir");
    if to_dir.exists() {
        fs::remove_dir_all(&to_dir).unwrap();
    }
    fs::create_dir(&to_dir).unwrap();

    let old_sub_dir = from_dir.join("old_sub");
    fs::create_dir(&old_sub_dir).unwrap();
    let new_sub_dir = to_dir.join("new_sub");
    fs::rename(&old_sub_dir, &new_sub_dir).unwrap();

    assert!(!old_sub_dir.exists());
    assert!(new_sub_dir.exists());

    fs::remove_dir_all(&from_dir).unwrap();
    assert!(!from_dir.exists());
    fs::remove_dir_all(&to_dir).unwrap();
    assert!(!to_dir.exists());
}

#[test]
fn run_test() {
    let mountpoint = match env::args_os().nth(1) {
        Some(path) => path,
        None => OsString::from(DEFAULT_MOUNT_DIR),
    };
    let mount_dir = Path::new(&mountpoint);

    let th = test_util::setup(&mount_dir);

    info!("begin integration test");
    test_file_manipulation_rust_way(&mount_dir);
    test_file_manipulation_nix_way(&mount_dir);
    test_dir_manipulation_nix_way(&mount_dir);
    test_deferred_deletion(&mount_dir);
    test_rename_file_no_replace(&mount_dir);
    test_rename_file(&mount_dir);
    test_rename_dir(&mount_dir);

    test_util::teardown(&mount_dir, th);
}
