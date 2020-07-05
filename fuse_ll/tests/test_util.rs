use log::{debug, info}; // error, warn
use nix::fcntl::{self, OFlag};
use nix::sys::stat::Mode;
use nix::unistd::{self, Whence};
use std::fs;
use std::iter;
use std::path::Path;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use fuse_ll::fuse;
use fuse_ll::memfs::MemoryFilesystem;

pub const DEFAULT_MOUNT_DIR: &str = "../fuse_test";
pub const FILE_CONTENT: &str = "0123456789ABCDEF";

pub fn setup(mount_dir: &Path) -> JoinHandle<()> {
    env_logger::init();
    let result = fuse::unmount(mount_dir);
    if result.is_ok() {
        debug!("umount {:?} before setup", mount_dir);
    }

    if mount_dir.exists() {
        fs::remove_dir_all(mount_dir).unwrap();
    }
    fs::create_dir_all(mount_dir).unwrap();
    let abs_root_path = fs::canonicalize(mount_dir).unwrap();

    let options = [
        // "-d",
        //"-r",
        //"-s",
        //"-f",
        //"-o",
        //"debug",
        "fsname=fuse_rs_demo",
        "ro",
        "allow_other",
    ]
    .iter()
    .map(|o| o.as_ref())
    .collect::<Vec<&str>>();

    let fs = MemoryFilesystem::new(&abs_root_path);

    let th = thread::spawn(move || {
        info!("begin mount thread");
        fuse::mount(fs, &abs_root_path, &options)
            .unwrap_or_else(|_| panic!("Couldn't mount filesystem: {:?}", abs_root_path));
    });

    let seconds = 2;
    info!("sleep {} seconds", seconds);
    thread::sleep(Duration::new(seconds, 0));

    debug!(
        "euid={}, egid={}, uid={}, gid={}",
        unistd::geteuid(),
        unistd::getegid(),
        unistd::getuid(),
        unistd::getgid(),
    );
    // unistd::seteuid(Uid::from_raw(0)).unwrap();
    // unistd::setegid(Gid::from_raw(0)).unwrap();
    // debug!(
    //     "euid={}, egid={}, uid={}, gid={}",
    //     unistd::geteuid(), unistd::getegid(), unistd::getuid(), unistd::getgid(),
    // );
    info!("setup finished");
    th
}

pub fn teardown(mount_dir: &Path, th: JoinHandle<()>) {
    info!("begin teardown");

    debug!(
        "euid={}, egid={}, uid={}, gid={}",
        unistd::geteuid(),
        unistd::getegid(),
        unistd::getuid(),
        unistd::getgid(),
    );
    // unistd::seteuid(Uid::from_raw(0)).unwrap();
    // unistd::setegid(Gid::from_raw(0)).unwrap();
    // debug!(
    //     "euid={}, egid={}, uid={}, gid={}",
    //     unistd::geteuid(), unistd::getegid(), unistd::getuid(), unistd::getgid(),
    // );

    fuse::unmount(&mount_dir).unwrap();
    let abs_mount_path = fs::canonicalize(mount_dir).unwrap();
    fs::remove_dir_all(&abs_mount_path).unwrap();
    th.join().unwrap();
}

pub fn benchmark(mount_dir: &Path, repeat_times: usize) {
    let dir_path = Path::new(&mount_dir).join("bench_dir");
    if dir_path.exists() {
        fs::remove_dir_all(&dir_path).unwrap();
    }
    fs::create_dir_all(&dir_path).unwrap();

    let file_path = dir_path.join("bench_test.txt");
    let oflags = OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_RDWR;
    let file_mode = Mode::from_bits_truncate(0o644);
    let fd = fcntl::open(&file_path, oflags, file_mode).unwrap();
    unistd::unlink(&file_path).unwrap(); // deferred deletion

    info!("repeat write {} times", repeat_times);
    for _ in 0..repeat_times {
        let content = String::from(FILE_CONTENT);
        let write_size = unistd::write(fd, content.as_bytes()).unwrap();
        assert_eq!(content.len(), write_size);
    }
    unistd::fsync(fd).unwrap();

    info!("repeat read {} times", repeat_times);
    let mut buffer: Vec<u8> = iter::repeat(0u8)
        .take(FILE_CONTENT.len() * repeat_times)
        .collect();
    for _ in 0..repeat_times {
        unistd::lseek(fd, 0, Whence::SeekSet).unwrap();
        let read_size = unistd::read(fd, &mut *buffer).unwrap();
        assert_eq!(FILE_CONTENT.len() * repeat_times, read_size);
    }

    unistd::close(fd).unwrap();
    fs::remove_dir_all(&dir_path).unwrap();
}
