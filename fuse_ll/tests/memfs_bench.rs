use log::info; // debug, error, warn
use std::env;
use std::ffi::OsString;
use std::path::Path;

mod test_util;
use test_util::DEFAULT_MOUNT_DIR;

#[test]
fn bench_test() {
    let mountpoint = match env::args_os().nth(1) {
        Some(path) => path,
        None => OsString::from(DEFAULT_MOUNT_DIR),
    };
    let mount_dir = Path::new(&mountpoint);

    let th = test_util::setup(&mount_dir);

    let repeat_times: usize = 10000;
    info!("begin benchmark test");
    test_util::benchmark(mount_dir, repeat_times);

    test_util::teardown(&mount_dir, th);
}
