const BLOCK_SIZE_IN_BYTES: usize = 8;
const BLOCK_CONTENT: &[u8; BLOCK_SIZE_IN_BYTES] = b"foo bar ";
const BACKEND_ROOT: &str = "/tmp/opendal";

mod common;
mod pessimistic;

use opendal::services::Fs;
use opendal::Operator;

use super::Backend;

/// Prepare a backend
fn prepare_backend(root: &str) -> Backend {
    let mut builder = Fs::default();
    builder.root(root);
    let op = Operator::new(builder).unwrap().finish();

    Backend::new(op, BLOCK_SIZE_IN_BYTES)
}
