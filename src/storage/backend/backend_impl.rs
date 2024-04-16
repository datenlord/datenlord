//! The backend implementation.

use std::time::Duration;

use async_trait::async_trait;
use clippy_utilities::OverflowArithmetic;
use datenlord::config::{StorageParams, StorageS3Config};
use datenlord::metrics::DATENLORD_REGISTRY;
use futures::{stream, AsyncReadExt, AsyncWriteExt, StreamExt};
use opendal::layers::{ConcurrentLimitLayer, PrometheusLayer, RetryLayer};
use opendal::services::{Fs, S3};
use opendal::{ErrorKind, Operator};
use prometheus::{exponential_buckets, linear_buckets};
use tokio::time::sleep;
use tracing::debug;

use crate::async_fuse::fuse::protocol::INum;
use crate::storage::error::StorageResult;
use crate::storage::{Block, Storage};

/// The maximum number of retries for writing a block.
const MAX_RETRIES: usize = 3;
/// The delay between retries for writing a block.
const RETRY_DELAY: Duration = Duration::from_secs(1);

/// Get file path by `ino`
fn get_file_path(ino: INum) -> String {
    format!("{ino}/")
}

/// Get block path by `ino` and `block_id`
fn get_block_path(ino: INum, block_id: usize) -> String {
    format!("{ino}/{block_id}.block")
}

/// A builder to build `BackendWrapper`.
#[derive(Debug)]
pub struct BackendBuilder {
    /// The storage config
    config: StorageParams,
    /// The size of a block
    block_size: usize,
}

impl BackendBuilder {
    /// Create a backend builder.
    #[must_use]
    pub fn new(config: StorageParams, block_size: usize) -> Self {
        Self { config, block_size }
    }

    /// Build the backend.
    #[allow(clippy::expect_used, clippy::unwrap_in_result)] // `.expect()` here are ensured not to panic.
    pub async fn build(self) -> opendal::Result<Backend> {
        let BackendBuilder { config, block_size } = self;

        let layer = PrometheusLayer::with_registry(DATENLORD_REGISTRY.clone())
            .bytes_total_buckets(
                exponential_buckets(1024.0, 2.0, 10).expect("Arguments are legal."),
            )
            .requests_duration_seconds_buckets(
                linear_buckets(0.005, 0.005, 20).expect("Arguments are legal."),
            );

        let operator = match config {
            StorageParams::S3(StorageS3Config {
                ref endpoint_url,
                ref access_key_id,
                ref secret_access_key,
                ref bucket_name,
                ref region,
                ref max_concurrent_requests,
            }) => {
                let mut builder = S3::default();

                builder
                    .endpoint(endpoint_url)
                    .access_key_id(access_key_id)
                    .secret_access_key(secret_access_key)
                    .bucket(bucket_name);

                // Init region
                if let Some(region) = region.to_owned() {
                    builder.region(region.as_str());
                } else {
                    // Auto detect region
                    if let Some(region) = S3::detect_region(endpoint_url, bucket_name).await {
                        builder.region(region.as_str());
                    } else {
                        builder.region("auto");
                    }
                }

                // For aws s3 issue: https://repost.aws/questions/QU_F-UC6-fSdOYzp-gZSDTvQ/receiving-s3-503-slow-down-responses
                // 3,500 PUT/COPY/POST/DELETE or 5,500 GET/HEAD requests per second per prefix in a bucket
                let valid_max_concurrent_requests = max_concurrent_requests.map_or(1000, |v| v);

                let conncurrency_layer =
                    ConcurrentLimitLayer::new(valid_max_concurrent_requests.to_owned());
                let retry_layer = RetryLayer::new();

                Operator::new(builder)?
                    .layer(layer)
                    .layer(conncurrency_layer)
                    .layer(retry_layer)
                    .finish()
            }
            StorageParams::Fs(ref root) => {
                let mut builder = Fs::default();
                builder.root(root);
                Operator::new(builder)?.layer(layer).finish()
            }
        };

        Ok(Backend {
            operator,
            block_size,
        })
    }
}

/// The backend wrapper of `openDAL` operator.
#[derive(Debug)]
pub struct Backend {
    /// The inner `Operator`
    operator: Operator,
    /// Block size
    block_size: usize,
}

impl Backend {
    /// Create a new backend
    #[must_use]
    pub fn new(operator: Operator, block_size: usize) -> Self {
        Self {
            operator,
            block_size,
        }
    }
}

#[async_trait]
impl Storage for Backend {
    async fn load_from_self(&self, ino: INum, block_id: usize) -> StorageResult<Option<Block>> {
        let mut block = Block::new_zeroed(self.block_size);

        let mut reader = self.operator.reader(&get_block_path(ino, block_id)).await?;
        let mut offset = 0;
        // Check if the reader point is at the end of the file.
        loop {
            match reader
                .read(block.make_mut_slice().get_mut(offset..).ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::Other, "slice bounds out of range")
                })?)
                .await
            {
                Ok(0) => {
                    // The reader point is at the end of the file.
                    break;
                }
                Ok(bytes_read) => {
                    // The block is not full, continue to read.
                    offset += bytes_read;
                    if offset == self.block_size {
                        // The block is full, process is done.
                        break;
                    }
                }
                Err(e) => {
                    // Meet an error.
                    if e.kind() == std::io::ErrorKind::NotFound {
                        return Ok(None);
                    }
                    return Err(e.into());
                }
            }
        }

        Ok(Some(block))
    }

    async fn load_from_backend(&self, _: INum, _: usize) -> StorageResult<Option<Block>> {
        // This storage has no backend.
        Ok(None)
    }

    async fn cache_block_from_backend(&self, _: INum, _: usize, _: Block) -> StorageResult<()> {
        unreachable!("This storage has no backend, and has no cache.");
    }

    async fn store(&self, ino: INum, block_id: usize, block: Block) -> StorageResult<()> {
        let path = get_block_path(ino, block_id);

        let block_start = block.start();
        let block_end = block.end();

        if block_start == 0 && block_end == self.block_size {
            // To store a whole block
            let mut writer = self.operator.writer(&path).await?;

            // Retry for a few times if the write fails.
            for attempt in 0..MAX_RETRIES {
                match writer.write_all(block.as_slice()).await {
                    Ok(()) => {
                        writer.close().await?;
                        return Ok(());
                    }
                    Err(_) if attempt < MAX_RETRIES - 1 => {
                        debug!(
                            "Failed to write block, retrying after {} seconds.",
                            RETRY_DELAY.as_secs()
                        );
                        sleep(RETRY_DELAY).await;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
        }

        let mut dest = match self.operator.read(&path).await {
            Ok(dest) => dest,
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    // Create an empty block for overwriting is ok.
                    vec![]
                } else {
                    return Err(e.into());
                }
            }
        };

        // Ensure that the vector is long enough to be overwritten
        if dest.len() < block_end {
            dest.resize(block_end, 0);
        }

        // merge two blocks
        dest.get_mut(block_start..block_end)
            .unwrap_or_else(|| unreachable!("The vector is ensured to be long enough."))
            .copy_from_slice(block.as_slice());

        // Retry for a few times if the write fails.
        for attempt in 0..MAX_RETRIES {
            match self.operator.write(&path, dest.clone()).await {
                Ok(()) => return Ok(()),
                Err(_) if attempt < MAX_RETRIES - 1 => {
                    debug!(
                        "Failed to write block, retrying after {} seconds.",
                        RETRY_DELAY.as_secs()
                    );
                    sleep(RETRY_DELAY).await;
                }
                Err(e) => return Err(e.into()),
            }
        }

        Ok(())
    }

    async fn remove(&self, ino: INum) -> StorageResult<()> {
        self.operator.remove_all(&get_file_path(ino)).await?;

        Ok(())
    }

    async fn invalidate(&self, _: INum) -> StorageResult<()> {
        // This storage has no cache, therefore, its contents cannot be
        // invalidated.
        Ok(())
    }

    async fn flush(&self, _: INum) -> StorageResult<()> {
        // This storage has no cache and backend, therefore, there is no need to
        // flush its data.
        Ok(())
    }

    async fn flush_all(&self) -> StorageResult<()> {
        // This storage has no cache and backend, therefore, there is no need to
        // flush its data.
        Ok(())
    }

    async fn truncate(
        &self,
        ino: INum,
        from_block: usize,
        to_block: usize,
        fill_start: usize,
    ) -> StorageResult<()> {
        let paths =
            stream::iter(to_block..from_block).map(|block_id| get_block_path(ino, block_id));

        let file_path = get_file_path(ino);

        let file_exists = self.operator.is_exist(&file_path).await.unwrap_or(false);

        if file_exists {
            if to_block == 0 {
                self.operator.remove_all(&file_path).await?;
                return Ok(());
            }

            self.operator.remove_via(paths).await?;

            // truncate the last block
            if to_block > 0 && fill_start < self.block_size {
                let truncate_block_id = to_block.overflow_sub(1);
                let path = get_block_path(ino, truncate_block_id);
                match self.operator.read(&path).await {
                    Ok(mut dest) => {
                        dest.truncate(fill_start);
                        // Retry for a few times if the write fails.
                        for attempt in 0..MAX_RETRIES {
                            match self.operator.write(&path, dest.clone()).await {
                                Ok(()) => return Ok(()),
                                Err(_) if attempt < MAX_RETRIES - 1 => {
                                    debug!(
                                        "Failed to write block, retrying after {} seconds.",
                                        RETRY_DELAY.as_secs()
                                    );
                                    sleep(RETRY_DELAY).await;
                                }
                                Err(e) => return Err(e.into()),
                            }
                        }
                    }
                    Err(e) => {
                        // It's OK that the block is not found for truncate.
                        if e.kind() != ErrorKind::NotFound {
                            return Err(e.into());
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
