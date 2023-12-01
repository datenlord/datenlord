use std::fmt::Write;

use async_trait::async_trait;
use clippy_utilities::{Cast, OverflowArithmetic};
#[cfg(test)]
use mockall::{automock, predicate::str};
use s3::bucket::Bucket;
use s3::bucket_ops::BucketConfiguration;
use s3::command::{Command, Multipart};
use s3::creds::Credentials;
use s3::request_trait::Request;
use s3::serde_types::{CompleteMultipartUploadData, InitiateMultipartUploadResponse, Part};
use s3::surf_request::SurfRequest as RequestImpl;
use s3::Region;
use serde_xml_rs as serde_xml;

use super::cache::IoMemBlock;
use crate::async_fuse::fuse::protocol::INum;

/// S3 backend error
#[derive(thiserror::Error, Debug)]
pub enum S3Error {
    /// S3 backend internal error
    #[error("S3InternalError, the error is {0}")]
    S3InternalError(String),
}

/// S3 backend result
pub type S3Result<T> = Result<T, S3Error>;

/// S3 backend
#[allow(clippy::arithmetic_side_effects, clippy::indexing_slicing)]
#[cfg_attr(test, automock)]
#[async_trait]
pub trait S3BackEnd {
    /// New `S3BackEnd`
    async fn new_backend(
        bucket_name: &str,
        endpoint: &str,
        access_key: &str,
        secret_key: &str,
    ) -> S3Result<Self>
    where
        Self: Sized;
    /// Get data of a whole file from S3 backend
    async fn get_data(&self, file: INum) -> S3Result<Vec<u8>>;
    /// Get partial data of a file from S3 backend
    async fn get_partial_data(&self, file: INum, offset: usize, len: usize) -> S3Result<Vec<u8>>;
    /// Put data of a file to S3 backend
    async fn put_data(&self, file: INum, data: &[u8], offset: usize, len: usize) -> S3Result<()>;
    /// Put data vector of a file to S3 backend
    async fn put_data_vec(&self, file: INum, data: Vec<IoMemBlock>) -> S3Result<()>;
    /// Delete a file from S3 backend
    async fn delete_data(&self, file: INum) -> S3Result<()>;
}

/// S3 backend implementation
#[derive(Debug)]
pub struct S3BackEndImpl {
    /// S3 bucket
    bucket: Bucket,
}

/// Transfer anyhow error to `S3Error`
macro_rules! resultify_anyhow {
    ($e:expr) => {
        match $e {
            Ok(data) => Ok(data),
            Err(ref anyhow_error) => {
                Err(S3Error::S3InternalError(format_anyhow_error(anyhow_error)))
            }
        }
    };
}

#[async_trait]
impl S3BackEnd for S3BackEndImpl {
    #[allow(dead_code)]
    async fn new_backend(
        bucket_name: &str,
        endpoint: &str,
        access_key: &str,
        secret_key: &str,
    ) -> S3Result<Self> {
        let region = Region::Custom {
            region: "fake_region".into(),
            endpoint: endpoint.into(),
        };
        let credentials = Credentials::new(Some(access_key), Some(secret_key), None, None, None)
            .unwrap_or_else(|e| panic!("failed to create credentials, error is {e:?}"));
        let config = BucketConfiguration::default();
        let bucket = Bucket::create_with_path_style(bucket_name, region, credentials, config)
            .await
            .unwrap_or_else(|e| panic!("failed to create bucket, error is {e:?}"));

        if !bucket.success() && bucket.response_code != 409 {
            return Err(S3Error::S3InternalError(format!(
                "S3 create bucket response code: {}, response message: {}",
                bucket.response_code, bucket.response_text,
            )));
        }

        Ok(Self {
            bucket: bucket.bucket,
        })
    }

    async fn get_data(&self, file: INum) -> S3Result<Vec<u8>> {
        resultify_anyhow!(self.bucket.get_object(file.to_string()).await).map(|(data, _)| data)
    }

    async fn get_partial_data(&self, file: INum, offset: usize, len: usize) -> S3Result<Vec<u8>> {
        resultify_anyhow!(
            self.bucket
                .get_object_range(
                    file.to_string(),
                    offset.cast(),
                    Some((offset.overflow_add(len)).cast())
                )
                .await
        )
        .map(|(data, _)| data)
    }

    async fn put_data(&self, file: INum, data: &[u8], offset: usize, len: usize) -> S3Result<()> {
        resultify_anyhow!(
            self.bucket
                //.put_object(file.to_owned(), &data[offset..(offset.overflow_add(len))])
                .put_object(
                    file.to_string(),
                    data.get(offset..(offset.overflow_add(len)))
                        .unwrap_or_else(|| panic!(
                            "failed to get slice index {}..{}, slice size={}",
                            offset,
                            offset.overflow_add(len),
                            data.len()
                        ))
                )
                .await
        )
        .map(|_| ())
    }

    async fn delete_data(&self, data: INum) -> S3Result<()> {
        resultify_anyhow!(self.bucket.delete_object(data.to_string()).await).map(|_| ())
    }

    async fn put_data_vec(&self, file: INum, vec: Vec<IoMemBlock>) -> S3Result<()> {
        if vec.is_empty() {
            return Ok(());
        }

        if vec.len() == 1 {
            let buf = unsafe {
                vec.get(0)
                    .unwrap_or_else(|| panic!("put_data_vec() vec is empty"))
                    .as_slice()
            };
            return self.put_data(file, buf, 0, buf.len()).await;
        }

        let command = Command::InitiateMultipartUpload;
        let file_inum_str = file.to_string();
        let request = RequestImpl::new(&self.bucket, &file_inum_str, command);
        let (data, _) = resultify_anyhow!(request.response_data(false).await)?;
        let msg: InitiateMultipartUploadResponse = serde_xml::from_str(
            std::str::from_utf8(data.as_slice())
                .map_err(|e| S3Error::S3InternalError(format!("{e}")))?,
        )
        .map_err(|e| S3Error::S3InternalError(format!("{e}")))?;
        let path = msg.key;
        let upload_id = &msg.upload_id;

        let mut etags = Vec::new();

        let last_index = vec.len().overflow_sub(1);
        for (part_number, (index, d)) in vec.iter().enumerate().enumerate() {
            let command = Command::PutObject {
                content: unsafe { d.as_slice() },
                content_type: "application/octet-stream",
                multipart: Some(Multipart::new(part_number.cast(), upload_id)), /* upload_id:
                                                                                 * &msg.upload_id,
                                                                                 */
            };
            let request = RequestImpl::new(&self.bucket, &path, command);
            let (data, _code) = resultify_anyhow!(request.response_data(true).await)?;
            let etag = std::str::from_utf8(data.as_slice())
                .map_err(|e| S3Error::S3InternalError(format!("{e}")))?;

            etags.push(etag.to_owned());

            if index == last_index {
                let inner_data = etags
                    .iter()
                    .enumerate()
                    .map(|(i, x)| Part {
                        etag: x.clone(),
                        part_number: i.cast::<u32>().overflow_add(1),
                    })
                    .collect::<Vec<Part>>();
                let data = CompleteMultipartUploadData { parts: inner_data };
                let complete = Command::CompleteMultipartUpload {
                    upload_id: &msg.upload_id,
                    data,
                };
                let complete_request = RequestImpl::new(&self.bucket, &path, complete);
                let (_, _) = resultify_anyhow!(complete_request.response_data(false).await)?;
            }
        }
        Ok(())
    }
}

/// Format anyhow error to string
#[must_use]
pub fn format_anyhow_error(error: &anyhow::Error) -> String {
    let err_msg_vec = error
        .chain()
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>();
    let mut err_msg = String::new();
    let _ignore = write!(
        err_msg,
        "{}, root cause: {}",
        err_msg_vec.as_slice().join(", caused by: "),
        error.root_cause()
    );

    err_msg
}

#[derive(Debug)]
/// Do nothing S3 backend
pub struct DoNothingImpl;

#[async_trait]
impl S3BackEnd for DoNothingImpl {
    #[allow(dead_code)]
    async fn new_backend(_: &str, _: &str, _: &str, _: &str) -> S3Result<Self> {
        Ok(Self {})
    }

    async fn get_data(&self, _: INum) -> S3Result<Vec<u8>> {
        Ok(vec![])
    }

    async fn get_partial_data(&self, _: INum, _: usize, _: usize) -> S3Result<Vec<u8>> {
        Ok(vec![])
    }

    async fn put_data(&self, _: INum, _: &[u8], _: usize, _: usize) -> S3Result<()> {
        Ok(())
    }

    async fn delete_data(&self, _: INum) -> S3Result<()> {
        Ok(())
    }

    async fn put_data_vec(&self, _: INum, _: Vec<IoMemBlock>) -> S3Result<()> {
        Ok(())
    }
}
