use std::fmt::Write;
use std::time::SystemTime;

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
#[allow(clippy::integer_arithmetic, clippy::indexing_slicing)]
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
    /// Get data of a file from S3 backend
    async fn get_data(&self, file: &str) -> S3Result<Vec<u8>>;
    /// Get partial data of a file from S3 backend
    async fn get_partial_data(&self, file: &str, offset: usize, len: usize) -> S3Result<Vec<u8>>;
    /// Put data of a file to S3 backend
    async fn put_data(&self, file: &str, data: &[u8], offset: usize, len: usize) -> S3Result<()>;
    /// Put data vector of a file to S3 backend
    async fn put_data_vec(&self, file: &str, data: Vec<IoMemBlock>) -> S3Result<()>;
    /// Delete a file from S3 backend
    async fn delete_data(&self, file: &str) -> S3Result<()>;
    /// List file from S3 backend
    async fn list_file(&self, dir: &str) -> S3Result<Vec<String>>;
    /// Create a dir to S3 backend
    async fn create_dir(&self, dir: &str) -> S3Result<()>;
    /// Get len of a file from S3 backend
    async fn get_len(&self, file: &str) -> S3Result<usize>;
    /// Get last modified time of a file from S3 backend
    async fn get_last_modified(&self, file: &str) -> S3Result<SystemTime>;
    /// Get meta of a file from S3 backend
    async fn get_meta(&self, file: &str) -> S3Result<(usize, SystemTime)>;
    /// Rename a file to S3 backend
    async fn rename(&self, old_file: &str, new_file: &str) -> S3Result<()>;
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

    async fn get_data(&self, file: &str) -> S3Result<Vec<u8>> {
        resultify_anyhow!(self.bucket.get_object(file.to_owned()).await).map(|(data, _)| data)
    }

    async fn get_partial_data(&self, file: &str, offset: usize, len: usize) -> S3Result<Vec<u8>> {
        resultify_anyhow!(
            self.bucket
                .get_object_range(
                    file.to_owned(),
                    offset.cast(),
                    Some((offset.overflow_add(len)).cast())
                )
                .await
        )
        .map(|(data, _)| data)
    }

    async fn put_data(&self, file: &str, data: &[u8], offset: usize, len: usize) -> S3Result<()> {
        resultify_anyhow!(
            self.bucket
                //.put_object(file.to_owned(), &data[offset..(offset.overflow_add(len))])
                .put_object(
                    file.to_owned(),
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

    async fn get_len(&self, file: &str) -> S3Result<usize> {
        match resultify_anyhow!(self.bucket.head_object(file.to_owned()).await) {
            Ok((head_object, _)) => match head_object.content_length {
                None => Err(S3Error::S3InternalError("Can't get S3 file length".into())),
                Some(size) => Ok(size.cast()),
            },
            Err(e) => Err(e),
        }
    }

    async fn list_file(&self, dir: &str) -> S3Result<Vec<String>> {
        resultify_anyhow!(
            self.bucket
                .list(
                    if dir.ends_with('/') {
                        dir.into()
                    } else {
                        format!("{dir}/")
                    },
                    Some("/".into()),
                )
                .await
        )
        .map(|list| {
            let mut result: Vec<String> = vec![];
            for lbr in &list {
                lbr.contents.iter().for_each(|c| {
                    result.push(c.key.clone());
                });
                if let Some(ref vec) = lbr.common_prefixes {
                    for cp in vec.iter() {
                        result.push(cp.prefix.clone());
                    }
                }
            }
            result
        })
    }

    async fn create_dir(&self, dir: &str) -> S3Result<()> {
        self.put_data(dir, b"", 0, 0).await
    }

    async fn get_last_modified(&self, file: &str) -> S3Result<SystemTime> {
        match resultify_anyhow!(self.bucket.head_object(file.to_owned()).await) {
            Ok((head_object, _)) => match head_object.last_modified {
                None => Err(S3Error::S3InternalError(
                    "Can't get S3 file last_modified time".into(),
                )),
                Some(ref lm) => Ok(chrono::DateTime::parse_from_str(lm, "%a, %e %b %Y %T %Z")
                    .unwrap_or_else(|e| {
                        panic!("failed to DateTime::parse_from_str {lm:?}, error is {e:?}")
                    })
                    .into()),
            },
            Err(e) => Err(e),
        }
    }

    async fn get_meta(&self, file: &str) -> S3Result<(usize, SystemTime)> {
        match resultify_anyhow!(self.bucket.head_object(file.to_owned()).await) {
            Ok((head_object, _)) => match head_object.last_modified {
                None => Err(S3Error::S3InternalError(
                    "Can't get S3 file last_modified time".into(),
                )),
                Some(ref lm) => match head_object.content_length {
                    None => Err(S3Error::S3InternalError("Can't get S3 file length".into())),
                    Some(size) => Ok((
                        size.cast(),
                        chrono::DateTime::parse_from_rfc2822(lm)
                            .unwrap_or_else(|e| {
                                panic!(
                                    "failed to DateTime::parse_from_rfc2822 {lm:?}, error is {e:?}"
                                )
                            })
                            .into(),
                    )),
                },
            },
            Err(e) => Err(e),
        }
    }

    async fn delete_data(&self, data: &str) -> S3Result<()> {
        resultify_anyhow!(self.bucket.delete_object(data).await).map(|_| ())
    }

    async fn put_data_vec(&self, file: &str, vec: Vec<IoMemBlock>) -> S3Result<()> {
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
        let request = RequestImpl::new(&self.bucket, file, command);
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

    async fn rename(&self, old_file: &str, new_file: &str) -> S3Result<()> {
        let _ret = resultify_anyhow!(
            self.bucket
                .copy_object_in_same_bucket(old_file, new_file)
                .await
        )?;

        resultify_anyhow!(self.bucket.delete_object(old_file).await).map(|_| ())
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

    async fn get_data(&self, _: &str) -> S3Result<Vec<u8>> {
        Ok(vec![])
    }

    async fn get_partial_data(&self, _: &str, _: usize, _: usize) -> S3Result<Vec<u8>> {
        Ok(vec![])
    }

    async fn put_data(&self, _: &str, _: &[u8], _: usize, _: usize) -> S3Result<()> {
        Ok(())
    }

    async fn get_len(&self, _: &str) -> S3Result<usize> {
        Ok(0)
    }

    async fn list_file(&self, _: &str) -> S3Result<Vec<String>> {
        Ok(vec![])
    }

    async fn create_dir(&self, _: &str) -> S3Result<()> {
        Ok(())
    }

    async fn get_last_modified(&self, _: &str) -> S3Result<SystemTime> {
        Ok(SystemTime::now())
    }

    async fn get_meta(&self, _: &str) -> S3Result<(usize, SystemTime)> {
        Ok((0, SystemTime::now()))
    }

    async fn delete_data(&self, _: &str) -> S3Result<()> {
        Ok(())
    }

    async fn put_data_vec(&self, _: &str, _: Vec<IoMemBlock>) -> S3Result<()> {
        Ok(())
    }

    async fn rename(&self, _: &str, _: &str) -> S3Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::{S3BackEnd, S3BackEndImpl};

    const TEST_BUCKET_NAME: &str = "s3-wrapper-test-bucket";
    const TEST_ENDPOINT: &str = "http://127.0.0.1:9000";
    const TEST_ACCESS_KEY: &str = "test";
    const TEST_SECRET_KEY: &str = "test1234";

    async fn create_backend() -> S3BackEndImpl {
        S3BackEndImpl::new_backend(
            TEST_BUCKET_NAME,
            TEST_ENDPOINT,
            TEST_ACCESS_KEY,
            TEST_SECRET_KEY,
        )
        .await
        .unwrap_or_else(|e| panic!("failed to create s3 backend, error is {e:?}"))
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn test_get_meta() {
        let s3_backend = create_backend().await;
        if let Err(e) = s3_backend.create_dir("test_dir").await {
            panic!("failed to create dir in s3 backend, error is {e:?}");
        }
        if let Err(e) = s3_backend.get_meta("test_dir").await {
            panic!("failed to get meta from s3 backend, error is {e:?}");
        }
    }
}
