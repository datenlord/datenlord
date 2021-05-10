use super::cache::IoMemBlock;
use async_trait::async_trait;
use s3::{
    bucket::Bucket,
    bucket_ops::BucketConfiguration,
    command::{Command, Multipart},
    creds::Credentials,
    request_trait::Request,
    serde_types::{CompleteMultipartUploadData, InitiateMultipartUploadResponse, Part},
    surf_request::SurfRequest as RequestImpl,
    Region,
};
use serde_xml_rs as serde_xml;
use std::time::SystemTime;
use utilities::Cast;

#[derive(Debug)]
pub enum S3Error {
    S3InternalError(String),
}

impl Into<anyhow::Error> for S3Error {
    fn into(self) -> anyhow::Error {
        match self {
            S3Error::S3InternalError(msg) => anyhow::Error::msg(msg),
        }
    }
}

pub type S3Result<T> = Result<T, S3Error>;

#[async_trait]
pub trait S3BackEnd {
    async fn get_data(&self, file: &str) -> S3Result<Vec<u8>>;
    async fn get_partial_data(&self, file: &str, offset: usize, len: usize) -> S3Result<Vec<u8>>;
    async fn put_data(&self, file: &str, data: &[u8], offset: usize, len: usize) -> S3Result<()>;
    async fn put_data_vec(&self, file: &str, data: Vec<IoMemBlock>) -> S3Result<()>;
    async fn delete_data(&self, file: &str) -> S3Result<()>;
    async fn list_file(&self, dir: &str) -> S3Result<Vec<String>>;
    async fn create_dir(&self, dir: &str) -> S3Result<()>;
    async fn get_len(&self, file: &str) -> S3Result<usize>;
    async fn get_last_modified(&self, file: &str) -> S3Result<SystemTime>;
    async fn get_meta(&self, file: &str) -> S3Result<(usize, SystemTime)>;
    async fn rename(&self, old_file: &str, new_file: &str) -> S3Result<()>;
}

#[derive(Debug)]
pub struct S3BackEndImpl {
    bucket: Bucket,
}

impl S3BackEndImpl {
    #[allow(dead_code)]
    pub async fn new(
        bucket_name: &str,
        endpoint: &str,
        access_key: &str,
        secret_key: &str,
    ) -> S3Result<Self> {
        let region = Region::Custom {
            region: "fake_region".into(),
            endpoint: endpoint.into(),
        };
        let credentials =
            Credentials::new(Some(access_key), Some(secret_key), None, None, None).unwrap();
        let config = BucketConfiguration::default();
        let bucket = Bucket::create_with_path_style(bucket_name, region, credentials, config)
            .await
            .unwrap();

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
}

macro_rules! resultify_anyhow {
    ($e: expr) => {
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
    async fn get_data(&self, file: &str) -> S3Result<Vec<u8>> {
        resultify_anyhow!(self.bucket.get_object(file.to_string()).await).map(|(data, _)| data)
    }

    async fn get_partial_data(&self, file: &str, offset: usize, len: usize) -> S3Result<Vec<u8>> {
        resultify_anyhow!(
            self.bucket
                .get_object_range(file.to_string(), offset.cast(), Some((offset + len).cast()))
                .await
        )
        .map(|(data, _)| data)
    }

    async fn put_data(&self, file: &str, data: &[u8], offset: usize, len: usize) -> S3Result<()> {
        resultify_anyhow!(
            self.bucket
                .put_object(file.to_string(), &data[offset..(offset + len)])
                .await
        )
        .map(|_| ())
    }
    async fn get_len(&self, file: &str) -> S3Result<usize> {
        match resultify_anyhow!(self.bucket.head_object(file.to_string()).await) {
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
                    if dir.ends_with("/") {
                        dir.into()
                    } else {
                        format!("{}/", dir)
                    },
                    Some("/".into()),
                )
                .await
        )
        .map(|list| {
            let mut result: Vec<String> = vec![];
            list.iter().for_each(|lbr| {
                lbr.contents.iter().for_each(|c| {
                    result.push(c.key.to_owned());
                });
                if let Some(ref vec) = lbr.common_prefixes {
                    vec.iter().for_each(|cp| {
                        result.push(cp.prefix.to_owned());
                    });
                }
            });
            result
        })
    }

    async fn create_dir(&self, dir: &str) -> S3Result<()> {
        self.put_data(dir, "".as_bytes(), 0, 0).await
    }

    async fn get_last_modified(&self, file: &str) -> S3Result<SystemTime> {
        match resultify_anyhow!(self.bucket.head_object(file.to_string()).await) {
            Ok((head_object, _)) => match head_object.last_modified {
                None => Err(S3Error::S3InternalError(
                    "Can't get S3 file last_modified time".into(),
                )),
                Some(ref lm) => Ok(chrono::DateTime::parse_from_str(lm, "%a, %e %b %Y %T %Z")
                    .unwrap()
                    .into()),
            },
            Err(e) => Err(e),
        }
    }

    async fn get_meta(&self, file: &str) -> S3Result<(usize, SystemTime)> {
        match resultify_anyhow!(self.bucket.head_object(file.to_string()).await) {
            Ok((head_object, _)) => match head_object.last_modified {
                None => Err(S3Error::S3InternalError(
                    "Can't get S3 file last_modified time".into(),
                )),
                Some(ref lm) => match head_object.content_length {
                    None => Err(S3Error::S3InternalError("Can't get S3 file length".into())),
                    Some(size) => Ok((
                        size.cast(),
                        chrono::DateTime::parse_from_rfc2822(lm).unwrap().into(),
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
        if vec.len() == 0 {
            return Ok(());
        }

        if vec.len() == 1 {
            let buf = unsafe { vec.get(0).unwrap().as_slice() };
            return self.put_data(file, buf, 0, buf.len()).await;
        }

        let command = Command::InitiateMultipartUpload;
        let request = RequestImpl::new(&self.bucket, file, command);
        let (data, _) = resultify_anyhow!(request.response_data(false).await)?;
        let msg: InitiateMultipartUploadResponse = serde_xml::from_str(
            std::str::from_utf8(data.as_slice())
                .map_err(|e| S3Error::S3InternalError(format!("{}", e)))?,
        )
        .map_err(|e| S3Error::S3InternalError(format!("{}", e)))?;
        let path = msg.key;
        let upload_id = &msg.upload_id;

        let mut part_number: u32 = 0;
        let mut etags = Vec::new();

        let last_index = vec.len() - 1;
        for (index, d) in vec.iter().enumerate() {
            part_number += 1;
            let command = Command::PutObject {
                content: unsafe { d.as_slice() },
                content_type: "application/octet-stream",
                multipart: Some(Multipart::new(part_number, upload_id)), // upload_id: &msg.upload_id,
            };
            let request = RequestImpl::new(&self.bucket, &path, command);
            let (data, _code) = resultify_anyhow!(request.response_data(true).await)?;
            let etag = std::str::from_utf8(data.as_slice())
                .map_err(|e| S3Error::S3InternalError(format!("{}", e)))?;

            etags.push(etag.to_string());

            if index == last_index {
                let inner_data = etags
                    .iter()
                    .enumerate()
                    .map(|(i, x)| Part {
                        etag: x.to_owned(),
                        part_number: i as u32 + 1,
                    })
                    .collect::<Vec<Part>>();
                let data = CompleteMultipartUploadData { parts: inner_data };
                let complete = Command::CompleteMultipartUpload {
                    upload_id: &msg.upload_id,
                    data,
                };
                let complete_request = RequestImpl::new(&self.bucket, &path, complete);
                let (_data, _code) =
                    resultify_anyhow!(complete_request.response_data(false).await)?;
            }
        }
        Ok(())
    }

    async fn rename(&self, old_file: &str, new_file: &str) -> S3Result<()> {
        let _ = resultify_anyhow!(
            self.bucket
                .copy_object_in_same_bucket(old_file, new_file)
                .await
        )?;

        resultify_anyhow!(self.bucket.delete_object(old_file).await).map(|_| ())
    }
}

#[must_use]
pub fn format_anyhow_error(error: &anyhow::Error) -> String {
    let err_msg_vec = error
        .chain()
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>();
    let mut err_msg = err_msg_vec.as_slice().join(", caused by: ");
    err_msg.push_str(&format!(", root cause: {}", error.root_cause()));
    err_msg
}

#[cfg(test)]
mod test {
    use super::{S3BackEnd, S3BackEndImpl};

    const TEST_BUCKET_NAME: &str = "s3-wrapper-test-bucket";
    const TEST_ENDPOINT: &str = "http://127.0.0.1:9000";
    const TEST_ACCESS_KEY: &str = "test";
    const TEST_SECRET_KEY: &str = "test1234";

    async fn create_backend() -> S3BackEndImpl {
        S3BackEndImpl::new(
            TEST_BUCKET_NAME,
            TEST_ENDPOINT,
            TEST_ACCESS_KEY,
            TEST_SECRET_KEY,
        )
        .await
        .unwrap()
    }

    #[test]
    fn test_get_meta() {
        smol::block_on(async {
            let s3_backend = create_backend().await;
            let _ = s3_backend.create_dir("test_dir").await;
            let _ = s3_backend.get_meta("test_dir").await;
        });
    }
}
