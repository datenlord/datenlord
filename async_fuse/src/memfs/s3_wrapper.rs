use async_trait::async_trait;
use s3::{bucket::Bucket, bucket_ops::BucketConfiguration, creds::Credentials, Region};
use utilities::Cast;

pub enum S3Error {
    S3InternalError(String),
}

pub type S3Result<T> = Result<T, S3Error>;

#[async_trait]
pub trait S3Backend {
    async fn get_data(&self, file: &str) -> S3Result<Vec<u8>>;
    async fn get_partial_data(&self, file: &str, offset: usize, len: usize) -> S3Result<Vec<u8>>;
    async fn put_data(&self, file: &str, data: &[u8], offset: usize, len: usize) -> S3Result<()>;
    async fn get_len(&self, file: &str) -> S3Result<usize>;
    async fn list_file(&self, dir: &str) -> S3Result<Vec<String>>;
}

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

#[async_trait]
impl S3Backend for S3BackEndImpl {
    async fn get_data(&self, file: &str) -> S3Result<Vec<u8>> {
        match self.bucket.get_object(file.to_string()).await {
            Ok((data, _)) => Ok(data),
            Err(ref anyhow_error) => {
                Err(S3Error::S3InternalError(format_anyhow_error(anyhow_error)))
            }
        }
    }

    async fn get_partial_data(&self, file: &str, offset: usize, len: usize) -> S3Result<Vec<u8>> {
        match self
            .bucket
            .get_object_range(file.to_string(), offset.cast(), Some((offset + len).cast()))
            .await
        {
            Ok((data, _)) => Ok(data),
            Err(ref anyhow_error) => {
                Err(S3Error::S3InternalError(format_anyhow_error(anyhow_error)))
            }
        }
    }

    async fn put_data(&self, file: &str, data: &[u8], offset: usize, len: usize) -> S3Result<()> {
        match self
            .bucket
            .put_object(file.to_string(), &data[offset..(offset + len)])
            .await
        {
            Ok(_) => Ok(()),
            Err(ref anyhow_error) => {
                Err(S3Error::S3InternalError(format_anyhow_error(anyhow_error)))
            }
        }
    }
    async fn get_len(&self, file: &str) -> S3Result<usize> {
        match self.bucket.head_object(file.to_string()).await {
            Ok((head_object, _)) => match head_object.content_length {
                None => Err(S3Error::S3InternalError("Can't get S3 file length".into())),
                Some(size) => Ok(size.cast()),
            },
            Err(ref anyhow_error) => {
                Err(S3Error::S3InternalError(format_anyhow_error(anyhow_error)))
            }
        }
    }
    async fn list_file(&self, dir: &str) -> S3Result<Vec<String>> {
        match self
            .bucket
            .list(
                if dir.ends_with("/") {
                    dir.into()
                } else {
                    format!("{}/", dir)
                },
                Some("/".into()),
            )
            .await
        {
            Ok(list) => {
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
                Ok(result)
            }
            Err(ref anyhow_error) => {
                Err(S3Error::S3InternalError(format_anyhow_error(anyhow_error)))
            }
        }
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
