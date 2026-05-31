use std::env;

use anyhow::{Context, Result};
use aws_config::{BehaviorVersion, Region};
use aws_credential_types::Credentials;
use aws_sdk_s3::{error::ProvideErrorMetadata, primitives::ByteStream, Client};
use tracing::{error, info, warn};

const DEFAULT_S3_REGION: &str = "ru-1";
const DEFAULT_S3_ENDPOINT_URL: &str = "https://s3.twcstorage.ru";

pub struct Config {
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub endpoint_url: String,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            region: env_value("S3_REGION").unwrap_or_else(|| DEFAULT_S3_REGION.to_string()),
            access_key_id: env_value("S3_ACCESS_KEY_ID").context("S3_ACCESS_KEY_ID is required")?,
            secret_access_key: env_value("S3_SECRET_ACCESS_KEY")
                .context("S3_SECRET_ACCESS_KEY is required")?,
            endpoint_url: env_value("S3_ENDPOINT_URL")
                .unwrap_or_else(|| DEFAULT_S3_ENDPOINT_URL.to_string()),
        })
    }
}

#[derive(Clone)]
pub struct S3Client {
    client: Client,
}

impl S3Client {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    pub async fn ensure_bucket(&self, bucket: &str) -> Result<()> {
        match self.client.head_bucket().bucket(bucket).send().await {
            Ok(_) => {
                info!("Bucket '{}' is available", bucket);
                return Ok(());
            }
            Err(err) if !should_create_bucket() => {
                return Err(err).with_context(|| {
                    format!(
                        "bucket '{bucket}' is not available; check S3_BUCKET and credentials, or set S3_CREATE_BUCKET=true for local S3-compatible storage"
                    )
                });
            }
            Err(err) => {
                warn!(
                    "Bucket '{}' is not available, trying to create it: code={:?}, message={:?}",
                    bucket,
                    err.code(),
                    err.message()
                );
            }
        }

        match self.client.create_bucket().bucket(bucket).send().await {
            Ok(_) => info!("Bucket '{}' created successfully", bucket),
            Err(err) if err.code() == Some("BucketAlreadyOwnedByYou") => {
                info!("Bucket '{}' already exists, skip create", bucket);
            }
            Err(err) => {
                error!(
                    "create_bucket error: code={:?}, message={:?}, raw={:?}",
                    err.code(),
                    err.message(),
                    err
                );
                return Err(err).with_context(|| format!("create_bucket failed for {bucket}"));
            }
        }

        Ok(())
    }

    pub async fn upload_bytes(
        &self,
        bucket: &str,
        object_key: &str,
        bytes: Vec<u8>,
        content_type: &str,
    ) -> Result<()> {
        let size_bytes = bytes.len();
        self.client
            .put_object()
            .bucket(bucket)
            .key(object_key)
            .content_type(content_type)
            .body(ByteStream::from(bytes))
            .send()
            .await
            .with_context(|| format!("error uploading '{object_key}' to bucket '{bucket}'"))?;

        info!(
            "uploaded object: bucket='{}', object='{}', bytes={}",
            bucket, object_key, size_bytes
        );

        Ok(())
    }

    pub async fn delete_object(&self, bucket: &str, object_key: &str) -> Result<()> {
        self.client
            .delete_object()
            .bucket(bucket)
            .key(object_key)
            .send()
            .await
            .with_context(|| {
                format!("error deleting object '{object_key}' from bucket '{bucket}'")
            })?;

        info!("deleted object '{object_key}' from bucket '{bucket}'");
        Ok(())
    }
}

pub async fn create_client(cfg: &Config) -> Result<S3Client> {
    let credentials = Credentials::new(
        cfg.access_key_id.clone(),
        cfg.secret_access_key.clone(),
        None,
        None,
        "s3-compatible",
    );

    let shared_config = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new(cfg.region.clone()))
        .credentials_provider(credentials)
        .endpoint_url(cfg.endpoint_url.clone())
        .load()
        .await;

    let s3_config = aws_sdk_s3::config::Builder::from(&shared_config)
        .force_path_style(true)
        .build();

    Ok(S3Client::new(Client::from_conf(s3_config)))
}

pub fn s3_url(bucket: &str, object_key: &str) -> String {
    format!("s3://{}/{}", bucket, object_key)
}

pub fn public_url(endpoint_url: &str, bucket: &str, object_key: &str) -> String {
    format!(
        "{}/{}/{}",
        endpoint_url.trim_end_matches('/'),
        bucket,
        object_key
    )
}

fn env_value(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn should_create_bucket() -> bool {
    env::var("S3_CREATE_BUCKET")
        .map(|value| value.eq_ignore_ascii_case("true") || value == "1")
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::public_url;

    #[test]
    fn public_url_uses_endpoint_without_duplicate_separator() {
        assert_eq!(
            public_url(
                "https://s3.twcstorage.ru/",
                "bucket",
                "media/uploads/avatar/file.jpeg"
            ),
            "https://s3.twcstorage.ru/bucket/media/uploads/avatar/file.jpeg"
        );
    }
}
