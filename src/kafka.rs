use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MediaObjectType {
    PodcastFile,
    Avatar,
    PodcastCover,
    Playlists,
}

impl MediaObjectType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PodcastFile => "podcast_file",
            Self::Avatar => "avatar",
            Self::PodcastCover => "podcast_cover",
            Self::Playlists => "playlists",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum MediaEvent {
    StartUpload {
        #[serde(rename = "type")]
        media_type: MediaObjectType,
        object_id: String,
        started_at: DateTime<Utc>,
    },
    Uploaded {
        #[serde(rename = "type")]
        media_type: MediaObjectType,
        object_id: String,
        url: String,
        size: usize,
        content_type: String,
        uploaded_at: DateTime<Utc>,
    },
    Error {
        #[serde(rename = "type")]
        media_type: MediaObjectType,
        object_id: String,
        error_message: String,
        timestamp: DateTime<Utc>,
    },
}

const TOPIC: &str = "media";

pub struct KafkaProducer {
    producer: FutureProducer,
}

impl KafkaProducer {
    pub fn new(brokers: &str) -> Result<Self> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .set("message.send.max.retries", "10")
            .set("retry.backoff.ms", "500")
            .set("reconnect.backoff.ms", "500")
            .set("reconnect.backoff.max.ms", "10000")
            .set("socket.keepalive.enable", "true")
            .create::<FutureProducer>()
            .context("Failed to create Kafka producer")?;

        Ok(Self { producer })
    }

    pub async fn send_start_upload(
        &self,
        media_type: MediaObjectType,
        object_id: &str,
    ) -> Result<()> {
        let event = MediaEvent::StartUpload {
            media_type,
            object_id: object_id.to_string(),
            started_at: Utc::now(),
        };

        self.send_event(object_id, &event, "media.start_upload")
            .await?;

        tracing::info!(
            "Published media.start_upload (type={}, object_id={})",
            media_type.as_str(),
            object_id,
        );

        Ok(())
    }

    pub async fn send_uploaded(
        &self,
        media_type: MediaObjectType,
        object_id: &str,
        url: &str,
        size: usize,
        content_type: &str,
    ) -> Result<()> {
        let event = MediaEvent::Uploaded {
            media_type,
            object_id: object_id.to_string(),
            url: url.to_string(),
            size,
            content_type: content_type.to_string(),
            uploaded_at: Utc::now(),
        };

        self.send_event(object_id, &event, "media.uploaded").await?;

        tracing::info!(
            "Published media.uploaded (type={}, object_id={}, size={})",
            media_type.as_str(),
            object_id,
            size,
        );

        Ok(())
    }

    pub async fn send_error(
        &self,
        media_type: MediaObjectType,
        object_id: &str,
        error_message: &str,
    ) -> Result<()> {
        let event = MediaEvent::Error {
            media_type,
            object_id: object_id.to_string(),
            error_message: error_message.to_string(),
            timestamp: Utc::now(),
        };

        self.send_event(object_id, &event, "media.error").await?;

        tracing::info!(
            "Published media.error (type={}, object_id={})",
            media_type.as_str(),
            object_id,
        );

        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        self.producer
            .flush(Duration::from_secs(10))
            .context("Failed to flush Kafka producer")?;
        Ok(())
    }

    async fn send_event(&self, key: &str, event: &MediaEvent, label: &str) -> Result<()> {
        let payload = serde_json::to_string(event)?;
        let record = FutureRecord::to(TOPIC).key(key).payload(&payload);

        self.producer
            .send(record, Duration::from_secs(30))
            .await
            .map_err(|(err, _msg)| anyhow::anyhow!("Failed to send {label}: {err}"))?;

        Ok(())
    }
}

pub type SharedKafkaProducer = Arc<KafkaProducer>;

pub fn new_producer(brokers: &str) -> Result<SharedKafkaProducer> {
    let producer = KafkaProducer::new(brokers)?;
    Ok(Arc::new(producer))
}
