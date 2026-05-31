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

    fn backend_as_str(self) -> &'static str {
        match self {
            Self::PodcastFile => "podcast_file_url",
            Self::Avatar => "avatar",
            Self::PodcastCover => "podcast_cover_url",
            Self::Playlists => "playlist",
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

#[derive(Debug, Clone, Serialize)]
struct BackendMediaUploadEvent {
    object_type: &'static str,
    object_id: String,
    event: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    audio_url_file: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    image_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    timestamp: DateTime<Utc>,
}

impl BackendMediaUploadEvent {
    fn start_upload(
        media_type: MediaObjectType,
        object_id: &str,
        timestamp: DateTime<Utc>,
    ) -> Self {
        Self {
            object_type: media_type.backend_as_str(),
            object_id: object_id.to_string(),
            event: "start_upload",
            audio_url_file: None,
            image_url: None,
            error: None,
            timestamp,
        }
    }

    fn uploaded(
        media_type: MediaObjectType,
        object_id: &str,
        url: &str,
        timestamp: DateTime<Utc>,
    ) -> Self {
        let (audio_url_file, image_url) = match media_type {
            MediaObjectType::PodcastFile => (Some(url.to_string()), None),
            _ => (None, Some(url.to_string())),
        };

        Self {
            object_type: media_type.backend_as_str(),
            object_id: object_id.to_string(),
            event: "uploaded",
            audio_url_file,
            image_url,
            error: None,
            timestamp,
        }
    }

    fn error(
        media_type: MediaObjectType,
        object_id: &str,
        error: &str,
        timestamp: DateTime<Utc>,
    ) -> Self {
        Self {
            object_type: media_type.backend_as_str(),
            object_id: object_id.to_string(),
            event: "error",
            audio_url_file: None,
            image_url: None,
            error: Some(error.to_string()),
            timestamp,
        }
    }
}

const TOPIC_MEDIA: &str = "media";
const TOPIC_MEDIA_UPLOAD: &str = "media.upload";

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
        let timestamp = Utc::now();
        let event = MediaEvent::StartUpload {
            media_type,
            object_id: object_id.to_string(),
            started_at: timestamp,
        };
        let backend_event = BackendMediaUploadEvent::start_upload(media_type, object_id, timestamp);

        let media_result = self
            .send_event(TOPIC_MEDIA, object_id, &event, "media.start_upload")
            .await;
        let backend_result = self
            .send_event(
                TOPIC_MEDIA_UPLOAD,
                object_id,
                &backend_event,
                "backend media.upload start_upload",
            )
            .await;
        media_result?;
        backend_result?;

        tracing::info!(
            "Published media.start_upload and backend media.upload start_upload (type={}, object_id={})",
            media_type.as_str(),
            object_id,
        );

        Ok(())
    }

    pub async fn send_uploaded(
        &self,
        media_type: MediaObjectType,
        object_id: &str,
        s3_url: &str,
        public_url: &str,
        size: usize,
        content_type: &str,
    ) -> Result<()> {
        let timestamp = Utc::now();
        let event = MediaEvent::Uploaded {
            media_type,
            object_id: object_id.to_string(),
            url: s3_url.to_string(),
            size,
            content_type: content_type.to_string(),
            uploaded_at: timestamp,
        };
        let backend_event =
            BackendMediaUploadEvent::uploaded(media_type, object_id, public_url, timestamp);

        let media_result = self
            .send_event(TOPIC_MEDIA, object_id, &event, "media.uploaded")
            .await;
        let backend_result = self
            .send_event(
                TOPIC_MEDIA_UPLOAD,
                object_id,
                &backend_event,
                "backend media.upload uploaded",
            )
            .await;
        media_result?;
        backend_result?;

        tracing::info!(
            "Published media.uploaded and backend media.upload uploaded (type={}, object_id={}, size={})",
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
        let timestamp = Utc::now();
        let event = MediaEvent::Error {
            media_type,
            object_id: object_id.to_string(),
            error_message: error_message.to_string(),
            timestamp,
        };
        let backend_event =
            BackendMediaUploadEvent::error(media_type, object_id, error_message, timestamp);

        let media_result = self
            .send_event(TOPIC_MEDIA, object_id, &event, "media.error")
            .await;
        let backend_result = self
            .send_event(
                TOPIC_MEDIA_UPLOAD,
                object_id,
                &backend_event,
                "backend media.upload error",
            )
            .await;
        media_result?;
        backend_result?;

        tracing::info!(
            "Published media.error and backend media.upload error (type={}, object_id={})",
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

    async fn send_event<T: Serialize>(
        &self,
        topic: &str,
        key: &str,
        event: &T,
        label: &str,
    ) -> Result<()> {
        let payload = serde_json::to_string(event)?;
        let record = FutureRecord::to(topic).key(key).payload(&payload);

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_podcast_upload_uses_audio_url_file() {
        let event = BackendMediaUploadEvent::uploaded(
            MediaObjectType::PodcastFile,
            "11111111-1111-4111-8111-111111111111",
            "https://s3.twcstorage.ru/bucket/source.mp3",
            Utc::now(),
        );
        let value = serde_json::to_value(event).unwrap();

        assert_eq!(value["object_type"], "podcast_file_url");
        assert_eq!(
            value["audio_url_file"],
            "https://s3.twcstorage.ru/bucket/source.mp3"
        );
        assert!(value.get("image_url").is_none());
    }

    #[test]
    fn backend_cover_upload_uses_image_url() {
        let event = BackendMediaUploadEvent::uploaded(
            MediaObjectType::PodcastCover,
            "11111111-1111-4111-8111-111111111111",
            "https://s3.twcstorage.ru/bucket/cover.webp",
            Utc::now(),
        );
        let value = serde_json::to_value(event).unwrap();

        assert_eq!(value["object_type"], "podcast_cover_url");
        assert_eq!(
            value["image_url"],
            "https://s3.twcstorage.ru/bucket/cover.webp"
        );
        assert!(value.get("audio_url_file").is_none());
    }
}
