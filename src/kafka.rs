use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum MediaEvent {
    StartUpload {
        file_id: String,
        author_id: String,
        filename: String,
        started_at: DateTime<Utc>,
    },
    Uploaded {
        file_id: String,
        author_id: String,
        size_bytes: usize,
        original_format: String,
        temp_path: String,
        uploaded_at: DateTime<Utc>,
    },
    Error {
        file_id: String,
        stage: String,
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
        file_id: Uuid,
        author_id: &str,
        filename: &str,
    ) -> Result<()> {
        let file_id_key = file_id.to_string();
        let event = MediaEvent::StartUpload {
            file_id: file_id_key.clone(),
            author_id: author_id.to_string(),
            filename: filename.to_string(),
            started_at: Utc::now(),
        };

        let payload = serde_json::to_string(&event)?;
        let record = FutureRecord::to(TOPIC).key(&file_id_key).payload(&payload);

        self.producer
            .send(record, Duration::from_secs(30))
            .await
            .map_err(|(err, _msg)| anyhow::anyhow!("Failed to send media.start_upload: {}", err))?;

        tracing::info!(
            "Published media.start_upload (file_id={}, author_id={})",
            file_id,
            author_id,
        );

        Ok(())
    }

    pub async fn send_uploaded(
        &self,
        file_id: Uuid,
        author_id: &str,
        size_bytes: usize,
        original_format: &str,
        temp_path: &str,
    ) -> Result<()> {
        let file_id_key = file_id.to_string();
        let event = MediaEvent::Uploaded {
            file_id: file_id_key.clone(),
            author_id: author_id.to_string(),
            size_bytes,
            original_format: original_format.to_string(),
            temp_path: temp_path.to_string(),
            uploaded_at: Utc::now(),
        };

        let payload = serde_json::to_string(&event)?;
        let record = FutureRecord::to(TOPIC).key(&file_id_key).payload(&payload);

        self.producer
            .send(record, Duration::from_secs(30))
            .await
            .map_err(|(err, _msg)| anyhow::anyhow!("Failed to send media.uploaded: {}", err))?;

        tracing::info!(
            "Published media.uploaded (file_id={}, size={})",
            file_id,
            size_bytes,
        );

        Ok(())
    }

    pub async fn send_error(&self, file_id: Uuid, stage: &str, error_message: &str) -> Result<()> {
        let file_id_key = file_id.to_string();
        let event = MediaEvent::Error {
            file_id: file_id_key.clone(),
            stage: stage.to_string(),
            error_message: error_message.to_string(),
            timestamp: Utc::now(),
        };

        let payload = serde_json::to_string(&event)?;
        let record = FutureRecord::to(TOPIC).key(&file_id_key).payload(&payload);

        self.producer
            .send(record, Duration::from_secs(30))
            .await
            .map_err(|(err, _msg)| anyhow::anyhow!("Failed to send media.error: {}", err))?;

        tracing::info!(
            "Published media.error (file_id={}, stage={})",
            file_id,
            stage,
        );

        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        self.producer
            .flush(Duration::from_secs(10))
            .context("Failed to flush Kafka producer")?;
        Ok(())
    }
}

pub type SharedKafkaProducer = Arc<KafkaProducer>;

pub fn new_producer(brokers: &str) -> Result<SharedKafkaProducer> {
    let producer = KafkaProducer::new(brokers)?;
    Ok(Arc::new(producer))
}
