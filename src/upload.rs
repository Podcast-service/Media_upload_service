use axum::{
    extract::{Multipart, State},
    http::StatusCode,
    Json,
};
use serde::Serialize;
use tokio::io::{AsyncWriteExt, BufWriter};
use uuid::Uuid;
use utoipa::ToSchema;

use crate::auth::AuthUser;
use crate::kafka::SharedKafkaProducer;
use crate::validation;
use tracing::info;

const MAX_FILE_SIZE: usize = validation::MAX_FILE_SIZE;

#[derive(Clone)]
pub struct AppState {
    pub kafka: SharedKafkaProducer,
    pub jwt_secret: String,
    pub temp_dir: String,
}


#[derive(Serialize, ToSchema)]
pub struct UploadResponse {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_format: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temp_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(ToSchema)]
pub struct UploadRequest {
    #[schema(format = "binary")]
    pub audio: String,
}


fn error_response(status: StatusCode, error: String) -> (StatusCode, Json<UploadResponse>) {
    tracing::warn!("Upload error: {}", error);
    (
        status,
        Json(UploadResponse {
            success: false,
            file_id: None,
            filename: None,
            size_bytes: None,
            original_format: None,
            temp_path: None,
            message: None,
            error: Some(error),
        }),
    )
}


#[utoipa::path(
    post,
    path = "/api/media/upload",
    request_body(
        content = UploadRequest,
        content_type = "multipart/form-data",
        description = "Multipart form with audio file"
    ),
    responses(
        (status = 200, description = "Upload accepted", body = UploadResponse),
        (status = 400, description = "Bad request", body = UploadResponse),
        (status = 401, description = "Unauthorized"),
        (status = 413, description = "Payload too large", body = UploadResponse),
        (status = 415, description = "Unsupported media type", body = UploadResponse)
    ),
    security(
        ("bearer_auth" = [])
    ),
    tag = "media"
)]
pub async fn upload_media(
    State(state): State<AppState>,
    auth: AuthUser,
    mut multipart: Multipart,
) -> (StatusCode, Json<UploadResponse>) {
    let author_id = auth.0.sub.clone();
    let kafka = &state.kafka;
    let file_id = Uuid::new_v4();

    let mut field = match multipart.next_field().await {
        Ok(Some(field)) => field,
        Ok(None) => {
            return error_response(StatusCode::BAD_REQUEST, "Файл не найден в запросе".into());
        }
        Err(e) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                format!("Ошибка чтения multipart: {}", e),
            );
        }
    };

    let filename = field.file_name().unwrap_or("unknown").to_string();

    let extension = match validation::validate_extension(&filename) {
        Ok(ext) => ext,
        Err(e) => {
            send_error_event(kafka, file_id, "validation", &e);
            return error_response(StatusCode::UNSUPPORTED_MEDIA_TYPE, e);
        }
    };

    if let Err(e) = kafka.send_start_upload(file_id, &author_id, &filename).await {
        tracing::warn!("Failed to publish media.start_upload: {}", e);
    }

    let temp_path = format!("{}/{}.{}", state.temp_dir, file_id, extension);
    let file = match tokio::fs::File::create(&temp_path).await {
        Ok(f) => f,
        Err(e) => {
            let msg = format!("Не удалось создать временный файл: {}", e);
            send_error_event(kafka, file_id, "upload", &msg);
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, msg);
        }
    };
    let mut writer = BufWriter::new(file);

    let mut total_bytes: usize = 0;
    let mut magic_checked = false;
    let mut head_buf = Vec::new();

    info!(
        "Start receiving file '{}' (ext={}, file_id={}, author_id={})",
        filename, extension, file_id, author_id
    );

    loop {
        let chunk = match field.chunk().await {
            Ok(Some(c)) => c,
            Ok(None) => break,
            Err(e) => {
                let msg = format!("Ошибка чтения данных файла: {}", e);
                send_error_event(kafka, file_id, "upload", &msg);
                cleanup_temp(&temp_path).await;
                return error_response(StatusCode::BAD_REQUEST, msg);
            }
        };

        total_bytes += chunk.len();

        if total_bytes > MAX_FILE_SIZE {
            let msg = format!(
                "Файл слишком большой: {} MB (максимум: {} MB)",
                total_bytes / (1024 * 1024),
                MAX_FILE_SIZE / (1024 * 1024)
            );
            send_error_event(kafka, file_id, "validation", &msg);
            cleanup_temp(&temp_path).await;
            return error_response(StatusCode::PAYLOAD_TOO_LARGE, msg);
        }

        if !magic_checked {
            head_buf.extend_from_slice(&chunk);
            if head_buf.len() >= 12 {
                let detected_format = match validation::validate_magic_bytes(&head_buf) {
                    Ok(fmt) => fmt,
                    Err(e) => {
                        send_error_event(kafka, file_id, "validation", &e);
                        cleanup_temp(&temp_path).await;
                        return error_response(StatusCode::UNSUPPORTED_MEDIA_TYPE, e);
                    }
                };
                if let Err(e) =
                    validation::check_extension_magic_compatibility(&extension, detected_format)
                {
                    send_error_event(kafka, file_id, "validation", &e);
                    cleanup_temp(&temp_path).await;
                    return error_response(StatusCode::BAD_REQUEST, e);
                }
                info!(
                    "File '{}' passed magic bytes check: ext={}, magic={}",
                    filename, extension, detected_format
                );
                magic_checked = true;
                head_buf = Vec::new();
            }
        }

        if let Err(e) = writer.write_all(&chunk).await {
            let msg = format!("Ошибка записи во временный файл: {}", e);
            send_error_event(kafka, file_id, "upload", &msg);
            cleanup_temp(&temp_path).await;
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, msg);
        }
    }

    if let Err(e) = writer.flush().await {
        let msg = format!("Ошибка сброса буфера: {}", e);
        send_error_event(kafka, file_id, "upload", &msg);
        cleanup_temp(&temp_path).await;
        return error_response(StatusCode::INTERNAL_SERVER_ERROR, msg);
    }

    if total_bytes == 0 {
        send_error_event(kafka, file_id, "validation", "Файл пустой");
        cleanup_temp(&temp_path).await;
        return error_response(StatusCode::BAD_REQUEST, "Файл пустой".into());
    }

    if !magic_checked {
        let msg = "Файл слишком мал для определения формата";
        send_error_event(kafka, file_id, "validation", msg);
        cleanup_temp(&temp_path).await;
        return error_response(StatusCode::BAD_REQUEST, msg.into());
    }

    info!(
        "File '{}' saved: {} bytes -> {}",
        filename, total_bytes, temp_path
    );

    let original_format = validation::mime_from_extension(&extension).to_string();

    if let Err(e) = kafka
        .send_uploaded(file_id, &author_id, total_bytes, &original_format, &temp_path)
        .await
    {
        tracing::warn!("Failed to publish media.uploaded: {}", e);
    }

    (
        StatusCode::OK,
        Json(UploadResponse {
            success: true,
            file_id: Some(file_id.to_string()),
            filename: Some(filename),
            size_bytes: Some(total_bytes),
            original_format: Some(original_format),
            temp_path: Some(temp_path),
            message: Some("Файл загружен и отправлен на обработку".into()),
            error: None,
        }),
    )
}


fn send_error_event(kafka: &SharedKafkaProducer, file_id: Uuid, stage: &str, msg: &str) {
    let kafka = kafka.clone();
    let stage = stage.to_string();
    let msg = msg.to_string();
    tokio::spawn(async move {
        if let Err(e) = kafka.send_error(file_id, &stage, &msg).await {
            tracing::warn!("Failed to publish media.error: {}", e);
        }
    });
}

async fn cleanup_temp(path: &str) {
    if let Err(e) = tokio::fs::remove_file(path).await {
        tracing::debug!("Failed to cleanup temp file {}: {}", path, e);
    }
}
