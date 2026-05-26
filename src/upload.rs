use std::sync::Arc;

use axum::{
    extract::{multipart::Field, Multipart, State},
    http::StatusCode,
    Json,
};
use serde::Serialize;
use tracing::info;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::auth::AuthUser;
use crate::kafka::{MediaObjectType, SharedKafkaProducer};
use crate::s3::{self, S3Client};
use crate::validation::{self, FileKind};

const MAX_FILE_SIZE: usize = validation::MAX_FILE_SIZE;
const MIN_MAGIC_BYTES: usize = 12;

#[derive(Clone)]
pub struct AppState {
    pub kafka: SharedKafkaProducer,
    pub s3: Arc<S3Client>,
    pub jwt_secret: String,
    pub audio_bucket: String,
}

#[derive(Serialize, ToSchema)]
pub struct UploadResponse {
    pub success: bool,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(ToSchema)]
#[allow(dead_code)]
pub struct UploadAudioRequest {
    pub id_podcast: String,
    #[schema(format = "binary")]
    pub audio: String,
}

#[derive(ToSchema)]
#[allow(dead_code)]
pub struct UploadProfileCoverRequest {
    #[schema(format = "binary")]
    pub image: String,
}

#[derive(ToSchema)]
#[allow(dead_code)]
pub struct UploadPodcastCoverRequest {
    pub id_podcast: String,
    #[schema(format = "binary")]
    pub image: String,
}

#[derive(ToSchema)]
#[allow(dead_code)]
pub struct UploadPlaylistCoverRequest {
    pub id_playlist: String,
    #[schema(format = "binary")]
    pub image: String,
}

#[derive(Clone, Copy)]
struct UploadConfig {
    media_type: MediaObjectType,
    file_kind: FileKind,
    object_id_source: ObjectIdSource,
    file_field: &'static str,
    message: &'static str,
}

#[derive(Clone, Copy)]
enum ObjectIdSource {
    JwtSubject,
    MultipartField(&'static str),
}

struct PreparedFile {
    filename: String,
    extension: String,
    size: usize,
    content_type: String,
    bytes: Vec<u8>,
}

struct ParsedUpload {
    object_id: String,
    file: PreparedFile,
}

#[utoipa::path(
    post,
    path = "/api/media/upload_audio",
    request_body(
        content = UploadAudioRequest,
        content_type = "multipart/form-data",
        description = "Multipart form with id_podcast string and audio file"
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
pub async fn upload_audio(
    State(state): State<AppState>,
    auth: AuthUser,
    multipart: Multipart,
) -> (StatusCode, Json<UploadResponse>) {
    handle_upload(
        state,
        auth,
        multipart,
        UploadConfig {
            media_type: MediaObjectType::PodcastFile,
            file_kind: FileKind::Audio,
            object_id_source: ObjectIdSource::MultipartField("id_podcast"),
            file_field: "audio",
            message: "Аудиофайл сохранен в S3 и отправлен на обработку",
        },
    )
    .await
}

#[utoipa::path(
    post,
    path = "/api/media/upload_cover_profile",
    request_body(
        content = UploadProfileCoverRequest,
        content_type = "multipart/form-data",
        description = "Multipart form with image file. object_id is taken from JWT sub"
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
pub async fn upload_cover_profile(
    State(state): State<AppState>,
    auth: AuthUser,
    multipart: Multipart,
) -> (StatusCode, Json<UploadResponse>) {
    handle_upload(
        state,
        auth,
        multipart,
        UploadConfig {
            media_type: MediaObjectType::Avatar,
            file_kind: FileKind::Image,
            object_id_source: ObjectIdSource::JwtSubject,
            file_field: "image",
            message: "Изображение профиля сохранено в S3",
        },
    )
    .await
}

#[utoipa::path(
    post,
    path = "/api/media/upload_cover_podcast",
    request_body(
        content = UploadPodcastCoverRequest,
        content_type = "multipart/form-data",
        description = "Multipart form with id_podcast string and image file"
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
pub async fn upload_cover_podcast(
    State(state): State<AppState>,
    auth: AuthUser,
    multipart: Multipart,
) -> (StatusCode, Json<UploadResponse>) {
    handle_upload(
        state,
        auth,
        multipart,
        UploadConfig {
            media_type: MediaObjectType::PodcastCover,
            file_kind: FileKind::Image,
            object_id_source: ObjectIdSource::MultipartField("id_podcast"),
            file_field: "image",
            message: "Обложка подкаста сохранена в S3",
        },
    )
    .await
}

#[utoipa::path(
    post,
    path = "/api/media/upload_cover_playlist",
    request_body(
        content = UploadPlaylistCoverRequest,
        content_type = "multipart/form-data",
        description = "Multipart form with id_playlist string and image file"
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
pub async fn upload_cover_playlist(
    State(state): State<AppState>,
    auth: AuthUser,
    multipart: Multipart,
) -> (StatusCode, Json<UploadResponse>) {
    handle_upload(
        state,
        auth,
        multipart,
        UploadConfig {
            media_type: MediaObjectType::Playlists,
            file_kind: FileKind::Image,
            object_id_source: ObjectIdSource::MultipartField("id_playlist"),
            file_field: "image",
            message: "Обложка плейлиста сохранена в S3",
        },
    )
    .await
}

async fn handle_upload(
    state: AppState,
    auth: AuthUser,
    multipart: Multipart,
    config: UploadConfig,
) -> (StatusCode, Json<UploadResponse>) {
    let parsed = match parse_upload(&state.kafka, auth, multipart, config).await {
        Ok(value) => value,
        Err(response) => return response,
    };

    if let Err(e) = state
        .kafka
        .send_start_upload(config.media_type, &parsed.object_id)
        .await
    {
        tracing::warn!("Failed to publish media.start_upload: {}", e);
    }

    let upload_id = Uuid::new_v4();
    let object_key = format!(
        "media/uploads/{}/{}/{}.{}",
        config.media_type.as_str(),
        parsed.object_id,
        upload_id,
        parsed.file.extension
    );

    if let Err(e) = state
        .s3
        .upload_bytes(
            &state.audio_bucket,
            &object_key,
            parsed.file.bytes,
            &parsed.file.content_type,
        )
        .await
    {
        let msg = format!("Не удалось сохранить файл в S3: {}", e);
        send_error_event(&state.kafka, config.media_type, &parsed.object_id, &msg);
        return error_response(StatusCode::INTERNAL_SERVER_ERROR, msg);
    }

    let url = s3::s3_url(&state.audio_bucket, &object_key);

    if let Err(e) = state
        .kafka
        .send_uploaded(
            config.media_type,
            &parsed.object_id,
            &url,
            parsed.file.size,
            &parsed.file.content_type,
        )
        .await
    {
        tracing::warn!("Failed to publish media.uploaded: {}", e);
    }

    (
        StatusCode::OK,
        Json(UploadResponse {
            success: true,
            media_type: Some(config.media_type.as_str().to_string()),
            object_id: Some(parsed.object_id),
            url: Some(url),
            size: Some(parsed.file.size),
            content_type: Some(parsed.file.content_type),
            filename: Some(parsed.file.filename),
            message: Some(config.message.into()),
            error: None,
        }),
    )
}

async fn parse_upload(
    kafka: &SharedKafkaProducer,
    auth: AuthUser,
    mut multipart: Multipart,
    config: UploadConfig,
) -> Result<ParsedUpload, (StatusCode, Json<UploadResponse>)> {
    let mut object_id = match config.object_id_source {
        ObjectIdSource::JwtSubject => {
            let subject = validate_uuid_value(&auth.0.sub, "JWT sub")?;
            Some(subject)
        }
        ObjectIdSource::MultipartField(_) => None,
    };
    let mut file: Option<PreparedFile> = None;

    loop {
        let field = match multipart.next_field().await {
            Ok(Some(field)) => field,
            Ok(None) => break,
            Err(e) => {
                let msg = format!("Ошибка чтения multipart: {}", e);
                return Err(error_with_optional_event(
                    kafka,
                    config.media_type,
                    object_id.as_deref(),
                    StatusCode::BAD_REQUEST,
                    msg,
                ));
            }
        };

        let field_name = field.name().unwrap_or_default().to_string();
        if matches_field(config.object_id_source, &field_name) {
            if object_id.is_some() {
                let msg = format!("Поле {} передано несколько раз", field_name);
                return Err(error_with_optional_event(
                    kafka,
                    config.media_type,
                    object_id.as_deref(),
                    StatusCode::BAD_REQUEST,
                    msg,
                ));
            }

            let value = match read_text_field(field, &field_name).await {
                Ok(value) => value,
                Err((status, msg)) => {
                    return Err(error_with_optional_event(
                        kafka,
                        config.media_type,
                        object_id.as_deref(),
                        status,
                        msg,
                    ));
                }
            };

            let value = match validate_uuid_value(&value, &field_name) {
                Ok(value) => value,
                Err(response) => return Err(response),
            };
            object_id = Some(value);
        } else if field_name == config.file_field {
            if file.is_some() {
                let msg = format!("Поле {} передано несколько раз", config.file_field);
                return Err(error_with_optional_event(
                    kafka,
                    config.media_type,
                    object_id.as_deref(),
                    StatusCode::BAD_REQUEST,
                    msg,
                ));
            }

            match read_file_field(field, config.file_kind).await {
                Ok(value) => file = Some(value),
                Err((status, msg)) => {
                    return Err(error_with_optional_event(
                        kafka,
                        config.media_type,
                        object_id.as_deref(),
                        status,
                        msg,
                    ));
                }
            }
        } else {
            tracing::debug!("Ignoring unsupported multipart field '{}'", field_name);
        }
    }

    let object_id = match object_id {
        Some(value) => value,
        None => {
            let field_name = match config.object_id_source {
                ObjectIdSource::MultipartField(name) => name,
                ObjectIdSource::JwtSubject => "JWT sub",
            };
            return Err(error_response(
                StatusCode::BAD_REQUEST,
                format!("Поле {} обязательно", field_name),
            ));
        }
    };

    let file = match file {
        Some(value) => value,
        None => {
            return Err(error_with_optional_event(
                kafka,
                config.media_type,
                Some(&object_id),
                StatusCode::BAD_REQUEST,
                "Файл не найден в запросе".into(),
            ));
        }
    };

    Ok(ParsedUpload { object_id, file })
}

fn matches_field(source: ObjectIdSource, field_name: &str) -> bool {
    match source {
        ObjectIdSource::JwtSubject => false,
        ObjectIdSource::MultipartField(name) => field_name == name,
    }
}

async fn read_text_field(
    field: Field<'_>,
    field_name: &str,
) -> Result<String, (StatusCode, String)> {
    let raw = field.text().await.map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            format!("Ошибка чтения {}: {}", field_name, e),
        )
    })?;

    let value = raw.trim().to_string();
    if value.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("Поле {} не должно быть пустым", field_name),
        ));
    }

    Ok(value)
}

async fn read_file_field(
    mut field: Field<'_>,
    file_kind: FileKind,
) -> Result<PreparedFile, (StatusCode, String)> {
    let filename = field.file_name().unwrap_or("unknown").to_string();
    let extension = validation::validate_extension(&filename, file_kind)
        .map_err(|e| (StatusCode::UNSUPPORTED_MEDIA_TYPE, e))?;

    let mut total_bytes: usize = 0;
    let mut magic_checked = false;
    let mut head_buf = Vec::new();
    let mut bytes = Vec::new();

    info!(
        "Start receiving file '{}' (kind={:?}, ext={})",
        filename, file_kind, extension
    );

    loop {
        let chunk = match field.chunk().await {
            Ok(Some(c)) => c,
            Ok(None) => break,
            Err(e) => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!("Ошибка чтения данных файла: {}", e),
                ));
            }
        };

        total_bytes += chunk.len();

        if total_bytes > MAX_FILE_SIZE {
            return Err((
                StatusCode::PAYLOAD_TOO_LARGE,
                format!(
                    "Файл слишком большой: {} MB (максимум: {} MB)",
                    total_bytes / (1024 * 1024),
                    MAX_FILE_SIZE / (1024 * 1024)
                ),
            ));
        }

        if !magic_checked {
            head_buf.extend_from_slice(&chunk);
            if head_buf.len() >= MIN_MAGIC_BYTES {
                let detected_format = validation::validate_magic_bytes(&head_buf, file_kind)
                    .map_err(|e| (StatusCode::UNSUPPORTED_MEDIA_TYPE, e))?;
                validation::check_extension_magic_compatibility(
                    &extension,
                    detected_format,
                    file_kind,
                )
                .map_err(|e| (StatusCode::BAD_REQUEST, e))?;
                info!(
                    "File '{}' passed magic bytes check: ext={}, magic={}",
                    filename, extension, detected_format
                );
                magic_checked = true;
                head_buf.clear();
            }
        }

        bytes.extend_from_slice(&chunk);
    }

    if total_bytes == 0 {
        return Err((StatusCode::BAD_REQUEST, "Файл пустой".into()));
    }

    if !magic_checked {
        return Err((
            StatusCode::BAD_REQUEST,
            "Файл слишком мал для определения формата".into(),
        ));
    }

    Ok(PreparedFile {
        filename,
        content_type: validation::mime_from_extension(&extension).to_string(),
        extension,
        size: total_bytes,
        bytes,
    })
}

fn validate_uuid_value(
    value: &str,
    field_name: &str,
) -> Result<String, (StatusCode, Json<UploadResponse>)> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(error_response(
            StatusCode::BAD_REQUEST,
            format!("Поле {} не должно быть пустым", field_name),
        ));
    }

    Uuid::parse_str(trimmed)
        .map(|uuid| uuid.to_string())
        .map_err(|_| {
            error_response(
                StatusCode::BAD_REQUEST,
                format!("Поле {} должно быть UUID", field_name),
            )
        })
}

fn error_with_optional_event(
    kafka: &SharedKafkaProducer,
    media_type: MediaObjectType,
    object_id: Option<&str>,
    status: StatusCode,
    error: String,
) -> (StatusCode, Json<UploadResponse>) {
    if let Some(object_id) = object_id {
        send_error_event(kafka, media_type, object_id, &error);
    }

    error_response(status, error)
}

fn error_response(status: StatusCode, error: String) -> (StatusCode, Json<UploadResponse>) {
    tracing::warn!("Upload error: {}", error);
    (
        status,
        Json(UploadResponse {
            success: false,
            media_type: None,
            object_id: None,
            url: None,
            size: None,
            content_type: None,
            filename: None,
            message: None,
            error: Some(error),
        }),
    )
}

fn send_error_event(
    kafka: &SharedKafkaProducer,
    media_type: MediaObjectType,
    object_id: &str,
    msg: &str,
) {
    let kafka = kafka.clone();
    let object_id = object_id.to_string();
    let msg = msg.to_string();
    tokio::spawn(async move {
        if let Err(e) = kafka.send_error(media_type, &object_id, &msg).await {
            tracing::warn!("Failed to publish media.error: {}", e);
        }
    });
}
