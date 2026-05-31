mod api_doc;
mod auth;
mod kafka;
mod s3;
mod telemetry;
mod upload;
mod validation;

use std::sync::Arc;

use axum::{
    extract::DefaultBodyLimit,
    routing::{get, post},
    Json, Router,
};
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::{DefaultMakeSpan, TraceLayer};
use tracing::{info, Level};
use utoipa::OpenApi;

use api_doc::ApiDoc;

#[tokio::main]
async fn main() {
    let _telemetry = telemetry::init("media_upload_service");

    let kafka_brokers =
        std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string());
    let kafka_producer =
        kafka::new_producer(&kafka_brokers).expect("Failed to create Kafka producer");

    let s3_cfg = s3::Config::from_env().expect("S3 config: set S3_* env variables");
    let s3_client = Arc::new(
        s3::create_client(&s3_cfg)
            .await
            .expect("Failed to create S3 client"),
    );
    let audio_bucket = std::env::var("AUDIO_BUCKET")
        .ok()
        .or_else(|| std::env::var("S3_BUCKET").ok())
        .filter(|value| !value.trim().is_empty())
        .expect("AUDIO_BUCKET or S3_BUCKET is required");
    s3_client
        .ensure_bucket(&audio_bucket)
        .await
        .expect("Failed to access audio bucket");

    let jwt_secret = std::env::var("JWT_SECRET")
        .or_else(|_| std::env::var("ACCESS_TOKEN_SECRET"))
        .unwrap_or_else(|_| "super-secret-key-change-me".to_string());
    info!("Using JWT secret: {}", jwt_secret);

    let state = upload::AppState {
        kafka: kafka_producer,
        s3: s3_client,
        s3_endpoint_url: s3_cfg.endpoint_url,
        jwt_secret,
        audio_bucket,
    };

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .route("/api-docs/openapi.json", get(openapi_json))
        .route("/api/media/upload_audio", post(upload::upload_audio))
        .route(
            "/api/media/upload_cover_profile",
            post(upload::upload_cover_profile),
        )
        .route(
            "/api/media/upload_cover_podcast",
            post(upload::upload_cover_podcast),
        )
        .route(
            "/api/media/upload_cover_playlist",
            post(upload::upload_cover_playlist),
        )
        .layer(DefaultBodyLimit::max(50 * 1024 * 1024))
        .layer(cors)
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::new().level(Level::INFO)),
        )
        .with_state(state);

    let port = std::env::var("PORT").unwrap_or_else(|_| "8081".to_string());
    let addr = format!("0.0.0.0:{}", port);

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("Failed to bind");

    info!("media_api listening on http://{}", addr);
    info!(
        "OpenAPI JSON: http://localhost:{}/api-docs/openapi.json",
        port
    );

    axum::serve(listener, app).await.expect("Server error");
}

async fn openapi_json() -> Json<utoipa::openapi::OpenApi> {
    Json(ApiDoc::openapi())
}
