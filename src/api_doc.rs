use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::{Modify, OpenApi};

use crate::kafka::{MediaEvent, MediaObjectType};
use crate::upload::{
    UploadAudioRequest, UploadPlaylistCoverRequest, UploadPodcastCoverRequest,
    UploadProfileCoverRequest, UploadResponse,
};

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "bearer_auth",
                SecurityScheme::Http(
                    HttpBuilder::new()
                        .scheme(HttpAuthScheme::Bearer)
                        .bearer_format("JWT")
                        .build(),
                ),
            );
        }
    }
}

#[derive(OpenApi)]
#[openapi(
    paths(
        crate::upload::upload_audio,
        crate::upload::upload_cover_profile,
        crate::upload::upload_cover_podcast,
        crate::upload::upload_cover_playlist,
    ),
    components(
        schemas(
            UploadAudioRequest,
            UploadProfileCoverRequest,
            UploadPodcastCoverRequest,
            UploadPlaylistCoverRequest,
            UploadResponse,
            MediaObjectType,
            MediaEvent,
        )
    ),
    modifiers(&SecurityAddon),
    tags(
        (name = "media", description = "Media upload API — загрузка аудио и изображений")
    )
)]
pub struct ApiDoc;
