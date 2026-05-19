use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::{Modify, OpenApi};

use crate::kafka::MediaEvent;
use crate::upload::{UploadRequest, UploadResponse};

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
        crate::upload::upload_media,
    ),
    components(
        schemas(
            UploadRequest,
            UploadResponse,
            MediaEvent,
        )
    ),
    modifiers(&SecurityAddon),
    tags(
        (name = "media", description = "Media upload API — загрузка аудиофайлов")
    )
)]
pub struct ApiDoc;
