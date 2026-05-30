use axum::{
    extract::FromRequestParts,
    http::{header, request::Parts, StatusCode},
};
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::upload::AppState;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct Claims {
    /// User/author id. Legacy tokens may use the standard JWT `sub` claim.
    #[serde(alias = "sub")]
    pub user_id: String,
    /// Expiration (unix timestamp)
    pub exp: usize,
    /// Issued at (unix timestamp)
    #[serde(default)]
    pub iat: usize,
}

pub struct AuthUser(pub Claims);

#[axum::async_trait]
impl FromRequestParts<AppState> for AuthUser {
    type Rejection = (StatusCode, String);

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let auth_header = parts
            .headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| {
                (
                    StatusCode::UNAUTHORIZED,
                    "Missing Authorization header".to_string(),
                )
            })?;

        let token = auth_header.strip_prefix("Bearer ").ok_or_else(|| {
            (
                StatusCode::UNAUTHORIZED,
                "Invalid Authorization header format. Expected: Bearer <token>".to_string(),
            )
        })?;

        let decoding_key = DecodingKey::from_secret(state.jwt_secret.as_bytes());
        let mut validation = Validation::new(Algorithm::HS256);
        validation.validate_exp = true;

        let token_data = decode::<Claims>(token, &decoding_key, &validation).map_err(|e| {
            tracing::warn!("JWT validation failed: {}", e);
            (
                StatusCode::UNAUTHORIZED,
                format!("Invalid or expired token: {}", e),
            )
        })?;

        Ok(AuthUser(token_data.claims))
    }
}

#[cfg(test)]
mod tests {
    use super::Claims;

    #[test]
    fn deserializes_auth_service_user_id_claim() {
        let claims: Claims = serde_json::from_str(
            r#"{"user_id":"11111111-1111-4111-8111-111111111111","exp":1780150720}"#,
        )
        .unwrap();

        assert_eq!(claims.user_id, "11111111-1111-4111-8111-111111111111");
    }

    #[test]
    fn accepts_legacy_sub_claim() {
        let claims: Claims = serde_json::from_str(
            r#"{"sub":"11111111-1111-4111-8111-111111111111","exp":1780150720}"#,
        )
        .unwrap();

        assert_eq!(claims.user_id, "11111111-1111-4111-8111-111111111111");
    }
}
