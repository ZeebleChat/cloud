use axum::{Json, http::StatusCode};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct GetTokenRequest {
    pub room_name: String,
    pub user_id: String,
    pub username: String,
}

#[derive(Debug, Serialize)]
pub struct TokenResponse {
    pub token: String,
    pub url: String,
    pub room: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct VideoGrant {
    #[serde(rename = "roomJoin")]
    room_join: bool,
    room: String,
    #[serde(rename = "canPublish")]
    can_publish: bool,
    #[serde(rename = "canSubscribe")]
    can_subscribe: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct LiveKitClaims {
    iss: String,
    sub: String,
    name: String,
    exp: u64,
    nbf: u64,
    video: VideoGrant,
}

pub async fn get_livekit_token(
    Json(payload): Json<GetTokenRequest>,
) -> Result<Json<TokenResponse>, StatusCode> {
    let livekit_url = std::env::var("LIVEKIT_URL")
        .unwrap_or_else(|_| "http://localhost:7880".to_string());
    let livekit_api_key = std::env::var("LIVEKIT_API_KEY")
        .unwrap_or_else(|_| "zeeble-dev-key".to_string());
    let livekit_api_secret = std::env::var("LIVEKIT_API_SECRET")
        .unwrap_or_else(|_| "change-me-livekit-secret-min-32-chars".to_string());

    let room = format!("cloud-{}", payload.room_name);

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let claims = LiveKitClaims {
        iss: livekit_api_key,
        sub: payload.user_id,
        name: payload.username,
        exp: now + 3600,
        nbf: 0,
        video: VideoGrant {
            room_join: true,
            room: room.clone(),
            can_publish: true,
            can_subscribe: true,
        },
    };

    let token = encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(livekit_api_secret.as_bytes()),
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(TokenResponse { token, url: livekit_url, room }))
}
