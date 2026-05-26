use axum::{
    Json,
    extract::{State, Path},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use redis::AsyncCommands;
use serde::Deserialize;
use serde_json::json;
use tracing::{error, warn};

use crate::AppState;

pub mod ws;

// ── Auth ──────────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct VoiceClaims {
    sub: Option<String>,
    beam_identity: Option<String>,
}

pub fn validate_voice_token(token: &str, ed25519_x: &str) -> Option<String> {
    use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};

    let key = DecodingKey::from_ed_components(ed25519_x).ok()?;
    let mut val = Validation::new(Algorithm::EdDSA);
    val.validate_aud = false;

    decode::<VoiceClaims>(token, &key, &val)
        .ok()
        .and_then(|d| d.claims.beam_identity.or(d.claims.sub))
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
}

pub async fn require_voice_auth(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<String, (StatusCode, Json<serde_json::Value>)> {
    let token = bearer(headers);
    match token.and_then(|t| validate_voice_token(t, &state.ed25519_x)) {
        Some(id) => Ok(id),
        None => Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "Invalid or expired token" })),
        )),
    }
}

// ── REST handlers ─────────────────────────────────────────────────────────────

pub async fn get_voice_rooms(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(e) = require_voice_auth(&state, &headers).await {
        return e.into_response();
    }
    let mut redis = state.redis.clone();
    let room_ids: Vec<String> = match redis.smembers("voice:rooms").await {
        Ok(ids) => ids,
        Err(e) => {
            error!("GET /voice/v1/voice/rooms: redis failed: {e}");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "voice state unavailable" })),
            )
                .into_response();
        }
    };
    let mut rooms = Vec::new();
    for channel_id in room_ids {
        let participants: Vec<String> = match redis
            .smembers(format!("voice:room:{channel_id}"))
            .await
        {
            Ok(p) => p,
            Err(e) => {
                warn!("voice/rooms: redis failed for room {channel_id}: {e}");
                state.voice_participants(&channel_id)
            }
        };
        if !participants.is_empty() {
            rooms.push(json!({
                "channel_id": channel_id,
                "participant_count": participants.len(),
                "participants": participants,
            }));
        }
    }
    Json(json!({ "rooms": rooms })).into_response()
}

pub async fn get_voice_participants(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(channel_id): Path<String>,
) -> impl IntoResponse {
    if let Err(e) = require_voice_auth(&state, &headers).await {
        return e.into_response();
    }
    let mut redis = state.redis.clone();
    let participants: Vec<String> = match redis
        .smembers(format!("voice:room:{channel_id}"))
        .await
    {
        Ok(p) => p,
        Err(e) => {
            warn!("voice/participants/{channel_id}: redis failed: {e}");
            state.voice_participants(&channel_id)
        }
    };
    Json(json!({ "channel_id": channel_id, "participants": participants })).into_response()
}

pub async fn list_streams(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(e) = require_voice_auth(&state, &headers).await {
        return e.into_response();
    }
    let mut redis = state.redis.clone();
    let channel_ids: Vec<String> = match redis.smembers("stream:live").await {
        Ok(ids) => ids,
        Err(e) => {
            error!("GET /voice/v1/streams: redis failed: {e}");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "stream state unavailable" })),
            )
                .into_response();
        }
    };
    let mut streams = Vec::new();
    for channel_id in channel_ids {
        let broadcaster: Option<String> =
            match redis.get(format!("stream:{channel_id}")).await {
                Ok(b) => b,
                Err(e) => {
                    warn!("voice/streams: redis failed for {channel_id}: {e}");
                    None
                }
            };
        if let Some(broadcaster) = broadcaster {
            streams.push(json!({ "channel_id": channel_id, "broadcaster": broadcaster }));
        }
    }
    Json(json!({ "streams": streams })).into_response()
}

pub async fn get_stream(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(channel_id): Path<String>,
) -> impl IntoResponse {
    if let Err(e) = require_voice_auth(&state, &headers).await {
        return e.into_response();
    }
    let mut redis = state.redis.clone();
    let broadcaster: Option<String> =
        match redis.get(format!("stream:{channel_id}")).await {
            Ok(b) => b,
            Err(e) => {
                warn!("voice/stream/{channel_id}: redis failed: {e}");
                None
            }
        };
    match broadcaster {
        Some(b) => {
            Json(json!({ "live": true, "channel_id": channel_id, "broadcaster": b }))
                .into_response()
        }
        None => {
            Json(json!({ "live": false, "channel_id": channel_id })).into_response()
        }
    }
}
