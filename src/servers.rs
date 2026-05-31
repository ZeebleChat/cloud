use axum::{
    Json,
    extract::{State, Path, Multipart, Query},
    http::{StatusCode, header, HeaderMap},
    response::Response,
    body::Body,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{PgPool, Row};
use tracing::error;
use uuid::Uuid;
use chrono::{DateTime, Utc};

use crate::AppState;

#[derive(Debug, Deserialize)]
pub struct CreateServerRequest {
    pub name: String,
    pub about: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ServerResponse {
    pub id: Uuid,
    pub name: String,
    pub owner_id: String,
    pub about: Option<String>,
    pub icon_url: Option<String>,
    pub server_url: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct Channel {
    pub id: Uuid,
    pub server_id: Uuid,
    pub name: String,
    pub channel_type: String,
    pub position: i32,
    pub topic: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct ServerMember {
    pub beam_identity: String,
    pub role: String,
    pub joined_at: DateTime<Utc>,
    pub status: String,
}

pub async fn create_server(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<CreateServerRequest>,
) -> Result<(StatusCode, Json<ServerResponse>), StatusCode> {
    let claims = validate_jwt_claims(&headers, &state.ed25519_x)?;
    let owner_id = claims.sub;

    let row = sqlx::query(
        "INSERT INTO servers (name, owner_id, about) VALUES ($1, $2, $3) RETURNING *",
    )
    .bind(&payload.name)
    .bind(&owner_id)
    .bind(&payload.about)
    .fetch_one(&state.db)
    .await
    .map_err(|e| { error!("create_server: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    let server_id: Uuid = row.get("id");

    sqlx::query(
        "INSERT INTO server_members (server_id, user_id, role) VALUES ($1, $2, 'owner')",
    )
    .bind(server_id)
    .bind(&owner_id)
    .execute(&state.db)
    .await
    .map_err(|e| { error!("create_server members: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    sqlx::query(
        "INSERT INTO channels (server_id, name, channel_type, position) VALUES ($1, 'general', 'text', 0)",
    )
    .bind(server_id)
    .execute(&state.db)
    .await
    .map_err(|e| { error!("create_server channel: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    let server_url = format!("{}/servers/{}", state.public_url.trim_end_matches('/'), server_id);

    Ok((StatusCode::CREATED, Json(ServerResponse {
        id: server_id,
        name: row.get("name"),
        owner_id: row.get("owner_id"),
        about: row.get("about"),
        icon_url: row.get("icon_url"),
        server_url,
        created_at: row.get("created_at"),
    })))
}

#[allow(dead_code)]
pub async fn get_server_channels(
    State(state): State<AppState>,
    Path(server_id): Path<Uuid>,
) -> Result<Json<Vec<Channel>>, StatusCode> {
    let rows = sqlx::query(
        "SELECT id, server_id, name, channel_type, position, topic, created_at
         FROM channels WHERE server_id = $1 ORDER BY position, name",
    )
    .bind(server_id)
    .fetch_all(&state.db)
    .await
    .map_err(|e| { error!("get_server_channels: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    Ok(Json(rows.iter().map(|row| Channel {
        id: row.get("id"),
        server_id: row.get("server_id"),
        name: row.get("name"),
        channel_type: row.get("channel_type"),
        position: row.get("position"),
        topic: row.get("topic"),
        created_at: row.get("created_at"),
    }).collect()))
}

pub async fn get_server_members(
    State(state): State<AppState>,
    Path(server_id): Path<Uuid>,
) -> Result<Json<Vec<ServerMember>>, StatusCode> {
    let rows = sqlx::query(
        "SELECT user_id, role, joined_at FROM server_members WHERE server_id = $1",
    )
    .bind(server_id)
    .fetch_all(&state.db)
    .await
    .map_err(|e| { error!("get_server_members: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    let online = state.connections.read().await;
    Ok(Json(rows.iter().map(|row| {
        let beam_identity: String = row.get("user_id");
        let status = if online.online_beam_ids.contains(&beam_identity) {
            "online".to_string()
        } else {
            "offline".to_string()
        };
        ServerMember {
            beam_identity,
            role: row.get("role"),
            joined_at: row.get("joined_at"),
            status,
        }
    }).collect()))
}

// ── v1 API routes ─────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct ServerInfoResponse {
    pub name: String,
    pub about: Option<String>,
    pub public_url: String,
}

pub async fn server_info(
    State(state): State<AppState>,
    Path(server_id): Path<Uuid>,
) -> Result<Json<ServerInfoResponse>, StatusCode> {
    let row = sqlx::query("SELECT name, about FROM servers WHERE id = $1")
        .bind(server_id)
        .fetch_optional(&state.db)
        .await
        .map_err(|e| { error!("server_info: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?
        .ok_or(StatusCode::NOT_FOUND)?;

    Ok(Json(ServerInfoResponse {
        name: row.get("name"),
        about: row.get("about"),
        public_url: format!("{}/servers/{}", state.public_url.trim_end_matches('/'), server_id),
    }))
}

#[derive(Debug, Serialize)]
pub struct ApiChannel {
    pub id: Uuid,
    #[serde(rename = "type")]
    pub channel_type: String,
    pub name: String,
    pub position: i32,
    pub topic: Option<String>,
}

pub async fn get_channels_v1(
    State(state): State<AppState>,
    Path(server_id): Path<Uuid>,
) -> Result<Json<Vec<ApiChannel>>, StatusCode> {
    let rows = sqlx::query(
        "SELECT id, name, channel_type, position, topic
         FROM channels WHERE server_id = $1 ORDER BY position, name LIMIT 500",
    )
    .bind(server_id)
    .fetch_all(&state.db)
    .await
    .map_err(|e| { error!("get_channels_v1: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    Ok(Json(rows.iter().map(|row| ApiChannel {
        id: row.get("id"),
        channel_type: row.get("channel_type"),
        name: row.get("name"),
        position: row.get("position"),
        topic: row.get("topic"),
    }).collect()))
}


pub async fn get_categories(
    Path(_server_id): Path<Uuid>,
) -> Json<serde_json::Value> {
    Json(json!({ "categories": [] }))
}

pub async fn get_custom_roles(
    Path(_server_id): Path<Uuid>,
) -> Json<serde_json::Value> {
    Json(json!([]))
}

pub async fn get_voice_rooms(
    Path(_server_id): Path<Uuid>,
) -> Json<serde_json::Value> {
    Json(json!([]))
}

#[derive(Debug, Serialize)]
pub struct ApiMessage {
    pub id: Uuid,
    pub channel_id: Uuid,
    pub beam_identity: String,
    pub content: String,
    pub title: Option<String>,
    pub reply_to: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub edited_at: Option<DateTime<Utc>>,
    pub attachments: Vec<serde_json::Value>,
}

fn row_to_message(row: &sqlx::postgres::PgRow) -> ApiMessage {
    ApiMessage {
        id: row.get("id"),
        channel_id: row.get("channel_id"),
        beam_identity: row.get("beam_identity"),
        content: row.get("content"),
        title: row.get("title"),
        reply_to: row.get("reply_to"),
        created_at: row.get("created_at"),
        edited_at: row.get("edited_at"),
        attachments: vec![],
    }
}

/// Authenticate the request and confirm the caller is a member of `server_id`.
async fn verify_member(db: &PgPool, headers: &HeaderMap, ed25519_x: &str, server_id: Uuid) -> Result<(), StatusCode> {
    let claims = validate_jwt_claims(headers, ed25519_x)?;
    let row = sqlx::query(
        "SELECT 1 FROM server_members WHERE server_id = $1 AND user_id = $2",
    )
    .bind(server_id)
    .bind(&claims.sub)
    .fetch_optional(db)
    .await
    .map_err(|e| { error!("verify_member: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;
    row.map(|_| ()).ok_or(StatusCode::FORBIDDEN)
}

async fn verify_channel(db: &PgPool, channel_id: Uuid, server_id: Uuid) -> Result<(), StatusCode> {
    let exists = sqlx::query("SELECT id FROM channels WHERE id = $1 AND server_id = $2")
        .bind(channel_id)
        .bind(server_id)
        .fetch_optional(db)
        .await
        .map_err(|e| { error!("verify_channel: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;
    exists.map(|_| ()).ok_or(StatusCode::NOT_FOUND)
}

pub async fn get_messages(
    State(state): State<AppState>,
    Path((server_id, channel_id)): Path<(Uuid, Uuid)>,
    headers: HeaderMap,
) -> Result<Json<Vec<ApiMessage>>, StatusCode> {
    verify_member(&state.db, &headers, &state.ed25519_x, server_id).await?;
    verify_channel(&state.db, channel_id, server_id).await?;

    let rows = sqlx::query(
        "SELECT id, channel_id, beam_identity, content, title, reply_to, created_at, edited_at
         FROM messages WHERE channel_id = $1 ORDER BY created_at ASC LIMIT 200",
    )
    .bind(channel_id)
    .fetch_all(&state.db)
    .await
    .map_err(|e| { error!("get_messages: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    Ok(Json(rows.iter().map(row_to_message).collect()))
}

pub async fn get_board_posts(
    State(state): State<AppState>,
    Path((server_id, channel_id)): Path<(Uuid, Uuid)>,
    headers: HeaderMap,
) -> Result<Json<Vec<ApiMessage>>, StatusCode> {
    verify_member(&state.db, &headers, &state.ed25519_x, server_id).await?;
    verify_channel(&state.db, channel_id, server_id).await?;

    let rows = sqlx::query(
        "SELECT id, channel_id, beam_identity, content, title, reply_to, created_at, edited_at
         FROM messages
         WHERE channel_id = $1 AND reply_to IS NULL
         ORDER BY created_at DESC LIMIT 100",
    )
    .bind(channel_id)
    .fetch_all(&state.db)
    .await
    .map_err(|e| { error!("get_board_posts: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    Ok(Json(rows.iter().map(row_to_message).collect()))
}

pub async fn get_post_replies(
    State(state): State<AppState>,
    Path((server_id, channel_id, post_id)): Path<(Uuid, Uuid, Uuid)>,
    headers: HeaderMap,
) -> Result<Json<Vec<ApiMessage>>, StatusCode> {
    verify_member(&state.db, &headers, &state.ed25519_x, server_id).await?;
    verify_channel(&state.db, channel_id, server_id).await?;

    let rows = sqlx::query(
        "SELECT id, channel_id, beam_identity, content, title, reply_to, created_at, edited_at
         FROM messages
         WHERE channel_id = $1 AND reply_to = $2
         ORDER BY created_at ASC LIMIT 500",
    )
    .bind(channel_id)
    .bind(post_id)
    .fetch_all(&state.db)
    .await
    .map_err(|e| { error!("get_post_replies: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    Ok(Json(rows.iter().map(row_to_message).collect()))
}

// ── Channel read tracking ─────────────────────────────────────────────────────

pub async fn mark_channel_read(
    State(state): State<AppState>,
    Path((server_id, channel_id)): Path<(Uuid, Uuid)>,
    headers: HeaderMap,
) -> Result<StatusCode, StatusCode> {
    let claims = validate_jwt_claims(&headers, &state.ed25519_x)?;
    verify_member(&state.db, &headers, &state.ed25519_x, server_id).await?;
    verify_channel(&state.db, channel_id, server_id).await?;

    sqlx::query(
        "INSERT INTO channel_reads (user_id, channel_id, read_at)
         VALUES ($1, $2, now())
         ON CONFLICT (user_id, channel_id) DO UPDATE SET read_at = now()",
    )
    .bind(&claims.sub)
    .bind(channel_id)
    .execute(&state.db)
    .await
    .map_err(|e| { error!("mark_channel_read: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    Ok(StatusCode::NO_CONTENT)
}

pub async fn get_unread_channels(
    State(state): State<AppState>,
    Path(server_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let claims = validate_jwt_claims(&headers, &state.ed25519_x)?;
    verify_member(&state.db, &headers, &state.ed25519_x, server_id).await?;

    let rows = sqlx::query(
        "SELECT c.id
         FROM channels c
         WHERE c.server_id = $1
           AND EXISTS (
             SELECT 1 FROM messages m
             WHERE m.channel_id = c.id
               AND m.created_at > COALESCE(
                 (SELECT cr.read_at FROM channel_reads cr
                  WHERE cr.channel_id = c.id AND cr.user_id = $2),
                 '-infinity'::timestamptz
               )
           )",
    )
    .bind(server_id)
    .bind(&claims.sub)
    .fetch_all(&state.db)
    .await
    .map_err(|e| { error!("get_unread_channels: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    let channel_ids: Vec<String> = rows.iter()
        .map(|row| row.get::<Uuid, _>("id").to_string())
        .collect();

    Ok(Json(json!({ "channel_ids": channel_ids, "mentions": {} })))
}

// ── Invites ───────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct InviteResponse {
    pub code: String,
    pub created_by: String,
    pub max_uses: Option<i32>,
    pub use_count: i32,
    pub expires_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct ValidateInviteResponse {
    pub code: String,
    pub server_name: String,
    pub server_url: String,
    pub use_count: i32,
    pub max_uses: Option<i32>,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize)]
pub struct CreateInviteRequest {
    pub max_uses: Option<i32>,
    pub expires_in_secs: Option<i64>,
}

fn validate_jwt_claims(headers: &HeaderMap, ed25519_x: &str) -> Result<Claims, StatusCode> {
    let token = extract_bearer(headers).ok_or(StatusCode::UNAUTHORIZED)?;
    validate_jwt(&token, ed25519_x).map_err(|_| StatusCode::UNAUTHORIZED)
}

pub async fn list_invites(
    State(state): State<AppState>,
    Path(server_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Json<Vec<InviteResponse>>, StatusCode> {
    validate_jwt_claims(&headers, &state.ed25519_x)?;
    let rows = sqlx::query(
        "SELECT code, created_by, max_uses, use_count, expires_at, created_at
         FROM server_invites WHERE server_id = $1 ORDER BY created_at DESC",
    )
    .bind(server_id)
    .fetch_all(&state.db)
    .await
    .map_err(|e| { error!("list_invites: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    Ok(Json(rows.iter().map(|row| InviteResponse {
        code: row.get("code"),
        created_by: row.get("created_by"),
        max_uses: row.get("max_uses"),
        use_count: row.get("use_count"),
        expires_at: row.get("expires_at"),
        created_at: row.get("created_at"),
    }).collect()))
}

pub async fn create_invite(
    State(state): State<AppState>,
    Path(server_id): Path<Uuid>,
    headers: HeaderMap,
    Json(req): Json<CreateInviteRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let claims = validate_jwt_claims(&headers, &state.ed25519_x)?;
    let code = Uuid::new_v4().to_string().replace('-', "")[..10].to_string();
    let expires_at: Option<DateTime<Utc>> = req.expires_in_secs
        .map(|s| Utc::now() + chrono::Duration::seconds(s));
    sqlx::query(
        "INSERT INTO server_invites (code, server_id, created_by, max_uses, expires_at)
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(&code)
    .bind(server_id)
    .bind(&claims.sub)
    .bind(req.max_uses)
    .bind(expires_at)
    .execute(&state.db)
    .await
    .map_err(|e| { error!("create_invite: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    let server_url = format!("{}/servers/{}", state.public_url.trim_end_matches('/'), server_id);
    let url = format!("{}/join/{}", server_url, code);
    Ok(Json(json!({ "code": code, "url": url })))
}

pub async fn get_invite(
    State(state): State<AppState>,
    Path((server_id, code)): Path<(Uuid, String)>,
) -> Result<Json<ValidateInviteResponse>, StatusCode> {
    let row = sqlx::query(
        "SELECT si.code, si.max_uses, si.use_count, si.expires_at, s.name
         FROM server_invites si JOIN servers s ON s.id = si.server_id
         WHERE si.server_id = $1 AND si.code = $2",
    )
    .bind(server_id)
    .bind(&code)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| { error!("get_invite: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?
    .ok_or(StatusCode::NOT_FOUND)?;

    let expires_at: Option<DateTime<Utc>> = row.get("expires_at");
    if expires_at.map_or(false, |e| e < Utc::now()) {
        return Err(StatusCode::GONE);
    }
    let max_uses: Option<i32> = row.get("max_uses");
    let use_count: i32 = row.get("use_count");
    if max_uses.map_or(false, |m| use_count >= m) {
        return Err(StatusCode::GONE);
    }

    let server_url = format!("{}/servers/{}", state.public_url.trim_end_matches('/'), server_id);
    Ok(Json(ValidateInviteResponse {
        code: row.get("code"),
        server_name: row.get("name"),
        server_url,
        use_count,
        max_uses,
        expires_at,
    }))
}

pub async fn redeem_invite(
    State(state): State<AppState>,
    Path((server_id, code)): Path<(Uuid, String)>,
    headers: HeaderMap,
) -> Result<StatusCode, StatusCode> {
    let claims = validate_jwt_claims(&headers, &state.ed25519_x)?;

    let mut tx = state.db.begin().await
        .map_err(|e| { error!("redeem_invite begin tx: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    // FOR UPDATE locks the invite row so concurrent redemptions are serialized;
    // the max_uses check below is then guaranteed to see the committed use_count.
    let row = sqlx::query(
        "SELECT max_uses, use_count, expires_at FROM server_invites
         WHERE server_id = $1 AND code = $2 FOR UPDATE",
    )
    .bind(server_id)
    .bind(&code)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| { error!("redeem_invite fetch: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?
    .ok_or(StatusCode::NOT_FOUND)?;

    let expires_at: Option<DateTime<Utc>> = row.get("expires_at");
    if expires_at.map_or(false, |e| e < Utc::now()) {
        return Err(StatusCode::GONE);
    }
    let max_uses: Option<i32> = row.get("max_uses");
    let use_count: i32 = row.get("use_count");
    if max_uses.map_or(false, |m| use_count >= m) {
        return Err(StatusCode::GONE);
    }

    let inserted = sqlx::query(
        "INSERT INTO server_members (server_id, user_id, role)
         VALUES ($1, $2, 'member') ON CONFLICT DO NOTHING",
    )
    .bind(server_id)
    .bind(&claims.sub)
    .execute(&mut *tx)
    .await
    .map_err(|e| { error!("redeem_invite insert member: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    // Only count the redemption when the user was newly added; ON CONFLICT DO NOTHING
    // means they were already a member and no use should be consumed.
    if inserted.rows_affected() > 0 {
        sqlx::query(
            "UPDATE server_invites SET use_count = use_count + 1
             WHERE server_id = $1 AND code = $2",
        )
        .bind(server_id)
        .bind(&code)
        .execute(&mut *tx)
        .await
        .map_err(|e| { error!("redeem_invite update count: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;
    }

    tx.commit().await
        .map_err(|e| { error!("redeem_invite commit: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    Ok(StatusCode::NO_CONTENT)
}

pub async fn delete_invite(
    State(state): State<AppState>,
    Path((server_id, code)): Path<(Uuid, String)>,
    headers: HeaderMap,
) -> Result<StatusCode, StatusCode> {
    validate_jwt_claims(&headers, &state.ed25519_x)?;
    sqlx::query("DELETE FROM server_invites WHERE server_id = $1 AND code = $2")
        .bind(server_id)
        .bind(&code)
        .execute(&state.db)
        .await
        .map_err(|e| { error!("delete_invite: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn server_health(
    State(state): State<AppState>,
    Path(server_id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let exists = sqlx::query("SELECT id FROM servers WHERE id = $1")
        .bind(server_id)
        .fetch_optional(&state.db)
        .await
        .map_err(|e| { error!("server_health: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    if exists.is_none() {
        return Err(StatusCode::NOT_FOUND);
    }
    Ok(Json(json!({ "status": "ok" })))
}

#[derive(Deserialize)]
pub struct TokenQuery {
    pub token: Option<String>,
}

#[derive(Deserialize)]
struct Claims {
    sub: String,
}

fn extract_bearer(headers: &HeaderMap) -> Option<String> {
    headers.get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(|s| s.to_string())
}

fn validate_jwt(token: &str, ed25519_x: &str) -> Result<Claims, ()> {
    use jsonwebtoken::{decode, DecodingKey, Validation, Algorithm};
    let key = DecodingKey::from_ed_components(ed25519_x).map_err(|_| ())?;
    let mut val = Validation::new(Algorithm::EdDSA);
    val.validate_aud = false;
    decode::<Claims>(token, &key, &val).map(|d| d.claims).map_err(|_| ())
}

fn is_allowed_mime(mime: &str) -> bool {
    // Normalise: strip parameters ("image/png; charset=utf-8" -> "image/png")
    // and lowercase so comparisons are case-insensitive.
    let base = mime.split(';').next().unwrap_or("").trim().to_lowercase();
    matches!(
        base.as_str(),
        "image/jpeg"
            | "image/png"
            | "image/gif"
            | "image/webp"
            | "video/mp4"
            | "video/webm"
            | "video/quicktime"
            | "audio/mpeg"
            | "audio/ogg"
            | "audio/wav"
            | "audio/webm"
            | "audio/aac"
            | "application/pdf"
            | "text/plain"
    )
}

pub async fn upload_file(
    State(state): State<crate::AppState>,
    Path(server_id): Path<Uuid>,
    headers: HeaderMap,
    mut multipart: Multipart,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let token = extract_bearer(&headers).ok_or(StatusCode::UNAUTHORIZED)?;
    let claims = validate_jwt(&token, &state.ed25519_x).map_err(|_| StatusCode::UNAUTHORIZED)?;

    sqlx::query("SELECT id FROM servers WHERE id = $1")
        .bind(server_id)
        .fetch_optional(&state.db)
        .await
        .map_err(|e| { error!("upload_file verify server: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?
        .ok_or(StatusCode::NOT_FOUND)?;

    while let Some(field) = multipart.next_field().await.map_err(|_| StatusCode::BAD_REQUEST)? {
        let name = field.name().unwrap_or("").to_string();
        if name != "file0" { continue; }

        const MAX_FILE_BYTES: usize = 25 * 1024 * 1024; // 25 MB

        let filename = field.file_name().unwrap_or("upload").to_string();
        let mime = field.content_type().unwrap_or("application/octet-stream").to_string();

        if !is_allowed_mime(&mime) {
            return Err(StatusCode::UNSUPPORTED_MEDIA_TYPE);
        }

        // Stream chunks so an oversized upload is rejected before it is fully
        // buffered in memory, not just before it is written to the database.
        let mut buf: Vec<u8> = Vec::new();
        loop {
            match field.chunk().await {
                Ok(Some(chunk)) => {
                    buf.extend_from_slice(&chunk);
                    if buf.len() > MAX_FILE_BYTES {
                        return Err(StatusCode::PAYLOAD_TOO_LARGE);
                    }
                }
                Ok(None) => break,
                Err(_) => return Err(StatusCode::BAD_REQUEST),
            }
        }
        let size = buf.len() as i64;

        let row = sqlx::query(
            "INSERT INTO attachments (server_id, filename, mime_type, file_size, file_data, uploaded_by)
             VALUES ($1, $2, $3, $4, $5, $6) RETURNING id",
        )
        .bind(server_id)
        .bind(&filename)
        .bind(&mime)
        .bind(size)
        .bind(buf.as_slice())
        .bind(&claims.sub)
        .fetch_one(&state.db)
        .await
        .map_err(|e| { error!("upload_file insert: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

        let id: Uuid = row.get("id");
        return Ok(Json(json!({ "attachments": [{ "attachment_id": id }] })));
    }

    Err(StatusCode::BAD_REQUEST)
}

pub async fn get_attachment(
    State(state): State<crate::AppState>,
    Path((server_id, attach_id)): Path<(Uuid, Uuid)>,
    Query(q): Query<TokenQuery>,
    headers: HeaderMap,
) -> Result<Response<Body>, StatusCode> {
    let token = extract_bearer(&headers)
        .or_else(|| q.token.clone())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    validate_jwt(&token, &state.ed25519_x).map_err(|_| StatusCode::UNAUTHORIZED)?;

    let row = sqlx::query(
        "SELECT filename, mime_type, file_data FROM attachments WHERE id = $1 AND server_id = $2",
    )
    .bind(attach_id)
    .bind(server_id)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| { error!("get_attachment: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?
    .ok_or(StatusCode::NOT_FOUND)?;

    let mime: String = row.get("mime_type");
    let data: Vec<u8> = row.get("file_data");
    // Sanitise filename for use in a header value (strip CR/LF/quotes).
    let raw_filename: String = row.get("filename");
    let safe_filename: String = raw_filename.chars()
        .filter(|c| *c != '"' && *c != '\r' && *c != '\n' && *c != '\\')
        .collect();
    let disposition = format!("attachment; filename=\"{}\"", safe_filename);

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, mime)
        .header(header::CONTENT_DISPOSITION, disposition)
        .body(Body::from(data))
        .unwrap())
}
