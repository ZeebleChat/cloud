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
use uuid::Uuid;
use chrono::{DateTime, Utc};
use tracing::error;

use crate::AppState;


#[derive(Deserialize)]
pub struct MessageQuery {
    pub before: Option<Uuid>,
    pub limit: Option<i64>,
}

#[derive(Deserialize)]
pub struct OffsetQuery {
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

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
    #[serde(rename = "beam_identity")]
    pub user_id: String,
    pub display_name: Option<String>,
    pub role: String,
    pub joined_at: DateTime<Utc>,
    pub is_owner: bool,
    pub status: String,
}

pub async fn delete_server(
    State(state): State<AppState>,
    Path(server_id): Path<Uuid>,
    headers: HeaderMap,
) -> StatusCode {
    let claims = match require_auth_sync(&headers, &state.ed25519_x) {
        Ok(c) => c,
        Err(s) => return s,
    };

    let row = sqlx::query("SELECT owner_id FROM servers WHERE id = $1")
        .bind(server_id)
        .fetch_optional(&state.db)
        .await;

    let owner_id: String = match row {
        Ok(Some(r)) => r.get("owner_id"),
        Ok(None) => return StatusCode::NOT_FOUND,
        Err(e) => { error!("zcloud delete_server error: {e}"); return StatusCode::INTERNAL_SERVER_ERROR; }
    };

    if claims.sub != owner_id {
        return StatusCode::FORBIDDEN;
    }

    match sqlx::query("DELETE FROM servers WHERE id = $1")
        .bind(server_id)
        .execute(&state.db)
        .await
    {
        Ok(_) => StatusCode::OK,
        Err(e) => { error!("zcloud delete_server error: {e}"); StatusCode::INTERNAL_SERVER_ERROR }
    }
}

pub async fn create_server(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<CreateServerRequest>,
) -> Result<(StatusCode, Json<ServerResponse>), StatusCode> {
    let claims = require_auth_sync(&headers, &state.ed25519_x)?;
    let owner_id = &claims.sub;

    let row = sqlx::query(
        "INSERT INTO servers (name, owner_id, about) VALUES ($1, $2, $3) RETURNING *",
    )
    .bind(&payload.name)
    .bind(owner_id)
    .bind(&payload.about)
    .fetch_one(&state.db)
    .await
    .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    let server_id: Uuid = row.get("id");

    let owner_display_name: Option<String> = claims.display_name.clone()
        .or_else(|| owner_id.split('»').next().map(|s| s.to_string()));
    sqlx::query(
        "INSERT INTO server_members (server_id, user_id, role, display_name) VALUES ($1, $2, 'owner', $3)",
    )
    .bind(server_id)
    .bind(owner_id)
    .bind(&owner_display_name)
    .execute(&state.db)
    .await
    .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    sqlx::query(
        "INSERT INTO channels (server_id, name, channel_type, position) VALUES ($1, 'general', 'text', 0)",
    )
    .bind(server_id)
    .execute(&state.db)
    .await
    .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

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
    .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

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

async fn query_members(
    state: &AppState,
    server_id: Uuid,
    limit: i64,
    offset: i64,
) -> Result<(Vec<ServerMember>, bool), StatusCode> {
    let rows = sqlx::query(
        "SELECT user_id, display_name, role, joined_at FROM server_members
         WHERE server_id = $1 ORDER BY role, user_id LIMIT $2 OFFSET $3",
    )
    .bind(server_id)
    .bind(limit + 1)
    .bind(offset)
    .fetch_all(&state.db)
    .await
    .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    let has_more = rows.len() > limit as usize;
    let online = state.connections.read().await.online_beams.clone();

    let members = rows.iter().take(limit as usize).map(|row| {
        let role: String = row.get("role");
        let is_owner = role == "owner";
        let user_id: String = row.get("user_id");
        let status = if online.contains(&user_id) { "online".to_string() } else { "offline".to_string() };
        ServerMember {
            display_name: row.get("display_name"),
            user_id,
            role,
            joined_at: row.get("joined_at"),
            is_owner,
            status,
        }
    }).collect();

    Ok((members, has_more))
}

pub async fn get_server_members(
    State(state): State<AppState>,
    Path(server_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Json<Vec<ServerMember>>, StatusCode> {
    require_member(&state, &headers, server_id).await?;
    let (members, _) = query_members(&state, server_id, 1000, 0).await?;
    Ok(Json(members))
}

// ── v1 API routes (client uses https://cloud.zeeble.xyz/servers/:id as base) ──

#[derive(Debug, Serialize)]
pub struct ServerInfoResponse {
    pub name: String,
    pub about: Option<String>,
    pub public_url: String,
    pub owner_beam_identity: Option<String>,
    pub logo_attachment_id: Option<Uuid>,
    pub banner_attachment_id: Option<Uuid>,
}

pub async fn server_info(
    State(state): State<AppState>,
    Path(server_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Json<ServerInfoResponse>, StatusCode> {
    require_member(&state, &headers, server_id).await?;
    let row = sqlx::query(
        "SELECT name, about, owner_id, logo_attachment_id, banner_attachment_id
         FROM servers WHERE id = $1",
    )
    .bind(server_id)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?
    .ok_or(StatusCode::NOT_FOUND)?;

    Ok(Json(ServerInfoResponse {
        name: row.get("name"),
        about: row.get("about"),
        public_url: format!("{}/servers/{}", state.public_url.trim_end_matches('/'), server_id),
        owner_beam_identity: row.get("owner_id"),
        logo_attachment_id: row.get("logo_attachment_id"),
        banner_attachment_id: row.get("banner_attachment_id"),
    }))
}

#[derive(Debug, Deserialize)]
pub struct PatchServerSettingsRequest {
    pub name: Option<String>,
    pub about: Option<String>,
    pub logo_attachment_id: Option<String>,
    pub banner_attachment_id: Option<String>,
}

pub async fn patch_server_settings(
    State(state): State<AppState>,
    Path(server_id): Path<Uuid>,
    headers: HeaderMap,
    Json(payload): Json<PatchServerSettingsRequest>,
) -> Result<StatusCode, StatusCode> {
    let token = extract_bearer(&headers).ok_or(StatusCode::UNAUTHORIZED)?;
    let claims = validate_jwt(&token, &state.ed25519_x).map_err(|_| StatusCode::UNAUTHORIZED)?;

    let row = sqlx::query("SELECT owner_id FROM servers WHERE id = $1")
        .bind(server_id)
        .fetch_optional(&state.db)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let owner_id: String = row.get("owner_id");
    if claims.sub != owner_id {
        return Err(StatusCode::FORBIDDEN);
    }

    if let Some(name) = &payload.name {
        sqlx::query("UPDATE servers SET name = $1 WHERE id = $2")
            .bind(name)
            .bind(server_id)
            .execute(&state.db)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    }
    if let Some(about) = &payload.about {
        sqlx::query("UPDATE servers SET about = $1 WHERE id = $2")
            .bind(about)
            .bind(server_id)
            .execute(&state.db)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    }
    if let Some(logo_str) = &payload.logo_attachment_id {
        let logo_id = Uuid::parse_str(logo_str).map_err(|_| StatusCode::BAD_REQUEST)?;
        sqlx::query("UPDATE servers SET logo_attachment_id = $1 WHERE id = $2")
            .bind(logo_id)
            .bind(server_id)
            .execute(&state.db)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    }
    if let Some(banner_str) = &payload.banner_attachment_id {
        let banner_id = Uuid::parse_str(banner_str).map_err(|_| StatusCode::BAD_REQUEST)?;
        sqlx::query("UPDATE servers SET banner_attachment_id = $1 WHERE id = $2")
            .bind(banner_id)
            .bind(server_id)
            .execute(&state.db)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    }

    Ok(StatusCode::NO_CONTENT)
}

pub async fn get_server_settings(
    State(state): State<AppState>,
    Path(server_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let token = extract_bearer(&headers).ok_or(StatusCode::UNAUTHORIZED)?;
    let claims = validate_jwt(&token, &state.ed25519_x).map_err(|_| StatusCode::UNAUTHORIZED)?;

    let row = sqlx::query("SELECT name, about, owner_id FROM servers WHERE id = $1")
        .bind(server_id)
        .fetch_optional(&state.db)
        .await
        .map_err(|e| { error!("zcloud get_server_settings error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?
        .ok_or(StatusCode::NOT_FOUND)?;

    let owner_id: String = row.get("owner_id");
    if claims.sub != owner_id {
        return Err(StatusCode::FORBIDDEN);
    }

    let name: String = row.get("name");
    let about: Option<String> = row.get("about");
    let public_url = format!("{}/servers/{}", state.public_url.trim_end_matches('/'), server_id);

    Ok(Json(serde_json::json!({
        "server_name": name,
        "public_url": public_url,
        "owner_beam_identity": owner_id,
        "about": about,
        "max_message_length": 4000,
        "max_upload_size": "8MB",
        "invites_anyone_can_create": true,
        "default_invite_expiry_hours": 24,
        "default_invite_max_uses": 0,
        "allow_new_members": true,
        "logo_attachment_id": null,
        "banner_attachment_id": null,
        "require_email_verified": false,
        "require_phone_verified": false,
        "require_age_18_plus": false,
        "age_proof_methods": [],
        "allow_bots": true,
        "min_account_age_days": 0,
        "identity_whitelist": [],
        "identity_blacklist": [],
        "allowed_email_domains": [],
        "max_members": 0
    })))
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
    headers: HeaderMap,
) -> Result<Json<Vec<ApiChannel>>, StatusCode> {
    require_member(&state, &headers, server_id).await?;
    let rows = sqlx::query(
        "SELECT id, name, channel_type, position, topic
         FROM channels WHERE server_id = $1 ORDER BY position, name LIMIT 500",
    )
    .bind(server_id)
    .fetch_all(&state.db)
    .await
    .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    Ok(Json(rows.iter().map(|row| ApiChannel {
        id: row.get("id"),
        channel_type: row.get("channel_type"),
        name: row.get("name"),
        position: row.get("position"),
        topic: row.get("topic"),
    }).collect()))
}

#[derive(Debug, Deserialize)]
pub struct CreateChannelRequest {
    pub name: String,
    #[serde(rename = "type")]
    pub channel_type: Option<String>,
    pub position: Option<i32>,
    pub topic: Option<String>,
}

pub async fn create_channel_v1(
    State(state): State<AppState>,
    Path(server_id): Path<Uuid>,
    headers: HeaderMap,
    Json(payload): Json<CreateChannelRequest>,
) -> Result<(StatusCode, Json<ApiChannel>), StatusCode> {
    let token = extract_bearer(&headers).ok_or_else(|| {
        error!("create_channel_v1: missing bearer token");
        StatusCode::UNAUTHORIZED
    })?;
    let claims = validate_jwt(&token, &state.ed25519_x).map_err(|_| {
        error!("create_channel_v1: JWT validation failed for server_id={}", server_id);
        StatusCode::UNAUTHORIZED
    })?;

    let owner_row = sqlx::query("SELECT owner_id FROM servers WHERE id = $1")
        .bind(server_id)
        .fetch_optional(&state.db)
        .await
        .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?
        .ok_or(StatusCode::NOT_FOUND)?;

    let owner_id: String = owner_row.get("owner_id");
    if claims.sub != owner_id {
        error!("create_channel_v1: forbidden — token sub='{}' != owner_id='{}'", claims.sub, owner_id);
        return Err(StatusCode::FORBIDDEN);
    }
    tracing::debug!("create_channel_v1: authorized as owner '{}', creating channel '{}'", claims.sub, payload.name);

    let channel_type = payload.channel_type.unwrap_or_else(|| "text".to_string());
    let position = payload.position.unwrap_or(0);
    let topic = payload.topic.unwrap_or_default();

    let row = sqlx::query(
        "INSERT INTO channels (server_id, name, channel_type, position, topic)
         VALUES ($1, $2, $3, $4, $5)
         RETURNING id, name, channel_type, position, topic",
    )
    .bind(server_id)
    .bind(&payload.name)
    .bind(&channel_type)
    .bind(position)
    .bind(&topic)
    .fetch_one(&state.db)
    .await
    .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    Ok((StatusCode::CREATED, Json(ApiChannel {
        id: row.get("id"),
        channel_type: row.get("channel_type"),
        name: row.get("name"),
        position: row.get("position"),
        topic: row.get("topic"),
    })))
}

#[derive(Debug, Deserialize)]
pub struct UpdateChannelRequest {
    pub name: Option<String>,
    pub position: Option<i32>,
    pub topic: Option<String>,
}

pub async fn update_channel_v1(
    State(state): State<AppState>,
    Path((server_id, channel_id)): Path<(Uuid, Uuid)>,
    headers: HeaderMap,
    Json(payload): Json<UpdateChannelRequest>,
) -> Result<Json<ApiChannel>, StatusCode> {
    let token = extract_bearer(&headers).ok_or(StatusCode::UNAUTHORIZED)?;
    let claims = validate_jwt(&token, &state.ed25519_x).map_err(|_| StatusCode::UNAUTHORIZED)?;

    let owner_row = sqlx::query("SELECT owner_id FROM servers WHERE id = $1")
        .bind(server_id)
        .fetch_optional(&state.db)
        .await
        .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?
        .ok_or(StatusCode::NOT_FOUND)?;

    let owner_id: String = owner_row.get("owner_id");
    if claims.sub != owner_id {
        return Err(StatusCode::FORBIDDEN);
    }

    verify_channel(&state.db, channel_id, server_id).await?;

    if let Some(name) = &payload.name {
        sqlx::query("UPDATE channels SET name = $1 WHERE id = $2")
            .bind(name)
            .bind(channel_id)
            .execute(&state.db)
            .await
            .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;
    }
    if let Some(position) = payload.position {
        sqlx::query("UPDATE channels SET position = $1 WHERE id = $2")
            .bind(position)
            .bind(channel_id)
            .execute(&state.db)
            .await
            .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;
    }
    if let Some(topic) = &payload.topic {
        sqlx::query("UPDATE channels SET topic = $1 WHERE id = $2")
            .bind(topic)
            .bind(channel_id)
            .execute(&state.db)
            .await
            .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;
    }

    let row = sqlx::query(
        "SELECT id, name, channel_type, position, topic FROM channels WHERE id = $1",
    )
    .bind(channel_id)
    .fetch_one(&state.db)
    .await
    .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    Ok(Json(ApiChannel {
        id: row.get("id"),
        channel_type: row.get("channel_type"),
        name: row.get("name"),
        position: row.get("position"),
        topic: row.get("topic"),
    }))
}

pub async fn delete_channel_v1(
    State(state): State<AppState>,
    Path((server_id, channel_id)): Path<(Uuid, Uuid)>,
    headers: HeaderMap,
) -> Result<StatusCode, StatusCode> {
    let token = extract_bearer(&headers).ok_or(StatusCode::UNAUTHORIZED)?;
    let claims = validate_jwt(&token, &state.ed25519_x).map_err(|_| StatusCode::UNAUTHORIZED)?;

    let owner_row = sqlx::query("SELECT owner_id FROM servers WHERE id = $1")
        .bind(server_id)
        .fetch_optional(&state.db)
        .await
        .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?
        .ok_or(StatusCode::NOT_FOUND)?;

    let owner_id: String = owner_row.get("owner_id");
    if claims.sub != owner_id {
        return Err(StatusCode::FORBIDDEN);
    }

    verify_channel(&state.db, channel_id, server_id).await?;

    sqlx::query("DELETE FROM channels WHERE id = $1 AND server_id = $2")
        .bind(channel_id)
        .bind(server_id)
        .execute(&state.db)
        .await
        .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    Ok(StatusCode::NO_CONTENT)
}

pub async fn get_members_v1(
    State(state): State<AppState>,
    Path(server_id): Path<Uuid>,
    headers: HeaderMap,
    Query(q): Query<OffsetQuery>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    require_member(&state, &headers, server_id).await?;
    let limit = q.limit.unwrap_or(200).clamp(1, 1000);
    let offset = q.offset.unwrap_or(0).max(0);
    tracing::debug!("get_members_v1: server_id={} limit={} offset={}", server_id, limit, offset);
    let (members, has_more) = query_members(&state, server_id, limit, offset).await?;
    tracing::debug!("get_members_v1: returning {} members has_more={}", members.len(), has_more);
    Ok(Json(json!({ "members": members, "has_more": has_more, "offset": offset })))
}

pub async fn get_categories(
    State(state): State<AppState>,
    Path(server_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    require_member(&state, &headers, server_id).await?;
    Ok(Json(json!({ "categories": [] })))
}

pub async fn get_custom_roles(
    State(state): State<AppState>,
    Path(server_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    require_member(&state, &headers, server_id).await?;
    Ok(Json(json!([])))
}

pub async fn get_voice_rooms(
    State(state): State<AppState>,
    Path(server_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    require_member(&state, &headers, server_id).await?;
    Ok(Json(json!({ "rooms": [] })))
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
    let att_id: Option<Uuid> = row.try_get("att_id").ok().flatten();
    let attachments = if let Some(id) = att_id {
        let filename: String = row.get("att_filename");
        let mime: String = row.get("att_mime");
        let size: i64 = row.get("att_size");
        vec![serde_json::json!({
            "id": id,
            "filename": filename,
            "content_type": mime,
            "size": size
        })]
    } else {
        vec![]
    };
    ApiMessage {
        id: row.get("id"),
        channel_id: row.get("channel_id"),
        beam_identity: row.get("beam_identity"),
        content: row.get("content"),
        title: row.get("title"),
        reply_to: row.get("reply_to"),
        created_at: row.get("created_at"),
        edited_at: row.get("edited_at"),
        attachments,
    }
}

async fn verify_channel(db: &PgPool, channel_id: Uuid, server_id: Uuid) -> Result<(), StatusCode> {
    let valid = sqlx::query(
        "SELECT id FROM channels WHERE id = $1 AND server_id = $2",
    )
    .bind(channel_id)
    .bind(server_id)
    .fetch_optional(db)
    .await
    .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;
    valid.map(|_| ()).ok_or(StatusCode::NOT_FOUND)
}

pub async fn get_messages(
    State(state): State<AppState>,
    Path((server_id, channel_id)): Path<(Uuid, Uuid)>,
    headers: HeaderMap,
    Query(q): Query<MessageQuery>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    require_member(&state, &headers, server_id).await?;
    verify_channel(&state.db, channel_id, server_id).await?;

    let limit = q.limit.unwrap_or(50).clamp(1, 100);
    let fetch = limit + 1;

    let rows = match q.before {
        Some(before_id) => sqlx::query(
            "SELECT m.id, m.channel_id, m.beam_identity, m.content, m.title, m.reply_to, m.created_at, m.edited_at,
                    a.id AS att_id, a.filename AS att_filename, a.mime_type AS att_mime, a.file_size AS att_size
             FROM messages m
             LEFT JOIN attachments a ON a.message_id = m.id
             WHERE m.channel_id = $1
               AND m.created_at < (SELECT created_at FROM messages WHERE id = $2)
             ORDER BY m.created_at DESC LIMIT $3",
        )
        .bind(channel_id)
        .bind(before_id)
        .bind(fetch)
        .fetch_all(&state.db)
        .await,
        None => sqlx::query(
            "SELECT m.id, m.channel_id, m.beam_identity, m.content, m.title, m.reply_to, m.created_at, m.edited_at,
                    a.id AS att_id, a.filename AS att_filename, a.mime_type AS att_mime, a.file_size AS att_size
             FROM messages m
             LEFT JOIN attachments a ON a.message_id = m.id
             WHERE m.channel_id = $1 ORDER BY m.created_at DESC LIMIT $2",
        )
        .bind(channel_id)
        .bind(fetch)
        .fetch_all(&state.db)
        .await,
    }.map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    let has_more = rows.len() > limit as usize;
    let mut messages: Vec<ApiMessage> = rows.iter().take(limit as usize).map(row_to_message).collect();
    messages.reverse();
    Ok(Json(json!({ "messages": messages, "has_more": has_more })))
}

pub async fn get_board_posts(
    State(state): State<AppState>,
    Path((server_id, channel_id)): Path<(Uuid, Uuid)>,
    headers: HeaderMap,
    Query(q): Query<OffsetQuery>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    require_member(&state, &headers, server_id).await?;
    verify_channel(&state.db, channel_id, server_id).await?;

    let limit = q.limit.unwrap_or(50).clamp(1, 100);
    let offset = q.offset.unwrap_or(0).max(0);

    let rows = sqlx::query(
        "SELECT m.id, m.channel_id, m.beam_identity, m.content, m.title, m.reply_to, m.created_at, m.edited_at,
                a.id AS att_id, a.filename AS att_filename, a.mime_type AS att_mime, a.file_size AS att_size
         FROM messages m
         LEFT JOIN attachments a ON a.message_id = m.id
         WHERE m.channel_id = $1 AND m.reply_to IS NULL
         ORDER BY m.created_at DESC LIMIT $2 OFFSET $3",
    )
    .bind(channel_id)
    .bind(limit + 1)
    .bind(offset)
    .fetch_all(&state.db)
    .await
    .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    let has_more = rows.len() > limit as usize;
    let posts: Vec<ApiMessage> = rows.iter().take(limit as usize).map(row_to_message).collect();
    Ok(Json(json!({ "posts": posts, "has_more": has_more, "offset": offset })))
}

pub async fn get_post_replies(
    State(state): State<AppState>,
    Path((server_id, channel_id, post_id)): Path<(Uuid, Uuid, Uuid)>,
    headers: HeaderMap,
    Query(q): Query<OffsetQuery>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    require_member(&state, &headers, server_id).await?;
    verify_channel(&state.db, channel_id, server_id).await?;

    let limit = q.limit.unwrap_or(100).clamp(1, 200);
    let offset = q.offset.unwrap_or(0).max(0);

    let rows = sqlx::query(
        "SELECT m.id, m.channel_id, m.beam_identity, m.content, m.title, m.reply_to, m.created_at, m.edited_at,
                a.id AS att_id, a.filename AS att_filename, a.mime_type AS att_mime, a.file_size AS att_size
         FROM messages m
         LEFT JOIN attachments a ON a.message_id = m.id
         WHERE m.channel_id = $1 AND m.reply_to = $2
         ORDER BY m.created_at ASC LIMIT $3 OFFSET $4",
    )
    .bind(channel_id)
    .bind(post_id)
    .bind(limit + 1)
    .bind(offset)
    .fetch_all(&state.db)
    .await
    .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    let has_more = rows.len() > limit as usize;
    let replies: Vec<ApiMessage> = rows.iter().take(limit as usize).map(row_to_message).collect();
    Ok(Json(json!({ "replies": replies, "has_more": has_more, "offset": offset })))
}

pub async fn server_health(
    State(state): State<AppState>,
    Path(server_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    require_auth_sync(&headers, &state.ed25519_x)?;
    let exists = sqlx::query("SELECT id FROM servers WHERE id = $1")
        .bind(server_id)
        .fetch_optional(&state.db)
        .await
        .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

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
    #[serde(default)]
    display_name: Option<String>,
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

fn require_auth_sync(headers: &HeaderMap, ed25519_x: &str) -> Result<Claims, StatusCode> {
    let token = extract_bearer(headers).ok_or(StatusCode::UNAUTHORIZED)?;
    validate_jwt(&token, ed25519_x).map_err(|_| StatusCode::UNAUTHORIZED)
}

async fn require_member(state: &AppState, headers: &HeaderMap, server_id: Uuid) -> Result<Claims, StatusCode> {
    let claims = require_auth_sync(headers, &state.ed25519_x)?;
    sqlx::query("SELECT 1 FROM server_members WHERE server_id = $1 AND user_id = $2")
        .bind(server_id)
        .bind(&claims.sub)
        .fetch_optional(&state.db)
        .await
        .map_err(|e| { error!("zcloud require_member: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?
        .ok_or(StatusCode::FORBIDDEN)?;
    Ok(claims)
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
        .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?
        .ok_or(StatusCode::NOT_FOUND)?;

    const ALLOWED_MIME_TYPES: &[&str] = &[
        "image/jpeg", "image/png", "image/gif", "image/webp",
        "image/avif", "image/heic",
        "video/mp4", "video/webm", "video/quicktime",
        "audio/mpeg", "audio/ogg", "audio/wav", "audio/webm",
        "application/pdf", "text/plain",
    ];
    const MAX_FILE_SIZE: i64 = 100 * 1024 * 1024; // 100 MB (matches body limit)

    while let Some(field) = multipart.next_field().await.map_err(|_| StatusCode::BAD_REQUEST)? {
        let name = field.name().unwrap_or("").to_string();
        if name != "file0" { continue; }

        let filename = field.file_name().unwrap_or("upload").to_string();
        let mime = field.content_type().unwrap_or("application/octet-stream").to_string();

        if !ALLOWED_MIME_TYPES.contains(&mime.as_str()) {
            return Err(StatusCode::UNSUPPORTED_MEDIA_TYPE);
        }

        let data = field.bytes().await.map_err(|_| StatusCode::BAD_REQUEST)?;
        let size = data.len() as i64;
        if size > MAX_FILE_SIZE {
            return Err(StatusCode::PAYLOAD_TOO_LARGE);
        }

        let row = sqlx::query(
            "INSERT INTO attachments (server_id, filename, mime_type, file_size, file_data, uploaded_by)
             VALUES ($1, $2, $3, $4, $5, $6) RETURNING id",
        )
        .bind(server_id)
        .bind(&filename)
        .bind(&mime)
        .bind(size)
        .bind(data.as_ref())
        .bind(&claims.sub)
        .fetch_one(&state.db)
        .await
        .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

        let id: Uuid = row.get("id");
        return Ok(Json(json!({ "attachments": [{ "attachment_id": id }] })));
    }

    Err(StatusCode::BAD_REQUEST)
}

// ── Message edit / delete ─────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct EditMessageRequest {
    pub content: String,
}

pub async fn edit_message(
    State(state): State<AppState>,
    Path((server_id, message_id)): Path<(Uuid, Uuid)>,
    headers: HeaderMap,
    Json(payload): Json<EditMessageRequest>,
) -> Result<StatusCode, StatusCode> {
    let token = extract_bearer(&headers).ok_or(StatusCode::UNAUTHORIZED)?;
    let claims = validate_jwt(&token, &state.ed25519_x).map_err(|_| StatusCode::UNAUTHORIZED)?;

    let msg_row = sqlx::query(
        "SELECT m.channel_id, m.beam_identity FROM messages m
         JOIN channels c ON c.id = m.channel_id
         WHERE m.id = $1 AND c.server_id = $2",
    )
    .bind(message_id)
    .bind(server_id)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| { error!("zcloud edit_message lookup: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?
    .ok_or(StatusCode::NOT_FOUND)?;

    let channel_id: Uuid = msg_row.get("channel_id");
    let author: String = msg_row.get("beam_identity");

    if claims.sub != author {
        return Err(StatusCode::FORBIDDEN);
    }

    let old_content: String = sqlx::query_scalar("SELECT content FROM messages WHERE id = $1")
        .bind(message_id)
        .fetch_one(&state.db)
        .await
        .map_err(|e| { error!("zcloud edit_message fetch old: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    let now = chrono::Utc::now();

    sqlx::query(
        "INSERT INTO message_edit_history (message_id, content, edited_by, edited_at)
         VALUES ($1, $2, $3, $4)",
    )
    .bind(message_id)
    .bind(&old_content)
    .bind(&claims.sub)
    .bind(now)
    .execute(&state.db)
    .await
    .map_err(|e| { error!("zcloud edit_message history: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    sqlx::query("UPDATE messages SET content = $1, edited_at = $2 WHERE id = $3")
        .bind(&payload.content)
        .bind(now)
        .bind(message_id)
        .execute(&state.db)
        .await
        .map_err(|e| { error!("zcloud edit_message: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    let broadcast = serde_json::json!({
        "type": "message_edited",
        "id": message_id,
        "channel_id": channel_id,
        "content": payload.content,
        "edited_at": now.to_rfc3339(),
    }).to_string();

    let conns = state.connections.read().await;
    if let Some(subs) = conns.channel_subs.get(&(server_id, channel_id)) {
        for uid in subs {
            if let Some(sender) = conns.users.get(uid) {
                let _ = sender.send(broadcast.clone());
            }
        }
    }

    Ok(StatusCode::NO_CONTENT)
}

pub async fn get_message_history(
    State(state): State<AppState>,
    Path((server_id, message_id)): Path<(Uuid, Uuid)>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let token = extract_bearer(&headers).ok_or(StatusCode::UNAUTHORIZED)?;
    validate_jwt(&token, &state.ed25519_x).map_err(|_| StatusCode::UNAUTHORIZED)?;

    // Verify the message belongs to this server
    sqlx::query(
        "SELECT 1 FROM messages m JOIN channels c ON c.id = m.channel_id
         WHERE m.id = $1 AND c.server_id = $2",
    )
    .bind(message_id)
    .bind(server_id)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| { error!("zcloud get_message_history: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?
    .ok_or(StatusCode::NOT_FOUND)?;

    let rows = sqlx::query(
        "SELECT content, edited_by, edited_at FROM message_edit_history
         WHERE message_id = $1 ORDER BY edited_at ASC",
    )
    .bind(message_id)
    .fetch_all(&state.db)
    .await
    .map_err(|e| { error!("zcloud get_message_history fetch: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    let history: Vec<serde_json::Value> = rows.iter().map(|r| {
        let edited_at: chrono::DateTime<chrono::Utc> = r.get("edited_at");
        serde_json::json!({
            "content": r.get::<String, _>("content"),
            "edited_by": r.get::<String, _>("edited_by"),
            "edited_at": edited_at.timestamp_millis(),
        })
    }).collect();

    Ok(Json(json!({ "history": history })))
}

// ── Message delete ────────────────────────────────────────────────────────────

pub async fn delete_message(
    State(state): State<AppState>,
    Path((server_id, message_id)): Path<(Uuid, Uuid)>,
    headers: HeaderMap,
) -> Result<StatusCode, StatusCode> {
    let token = extract_bearer(&headers).ok_or(StatusCode::UNAUTHORIZED)?;
    let claims = validate_jwt(&token, &state.ed25519_x).map_err(|_| StatusCode::UNAUTHORIZED)?;

    let owner_row = sqlx::query("SELECT owner_id FROM servers WHERE id = $1")
        .bind(server_id)
        .fetch_optional(&state.db)
        .await
        .map_err(|e| { error!("zcloud delete_message server lookup: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?
        .ok_or(StatusCode::NOT_FOUND)?;
    let owner_id: String = owner_row.get("owner_id");

    let msg_row = sqlx::query(
        "SELECT m.channel_id, m.beam_identity FROM messages m
         JOIN channels c ON c.id = m.channel_id
         WHERE m.id = $1 AND c.server_id = $2",
    )
    .bind(message_id)
    .bind(server_id)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| { error!("zcloud delete_message lookup: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?
    .ok_or(StatusCode::NOT_FOUND)?;

    let channel_id: Uuid = msg_row.get("channel_id");
    let author: String = msg_row.get("beam_identity");

    if claims.sub != author && claims.sub != owner_id {
        return Err(StatusCode::FORBIDDEN);
    }

    sqlx::query("DELETE FROM messages WHERE id = $1")
        .bind(message_id)
        .execute(&state.db)
        .await
        .map_err(|e| { error!("zcloud delete_message: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    let broadcast = serde_json::json!({
        "type": "message_deleted",
        "id": message_id,
        "channel_id": channel_id,
    }).to_string();

    let conns = state.connections.read().await;
    if let Some(subs) = conns.channel_subs.get(&(server_id, channel_id)) {
        for uid in subs {
            if let Some(sender) = conns.users.get(uid) {
                let _ = sender.send(broadcast.clone());
            }
        }
    }

    Ok(StatusCode::NO_CONTENT)
}

// ── Invites ───────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct InviteInfo {
    pub code: String,
    pub created_by: String,
    pub use_count: i64,
    pub max_uses: Option<i64>,
    pub expires_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
pub struct CreateInviteRequest {
    pub max_uses: Option<i64>,
    pub expires_in_secs: Option<i64>,
}

fn generate_invite_code() -> String {
    Uuid::new_v4().to_string().replace('-', "")[..10].to_string()
}

pub async fn list_invites(
    State(state): State<AppState>,
    Path(server_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let token = extract_bearer(&headers).ok_or(StatusCode::UNAUTHORIZED)?;
    let claims = validate_jwt(&token, &state.ed25519_x).map_err(|_| StatusCode::UNAUTHORIZED)?;

    let owner_row = sqlx::query("SELECT owner_id FROM servers WHERE id = $1")
        .bind(server_id)
        .fetch_optional(&state.db)
        .await
        .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?
        .ok_or(StatusCode::NOT_FOUND)?;

    let owner_id: String = owner_row.get("owner_id");
    if claims.sub != owner_id {
        return Err(StatusCode::FORBIDDEN);
    }

    let rows = sqlx::query(
        "SELECT code, created_by, use_count, max_uses, expires_at, created_at
         FROM invites WHERE server_id = $1 ORDER BY created_at DESC",
    )
    .bind(server_id)
    .fetch_all(&state.db)
    .await
    .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    let invites: Vec<InviteInfo> = rows.iter().map(|r| InviteInfo {
        code: r.get("code"),
        created_by: r.get("created_by"),
        use_count: r.get::<i32, _>("use_count") as i64,
        max_uses: r.get::<Option<i32>, _>("max_uses").map(|v| v as i64),
        expires_at: r.get("expires_at"),
        created_at: r.get("created_at"),
    }).collect();

    Ok(Json(json!({ "invites": invites })))
}

pub async fn create_invite(
    State(state): State<AppState>,
    Path(server_id): Path<Uuid>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let token = extract_bearer(&headers).ok_or(StatusCode::UNAUTHORIZED)?;
    let claims = validate_jwt(&token, &state.ed25519_x).map_err(|_| StatusCode::UNAUTHORIZED)?;

    sqlx::query("SELECT id FROM servers WHERE id = $1")
        .bind(server_id)
        .fetch_optional(&state.db)
        .await
        .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?
        .ok_or(StatusCode::NOT_FOUND)?;

    let opts: CreateInviteRequest = if body.is_empty() {
        CreateInviteRequest { max_uses: None, expires_in_secs: None }
    } else {
        serde_json::from_slice(&body).unwrap_or(CreateInviteRequest { max_uses: None, expires_in_secs: None })
    };

    let expires_at: Option<DateTime<Utc>> = opts.expires_in_secs.map(|secs| {
        Utc::now() + chrono::Duration::seconds(secs)
    });

    let code = generate_invite_code();
    let max_uses = opts.max_uses.map(|v| v as i32);

    sqlx::query(
        "INSERT INTO invites (code, server_id, created_by, max_uses, expires_at)
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(&code)
    .bind(server_id)
    .bind(&claims.sub)
    .bind(max_uses)
    .bind(expires_at)
    .execute(&state.db)
    .await
    .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    let server_url = format!("{}/servers/{}", state.public_url.trim_end_matches('/'), server_id);
    let invite_url = format!("{}/join/{}", server_url, code);

    Ok(Json(json!({ "code": code, "url": invite_url })))
}

pub async fn validate_invite(
    State(state): State<AppState>,
    Path((server_id, code)): Path<(Uuid, String)>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let row = sqlx::query(
        "SELECT code, created_by, use_count, max_uses, expires_at
         FROM invites WHERE code = $1 AND server_id = $2",
    )
    .bind(&code)
    .bind(server_id)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?
    .ok_or(StatusCode::NOT_FOUND)?;

    let expires_at: Option<DateTime<Utc>> = row.get("expires_at");
    if let Some(exp) = expires_at {
        if exp < Utc::now() {
            return Err(StatusCode::GONE);
        }
    }

    let use_count: i32 = row.get("use_count");
    let max_uses: Option<i32> = row.get("max_uses");
    if let Some(max) = max_uses {
        if use_count >= max {
            return Err(StatusCode::GONE);
        }
    }

    let server_row = sqlx::query("SELECT name FROM servers WHERE id = $1")
        .bind(server_id)
        .fetch_one(&state.db)
        .await
        .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    Ok(Json(json!({
        "code": row.get::<String, _>("code"),
        "server_name": server_row.get::<String, _>("name"),
        "created_by": row.get::<String, _>("created_by"),
        "use_count": use_count,
        "max_uses": max_uses,
    })))
}

pub async fn delete_invite(
    State(state): State<AppState>,
    Path((server_id, code)): Path<(Uuid, String)>,
    headers: HeaderMap,
) -> Result<StatusCode, StatusCode> {
    let token = extract_bearer(&headers).ok_or(StatusCode::UNAUTHORIZED)?;
    let claims = validate_jwt(&token, &state.ed25519_x).map_err(|_| StatusCode::UNAUTHORIZED)?;

    let owner_row = sqlx::query("SELECT owner_id FROM servers WHERE id = $1")
        .bind(server_id)
        .fetch_optional(&state.db)
        .await
        .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?
        .ok_or(StatusCode::NOT_FOUND)?;

    let owner_id: String = owner_row.get("owner_id");
    if claims.sub != owner_id {
        return Err(StatusCode::FORBIDDEN);
    }

    let result = sqlx::query("DELETE FROM invites WHERE code = $1 AND server_id = $2")
        .bind(&code)
        .bind(server_id)
        .execute(&state.db)
        .await
        .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    if result.rows_affected() == 0 {
        Err(StatusCode::NOT_FOUND)
    } else {
        Ok(StatusCode::NO_CONTENT)
    }
}

pub async fn redeem_invite(
    State(state): State<AppState>,
    Path((server_id, code)): Path<(Uuid, String)>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let token = extract_bearer(&headers).ok_or(StatusCode::UNAUTHORIZED)?;
    let claims = validate_jwt(&token, &state.ed25519_x).map_err(|_| StatusCode::UNAUTHORIZED)?;

    let row = sqlx::query(
        "SELECT use_count, max_uses, expires_at FROM invites WHERE code = $1 AND server_id = $2",
    )
    .bind(&code)
    .bind(server_id)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?
    .ok_or(StatusCode::NOT_FOUND)?;

    let expires_at: Option<DateTime<Utc>> = row.get("expires_at");
    if let Some(exp) = expires_at {
        if exp < Utc::now() {
            return Ok(Json(json!({ "error": "Invite has expired" })));
        }
    }

    let use_count: i32 = row.get("use_count");
    let max_uses: Option<i32> = row.get("max_uses");
    if let Some(max) = max_uses {
        if use_count >= max {
            return Ok(Json(json!({ "error": "Invite has reached its maximum uses" })));
        }
    }

    let already_member = sqlx::query(
        "SELECT 1 FROM server_members WHERE server_id = $1 AND user_id = $2",
    )
    .bind(server_id)
    .bind(&claims.sub)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    if already_member.is_none() {
        sqlx::query(
            "INSERT INTO server_members (server_id, user_id, role, display_name) VALUES ($1, $2, 'member', $3)",
        )
        .bind(server_id)
        .bind(&claims.sub)
        .bind(&claims.display_name)
        .execute(&state.db)
        .await
        .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;
    } else if claims.display_name.is_some() {
        let _ = sqlx::query(
            "UPDATE server_members SET display_name = $1 WHERE server_id = $2 AND user_id = $3 AND display_name IS NULL",
        )
        .bind(&claims.display_name)
        .bind(server_id)
        .bind(&claims.sub)
        .execute(&state.db)
        .await;
    }

    sqlx::query("UPDATE invites SET use_count = use_count + 1 WHERE code = $1 AND server_id = $2")
        .bind(&code)
        .bind(server_id)
        .execute(&state.db)
        .await
        .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    Ok(Json(json!({ "ok": true })))
}

pub async fn get_attachment(
    State(state): State<crate::AppState>,
    Path((server_id, attach_id_str)): Path<(Uuid, String)>,
    Query(q): Query<TokenQuery>,
    headers: HeaderMap,
) -> Result<Response<Body>, StatusCode> {
    let attach_id = Uuid::parse_str(&attach_id_str).map_err(|_| StatusCode::NOT_FOUND)?;

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
    .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?
    .ok_or(StatusCode::NOT_FOUND)?;

    let mime: String = row.get("mime_type");
    let filename: String = row.get("filename");
    let data: Vec<u8> = row.get("file_data");

    let inline = mime.starts_with("image/") || mime.starts_with("video/") || mime.starts_with("audio/");
    let disposition = if inline {
        "inline".to_string()
    } else {
        format!("attachment; filename=\"{}\"", filename.replace('"', "\\\""))
    };

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, &mime)
        .header(header::CONTENT_DISPOSITION, disposition)
        .header(header::CACHE_CONTROL, "private, max-age=86400")
        .body(Body::from(data))
        .unwrap())
}
