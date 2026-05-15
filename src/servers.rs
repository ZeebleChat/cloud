use axum::{
    Json,
    extract::{State, Path, Multipart, Query},
    http::{StatusCode, header, HeaderMap},
    response::Response,
    body::Body,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;
use chrono::{DateTime, Utc};
use tracing::error;

use crate::AppState;


#[derive(Debug, Deserialize)]
pub struct CreateServerRequest {
    pub name: String,
    pub about: Option<String>,
    pub owner_id: String,
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
) -> StatusCode {
    match state.db
        .execute("DELETE FROM servers WHERE id = $1", &[&server_id])
        .await
    {
        Ok(_) => StatusCode::OK,
        Err(e) => { error!("zcloud delete_server error: {e}"); StatusCode::INTERNAL_SERVER_ERROR }
    }
}

pub async fn create_server(
    State(state): State<AppState>,
    Json(payload): Json<CreateServerRequest>,
) -> Result<(StatusCode, Json<ServerResponse>), StatusCode> {
    let row = state.db
        .query_one(
            "INSERT INTO servers (name, owner_id, about) VALUES ($1, $2, $3) RETURNING *",
            &[&payload.name, &payload.owner_id, &payload.about],
        )
        .await
        .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    let server_id: Uuid = row.get("id");

    state.db
        .execute(
            "INSERT INTO server_members (server_id, user_id, role) VALUES ($1, $2, 'owner')",
            &[&server_id, &payload.owner_id],
        )
        .await
        .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    state.db
        .execute(
            "INSERT INTO channels (server_id, name, channel_type, position) VALUES ($1, 'general', 'text', 0)",
            &[&server_id],
        )
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
    let rows = state.db
        .query(
            "SELECT id, server_id, name, channel_type, position, topic, created_at
             FROM channels WHERE server_id = $1 ORDER BY position, name",
            &[&server_id],
        )
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

pub async fn get_server_members(
    State(state): State<AppState>,
    Path(server_id): Path<Uuid>,
) -> Result<Json<Vec<ServerMember>>, StatusCode> {
    let rows = state.db
        .query(
            "SELECT user_id, display_name, role, joined_at FROM server_members WHERE server_id = $1",
            &[&server_id],
        )
        .await
        .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    let online = state.connections.read().await.online_beams.clone();

    Ok(Json(rows.iter().map(|row| {
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
    }).collect()))
}

// ── v1 API routes (client uses https://cloud.zeeble.xyz/servers/:id as base) ──

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
    let row = state.db
        .query_opt("SELECT name, about FROM servers WHERE id = $1", &[&server_id])
        .await
        .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?
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
    let rows = state.db
        .query(
            "SELECT id, name, channel_type, position, topic
             FROM channels WHERE server_id = $1 ORDER BY position, name",
            &[&server_id],
        )
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

    let owner_row = state.db
        .query_opt("SELECT owner_id FROM servers WHERE id = $1", &[&server_id])
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

    let row = state.db
        .query_one(
            "INSERT INTO channels (server_id, name, channel_type, position, topic)
             VALUES ($1, $2, $3, $4, $5)
             RETURNING id, name, channel_type, position, topic",
            &[&server_id, &payload.name, &channel_type, &position, &topic],
        )
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

    let owner_row = state.db
        .query_opt("SELECT owner_id FROM servers WHERE id = $1", &[&server_id])
        .await
        .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?
        .ok_or(StatusCode::NOT_FOUND)?;

    let owner_id: String = owner_row.get("owner_id");
    if claims.sub != owner_id {
        return Err(StatusCode::FORBIDDEN);
    }

    verify_channel(&state.db, channel_id, server_id).await?;

    if let Some(name) = &payload.name {
        state.db
            .execute("UPDATE channels SET name = $1 WHERE id = $2", &[name, &channel_id])
            .await
            .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;
    }
    if let Some(position) = payload.position {
        state.db
            .execute("UPDATE channels SET position = $1 WHERE id = $2", &[&position, &channel_id])
            .await
            .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;
    }
    if let Some(topic) = &payload.topic {
        state.db
            .execute("UPDATE channels SET topic = $1 WHERE id = $2", &[topic, &channel_id])
            .await
            .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;
    }

    let row = state.db
        .query_one(
            "SELECT id, name, channel_type, position, topic FROM channels WHERE id = $1",
            &[&channel_id],
        )
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

    let owner_row = state.db
        .query_opt("SELECT owner_id FROM servers WHERE id = $1", &[&server_id])
        .await
        .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?
        .ok_or(StatusCode::NOT_FOUND)?;

    let owner_id: String = owner_row.get("owner_id");
    if claims.sub != owner_id {
        return Err(StatusCode::FORBIDDEN);
    }

    verify_channel(&state.db, channel_id, server_id).await?;

    state.db
        .execute("DELETE FROM channels WHERE id = $1 AND server_id = $2", &[&channel_id, &server_id])
        .await
        .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    Ok(StatusCode::NO_CONTENT)
}

pub async fn get_members_v1(
    State(state): State<AppState>,
    Path(server_id): Path<Uuid>,
) -> Result<Json<Vec<ServerMember>>, StatusCode> {
    tracing::debug!("get_members_v1: server_id={}", server_id);
    let result = get_server_members(State(state), Path(server_id)).await;
    if let Ok(ref members) = result {
        tracing::debug!("get_members_v1: returning {} members", members.len());
    }
    result
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
    Json(json!({ "rooms": [] }))
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

fn row_to_message(row: &tokio_postgres::Row) -> ApiMessage {
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

async fn verify_channel(db: &tokio_postgres::Client, channel_id: Uuid, server_id: Uuid) -> Result<(), StatusCode> {
    let valid = db
        .query_opt(
            "SELECT id FROM channels WHERE id = $1 AND server_id = $2",
            &[&channel_id, &server_id],
        )
        .await
        .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;
    valid.map(|_| ()).ok_or(StatusCode::NOT_FOUND)
}

pub async fn get_messages(
    State(state): State<AppState>,
    Path((server_id, channel_id)): Path<(Uuid, Uuid)>,
) -> Result<Json<Vec<ApiMessage>>, StatusCode> {
    verify_channel(&state.db, channel_id, server_id).await?;

    let rows = state.db
        .query(
            "SELECT id, channel_id, beam_identity, content, title, reply_to, created_at, edited_at
             FROM messages WHERE channel_id = $1 ORDER BY created_at ASC LIMIT 200",
            &[&channel_id],
        )
        .await
        .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    Ok(Json(rows.iter().map(row_to_message).collect()))
}

pub async fn get_board_posts(
    State(state): State<AppState>,
    Path((server_id, channel_id)): Path<(Uuid, Uuid)>,
) -> Result<Json<Vec<ApiMessage>>, StatusCode> {
    verify_channel(&state.db, channel_id, server_id).await?;

    let rows = state.db
        .query(
            "SELECT id, channel_id, beam_identity, content, title, reply_to, created_at, edited_at
             FROM messages
             WHERE channel_id = $1 AND reply_to IS NULL
             ORDER BY created_at DESC LIMIT 100",
            &[&channel_id],
        )
        .await
        .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    Ok(Json(rows.iter().map(row_to_message).collect()))
}

pub async fn get_post_replies(
    State(state): State<AppState>,
    Path((server_id, channel_id, post_id)): Path<(Uuid, Uuid, Uuid)>,
) -> Result<Json<Vec<ApiMessage>>, StatusCode> {
    verify_channel(&state.db, channel_id, server_id).await?;

    let rows = state.db
        .query(
            "SELECT id, channel_id, beam_identity, content, title, reply_to, created_at, edited_at
             FROM messages
             WHERE channel_id = $1 AND reply_to = $2
             ORDER BY created_at ASC LIMIT 500",
            &[&channel_id, &post_id],
        )
        .await
        .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    Ok(Json(rows.iter().map(row_to_message).collect()))
}

pub async fn server_health(
    State(state): State<AppState>,
    Path(server_id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let exists = state.db
        .query_opt("SELECT id FROM servers WHERE id = $1", &[&server_id])
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

pub async fn upload_file(
    State(state): State<crate::AppState>,
    Path(server_id): Path<Uuid>,
    headers: HeaderMap,
    mut multipart: Multipart,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let token = extract_bearer(&headers).ok_or(StatusCode::UNAUTHORIZED)?;
    let claims = validate_jwt(&token, &state.ed25519_x).map_err(|_| StatusCode::UNAUTHORIZED)?;

    // Verify server exists
    state.db
        .query_opt("SELECT id FROM servers WHERE id = $1", &[&server_id])
        .await
        .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?
        .ok_or(StatusCode::NOT_FOUND)?;

    while let Some(field) = multipart.next_field().await.map_err(|_| StatusCode::BAD_REQUEST)? {
        let name = field.name().unwrap_or("").to_string();
        if name != "file0" { continue; }

        let filename = field.file_name().unwrap_or("upload").to_string();
        let mime = field.content_type().unwrap_or("application/octet-stream").to_string();
        let data = field.bytes().await.map_err(|_| StatusCode::BAD_REQUEST)?;
        let size = data.len() as i64;

        let row = state.db
            .query_one(
                "INSERT INTO attachments (server_id, filename, mime_type, file_size, file_data, uploaded_by)
                 VALUES ($1, $2, $3, $4, $5, $6) RETURNING id",
                &[&server_id, &filename, &mime, &size, &data.as_ref(), &claims.sub],
            )
            .await
            .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

        let id: Uuid = row.get("id");
        return Ok(Json(json!({ "attachments": [{ "attachment_id": id }] })));
    }

    Err(StatusCode::BAD_REQUEST)
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

    let owner_row = state.db
        .query_opt(
            "SELECT owner_id FROM servers WHERE id = $1",
            &[&server_id],
        )
        .await
        .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?
        .ok_or(StatusCode::NOT_FOUND)?;

    let owner_id: String = owner_row.get("owner_id");
    if claims.sub != owner_id {
        return Err(StatusCode::FORBIDDEN);
    }

    let rows = state.db
        .query(
            "SELECT code, created_by, use_count, max_uses, expires_at, created_at
             FROM invites WHERE server_id = $1 ORDER BY created_at DESC",
            &[&server_id],
        )
        .await
        .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    let invites: Vec<InviteInfo> = rows.iter().map(|r| InviteInfo {
        code: r.get("code"),
        created_by: r.get("created_by"),
        use_count: r.get::<_, i32>("use_count") as i64,
        max_uses: r.get::<_, Option<i32>>("max_uses").map(|v| v as i64),
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

    state.db
        .query_opt("SELECT id FROM servers WHERE id = $1", &[&server_id])
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
    let expires_at_ref: Option<&DateTime<Utc>> = expires_at.as_ref();

    state.db
        .execute(
            "INSERT INTO invites (code, server_id, created_by, max_uses, expires_at)
             VALUES ($1, $2, $3, $4, $5)",
            &[&code, &server_id, &claims.sub, &max_uses, &expires_at_ref],
        )
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
    let row = state.db
        .query_opt(
            "SELECT code, created_by, use_count, max_uses, expires_at
             FROM invites WHERE code = $1 AND server_id = $2",
            &[&code, &server_id],
        )
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

    let server_row = state.db
        .query_one("SELECT name FROM servers WHERE id = $1", &[&server_id])
        .await
        .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    Ok(Json(json!({
        "code": row.get::<_, String>("code"),
        "server_name": server_row.get::<_, String>("name"),
        "created_by": row.get::<_, String>("created_by"),
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

    let owner_row = state.db
        .query_opt("SELECT owner_id FROM servers WHERE id = $1", &[&server_id])
        .await
        .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?
        .ok_or(StatusCode::NOT_FOUND)?;

    let owner_id: String = owner_row.get("owner_id");
    if claims.sub != owner_id {
        return Err(StatusCode::FORBIDDEN);
    }

    let deleted = state.db
        .execute(
            "DELETE FROM invites WHERE code = $1 AND server_id = $2",
            &[&code, &server_id],
        )
        .await
        .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    if deleted == 0 {
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

    let row = state.db
        .query_opt(
            "SELECT use_count, max_uses, expires_at FROM invites WHERE code = $1 AND server_id = $2",
            &[&code, &server_id],
        )
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

    let already_member = state.db
        .query_opt(
            "SELECT 1 FROM server_members WHERE server_id = $1 AND user_id = $2",
            &[&server_id, &claims.sub],
        )
        .await
        .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    if already_member.is_none() {
        state.db
            .execute(
                "INSERT INTO server_members (server_id, user_id, role, display_name) VALUES ($1, $2, 'member', $3)",
                &[&server_id, &claims.sub, &claims.display_name],
            )
            .await
            .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;
    } else if claims.display_name.is_some() {
        // Update display_name if the member already exists and we have a fresher one
        let _ = state.db
            .execute(
                "UPDATE server_members SET display_name = $1 WHERE server_id = $2 AND user_id = $3 AND display_name IS NULL",
                &[&claims.display_name, &server_id, &claims.sub],
            )
            .await;
    }

    state.db
        .execute(
            "UPDATE invites SET use_count = use_count + 1 WHERE code = $1 AND server_id = $2",
            &[&code, &server_id],
        )
        .await
        .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?;

    Ok(Json(json!({ "ok": true })))
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

    let row = state.db
        .query_opt(
            "SELECT filename, mime_type, file_data FROM attachments WHERE id = $1 AND server_id = $2",
            &[&attach_id, &server_id],
        )
        .await
        .map_err(|e| { error!("zcloud error: {e}"); StatusCode::INTERNAL_SERVER_ERROR })?
        .ok_or(StatusCode::NOT_FOUND)?;

    let mime: String = row.get("mime_type");
    let data: Vec<u8> = row.get("file_data");

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, mime)
        .body(Body::from(data))
        .unwrap())
}
