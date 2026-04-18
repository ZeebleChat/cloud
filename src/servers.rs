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
    pub user_id: String,
    pub role: String,
    pub joined_at: DateTime<Utc>,
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
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let server_id: Uuid = row.get("id");

    state.db
        .execute(
            "INSERT INTO server_members (server_id, user_id, role) VALUES ($1, $2, 'owner')",
            &[&server_id, &payload.owner_id],
        )
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    state.db
        .execute(
            "INSERT INTO channels (server_id, name, channel_type, position) VALUES ($1, 'general', 'text', 0)",
            &[&server_id],
        )
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

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
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

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
            "SELECT user_id, role, joined_at FROM server_members WHERE server_id = $1",
            &[&server_id],
        )
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(rows.iter().map(|row| ServerMember {
        user_id: row.get("user_id"),
        role: row.get("role"),
        joined_at: row.get("joined_at"),
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
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
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
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(rows.iter().map(|row| ApiChannel {
        id: row.get("id"),
        channel_type: row.get("channel_type"),
        name: row.get("name"),
        position: row.get("position"),
        topic: row.get("topic"),
    }).collect()))
}

pub async fn get_members_v1(
    State(state): State<AppState>,
    Path(server_id): Path<Uuid>,
) -> Result<Json<Vec<ServerMember>>, StatusCode> {
    get_server_members(State(state), Path(server_id)).await
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
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
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
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

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
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

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
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(rows.iter().map(row_to_message).collect()))
}

pub async fn server_health(
    State(state): State<AppState>,
    Path(server_id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let exists = state.db
        .query_opt("SELECT id FROM servers WHERE id = $1", &[&server_id])
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

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
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
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
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

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

    let row = state.db
        .query_opt(
            "SELECT filename, mime_type, file_data FROM attachments WHERE id = $1 AND server_id = $2",
            &[&attach_id, &server_id],
        )
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let mime: String = row.get("mime_type");
    let data: Vec<u8> = row.get("file_data");

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, mime)
        .body(Body::from(data))
        .unwrap())
}
