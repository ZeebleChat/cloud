use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{error, info, warn};
use uuid::Uuid;

const WS_RATE_LIMIT_MSGS: u32 = 20;
const WS_RATE_LIMIT_WINDOW: Duration = Duration::from_secs(5);

use crate::AppState;

#[derive(Debug, Deserialize, Serialize)]
struct Claims {
    sub: String,
    uid: String,
    display_name: Option<String>,
    exp: u64,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
enum InMsg {
    #[serde(rename = "auth")]
    Auth { token: String },
    #[serde(rename = "activate")]
    Activate,
    #[serde(rename = "join")]
    Join { channel_id: String },
    #[serde(rename = "message")]
    Message { channel_id: String, content: String, title: Option<String>, reply_to: Option<String>, attachment_id: Option<String> },
    #[serde(rename = "ping")]
    Ping,
}

pub async fn handle_connection(socket: WebSocket, state: AppState, server_id: Uuid) {
    let (mut ws_sender, mut receiver) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();

    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if ws_sender.send(Message::Text(msg)).await.is_err() {
                break;
            }
        }
    });

    let mut authenticated = false;
    let mut user_id: Option<String> = None;       // uid (UUID) — used as connection key
    let mut beam_identity: Option<String> = None; // sub — used for display / message authorship
    let mut subscribed_channels: Vec<Uuid> = Vec::new();

    let mut rate_msg_count: u32 = 0;
    let mut rate_window_start = Instant::now();

    while let Some(msg) = receiver.next().await {
        let text = match msg {
            Ok(Message::Text(t)) => t,
            Ok(Message::Close(_)) | Err(_) => break,
            _ => continue,
        };

        let parsed = match serde_json::from_str::<InMsg>(&text) {
            Ok(m) => m,
            Err(e) => {
                warn!("Unparseable WS message: {e}");
                continue;
            }
        };

        match parsed {
            InMsg::Auth { token } => {
                match validate_token(&token, &state.ed25519_x) {
                    Ok(claims) => {
                        authenticated = true;
                        let uid = claims.uid.clone();
                        let sub = claims.sub.clone();
                        let dname = claims.display_name.clone();
                        user_id = Some(uid.clone());
                        beam_identity = Some(sub.clone());

                        {
                            let mut conns = state.connections.write().await;
                            conns.users.insert(uid.clone(), tx.clone());
                            conns.online_beams.insert(sub.clone());
                        }

                        if let Some(ref dn) = dname {
                            let _ = sqlx::query(
                                "UPDATE server_members SET display_name = $1 WHERE server_id = $2 AND user_id = $3",
                            )
                            .bind(dn)
                            .bind(server_id)
                            .bind(&sub)
                            .execute(&state.db)
                            .await;
                        }

                        let _ = tx.send(r#"{"type":"auth_ok"}"#.to_string());
                        info!("zcloud WS auth ok: {uid} ({sub}) on server {server_id}");
                        broadcast_member_list(&state, server_id).await;
                    }
                    Err(e) => {
                        let _ = tx.send(
                            serde_json::json!({ "type": "auth_error", "message": e }).to_string()
                        );
                    }
                }
            }

            InMsg::Activate { .. } => {
                let _ = tx.send(r#"{"type":"activated"}"#.to_string());
            }

            InMsg::Join { channel_id, .. } => {
                if authenticated {
                    if let (Ok(cid), Some(uid)) = (Uuid::parse_str(&channel_id), &user_id) {
                        let key = (server_id, cid);
                        state.connections.write().await
                            .channel_subs.entry(key).or_default().insert(uid.clone());
                        subscribed_channels.push(cid);
                    }
                }
            }

            InMsg::Message { channel_id, content, title, reply_to, attachment_id } => {
                if !authenticated { continue; }

                let now = Instant::now();
                if now.duration_since(rate_window_start) >= WS_RATE_LIMIT_WINDOW {
                    rate_msg_count = 0;
                    rate_window_start = now;
                }
                rate_msg_count += 1;
                if rate_msg_count > WS_RATE_LIMIT_MSGS {
                    let _ = tx.send(r#"{"type":"error","message":"rate limited"}"#.to_string());
                    continue;
                }
                let (Some(uid), Some(bident), Ok(cid)) = (
                    user_id.clone(),
                    beam_identity.clone(),
                    Uuid::parse_str(&channel_id),
                ) else {
                    continue;
                };

                let reply_to_uuid: Option<Uuid> = reply_to
                    .as_deref()
                    .and_then(|s| Uuid::parse_str(s).ok());

                let row = sqlx::query(
                    "INSERT INTO messages (channel_id, beam_identity, content, title, reply_to)
                     VALUES ($1, $2, $3, $4, $5)
                     RETURNING id, created_at",
                )
                .bind(cid)
                .bind(&bident)
                .bind(&content)
                .bind(&title)
                .bind(reply_to_uuid)
                .fetch_one(&state.db)
                .await;

                match row {
                  Err(e) => { error!("DB insert message: {e}"); continue; }
                  Ok(row) => {
                    let msg_id: uuid::Uuid = row.get("id");
                    let created_at: chrono::DateTime<chrono::Utc> = row.get("created_at");
                    let _ = uid;

                    // Link the uploaded attachment to this message and fetch its metadata
                    let att_value: Option<serde_json::Value> = if let Some(att_id_str) = &attachment_id {
                        if let Ok(att_id) = Uuid::parse_str(att_id_str) {
                            match sqlx::query(
                                "UPDATE attachments SET message_id = $1
                                 WHERE id = $2 AND server_id = $3
                                 RETURNING id, filename, mime_type, file_size",
                            )
                            .bind(msg_id)
                            .bind(att_id)
                            .bind(server_id)
                            .fetch_optional(&state.db)
                            .await
                            {
                                Ok(Some(r)) => {
                                    let id: Uuid = r.get("id");
                                    let filename: String = r.get("filename");
                                    let mime: String = r.get("mime_type");
                                    let size: i64 = r.get("file_size");
                                    Some(serde_json::json!({
                                        "id": id,
                                        "filename": filename,
                                        "content_type": mime,
                                        "size": size
                                    }))
                                }
                                Ok(None) => { warn!("attachment {att_id_str} not found for server {server_id}"); None }
                                Err(e) => { error!("link attachment: {e}"); None }
                            }
                        } else {
                            warn!("invalid attachment_id: {att_id_str}");
                            None
                        }
                    } else {
                        None
                    };

                    let attachments: Vec<serde_json::Value> = att_value.into_iter().collect();

                    let broadcast = serde_json::json!({
                        "type": "message",
                        "id": msg_id,
                        "channel_id": cid,
                        "beam_identity": bident,
                        "content": content,
                        "title": title,
                        "reply_to": reply_to_uuid,
                        "created_at": created_at.to_rfc3339(),
                        "attachments": attachments
                    }).to_string();

                    let key = (server_id, cid);
                    let conns = state.connections.read().await;
                    if let Some(subs) = conns.channel_subs.get(&key) {
                        for sub_uid in subs {
                            if let Some(sender) = conns.users.get(sub_uid) {
                                let _ = sender.send(broadcast.clone());
                            }
                        }
                    }
                  }
                }
            }

            InMsg::Ping => {
                let _ = tx.send(r#"{"type":"pong"}"#.to_string());
            }
        }
    }

    // Cleanup on disconnect
    if let Some(uid) = user_id {
        {
            let mut conns = state.connections.write().await;
            conns.users.remove(&uid);
            if let Some(ref beam) = beam_identity {
                conns.online_beams.remove(beam);
            }
            for cid in &subscribed_channels {
                if let Some(subs) = conns.channel_subs.get_mut(&(server_id, *cid)) {
                    subs.remove(&uid);
                }
            }
        }
        broadcast_member_list(&state, server_id).await;
    }
}

async fn broadcast_member_list(state: &AppState, server_id: Uuid) {
    let rows = match sqlx::query(
        "SELECT user_id, display_name, role FROM server_members WHERE server_id = $1 ORDER BY role, user_id",
    )
    .bind(server_id)
    .fetch_all(&state.db)
    .await
    {
        Ok(r) => r,
        Err(e) => { error!("broadcast_member_list query: {e}"); return; }
    };

    let online = state.connections.read().await.online_beams.clone();

    let members: Vec<serde_json::Value> = rows.iter().map(|row| {
        let user_id: String = row.get("user_id");
        let role: String = row.get("role");
        let is_owner = role == "owner";
        let status = if online.contains(&user_id) { "online" } else { "offline" };
        serde_json::json!({
            "beam_identity": user_id,
            "display_name": row.get::<Option<String>, _>("display_name"),
            "role": role,
            "is_owner": is_owner,
            "status": status,
        })
    }).collect();

    let msg = serde_json::json!({ "type": "member", "members": members }).to_string();

    let conns = state.connections.read().await;
    let mut seen = std::collections::HashSet::new();
    for ((srv_id, _), subs) in &conns.channel_subs {
        if *srv_id == server_id {
            for uid in subs {
                if seen.insert(uid.clone()) {
                    if let Some(sender) = conns.users.get(uid) {
                        let _ = sender.send(msg.clone());
                    }
                }
            }
        }
    }
}

fn validate_token(token: &str, ed25519_x: &str) -> Result<Claims, String> {
    use jsonwebtoken::{decode, DecodingKey, Validation, Algorithm};

    let decoding_key = DecodingKey::from_ed_components(ed25519_x)
        .map_err(|e| e.to_string())?;

    let mut validation = Validation::new(Algorithm::EdDSA);
    validation.validate_aud = false;

    decode::<Claims>(token, &decoding_key, &validation)
        .map(|data| data.claims)
        .map_err(|e| e.to_string())
}
