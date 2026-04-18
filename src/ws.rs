use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{info, warn};
use uuid::Uuid;

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
    Message { channel_id: String, content: String, title: Option<String>, reply_to: Option<String> },
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
                        user_id = Some(uid.clone());
                        beam_identity = Some(sub);

                        state.connections.write().await
                            .users.insert(uid.clone(), tx.clone());

                        let _ = tx.send(r#"{"type":"auth_ok"}"#.to_string());
                        info!("zcloud WS auth ok: {uid} on server {server_id}");
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

            InMsg::Message { channel_id, content, title, reply_to } => {
                if !authenticated { continue; }
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

                let row = state.db.query_one(
                    "INSERT INTO messages (channel_id, beam_identity, content, title, reply_to)
                     VALUES ($1, $2, $3, $4, $5)
                     RETURNING id, created_at",
                    &[&cid, &bident, &content, &title, &reply_to_uuid],
                ).await;

                if let Ok(row) = row {
                    let msg_id: uuid::Uuid = row.get("id");
                    let created_at: chrono::DateTime<chrono::Utc> = row.get("created_at");
                    let _ = uid;
                    let broadcast = serde_json::json!({
                        "type": "message",
                        "id": msg_id,
                        "channel_id": cid,
                        "beam_identity": bident,
                        "content": content,
                        "title": title,
                        "reply_to": reply_to_uuid,
                        "created_at": created_at.to_rfc3339(),
                        "attachments": []
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

            InMsg::Ping => {
                let _ = tx.send(r#"{"type":"pong"}"#.to_string());
            }
        }
    }

    // Cleanup on disconnect
    if let Some(uid) = user_id {
        let mut conns = state.connections.write().await;
        conns.users.remove(&uid);
        for cid in subscribed_channels {
            if let Some(subs) = conns.channel_subs.get_mut(&(server_id, cid)) {
                subs.remove(&uid);
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
