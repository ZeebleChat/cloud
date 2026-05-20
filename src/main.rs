use axum::{
    routing::{delete, get, patch, post},
    Router,
    extract::{State, WebSocketUpgrade, DefaultBodyLimit},
    response::IntoResponse,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tokio::sync::RwLock;
use tower_http::cors::{Any, CorsLayer, AllowOrigin};
use tower_http::trace::TraceLayer;
use tracing::{info, error};
use uuid::Uuid;

mod servers;
mod ws;

#[derive(Clone)]
pub struct AppState {
    pub db: PgPool,
    pub public_url: String,
    pub ed25519_x: String,
    pub connections: Arc<RwLock<Connections>>,
}

#[derive(Default)]
pub struct Connections {
    pub users: HashMap<String, tokio::sync::mpsc::UnboundedSender<String>>,
    pub channel_subs: HashMap<(Uuid, Uuid), HashSet<String>>,
    pub online_beams: HashSet<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("zcloud=debug".parse()?)
        )
        .init();

    let database_url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set");
    let public_url = std::env::var("ZCLOUD_PUBLIC_URL")
        .unwrap_or_else(|_| "https://cloud.zeeble.xyz".to_string());
    let cors_origin = std::env::var("CORS_ORIGIN")
        .unwrap_or_else(|_| "http://localhost:5173".to_string());
    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8003);
    let zbeam_url = std::env::var("ZBEAM_URL")
        .unwrap_or_else(|_| "http://zbeam:8001".to_string());

    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&database_url)
        .await?;

    sqlx::migrate!().run(&pool).await?;
    info!("Database migrations applied");

    let ed25519_x = fetch_ed25519_x(&zbeam_url).await;

    let state = AppState {
        db: pool,
        public_url,
        ed25519_x,
        connections: Arc::new(RwLock::new(Connections::default())),
    };

    let origins: Vec<axum::http::HeaderValue> = cors_origin
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();

    let cors = CorsLayer::new()
        .allow_origin(AllowOrigin::list(origins))
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .route("/health", get(health))
        // Cloud server management
        .route("/servers", post(servers::create_server))
        .route("/servers/:id", delete(servers::delete_server))
        // Per-server v1 API (client base URL = https://cloud.zeeble.xyz/servers/:id)
        .route("/servers/:id/health",          get(servers::server_health))
        .route("/servers/:id/v1/server/info",     get(servers::server_info))
        .route("/servers/:id/v1/server/settings", patch(servers::patch_server_settings))
        .route("/servers/:id/v1/channels",     get(servers::get_channels_v1).post(servers::create_channel_v1))
        .route("/servers/:id/v1/channels/:channel_id", patch(servers::update_channel_v1).delete(servers::delete_channel_v1))
        .route("/servers/:id/v1/members",      get(servers::get_members_v1))
        .route("/servers/:id/v1/categories",   get(servers::get_categories))
        .route("/servers/:id/v1/custom_roles", get(servers::get_custom_roles))
        .route("/servers/:id/v1/voice/rooms",  get(servers::get_voice_rooms))
        .route("/servers/:id/v1/ws",           get(ws_upgrade_handler))
        .route("/servers/:id/v1/channels/:channel_id/messages", get(servers::get_messages))
        .route("/servers/:id/v1/messages/:message_id",          delete(servers::delete_message).patch(servers::edit_message))
        .route("/servers/:id/v1/messages/:message_id/history",  get(servers::get_message_history))
        .route("/servers/:id/v1/channels/:channel_id/posts",    get(servers::get_board_posts))
        .route("/servers/:id/v1/channels/:channel_id/posts/:post_id/replies", get(servers::get_post_replies))
        .route("/servers/:id/v1/upload",                        post(servers::upload_file).layer(DefaultBodyLimit::max(100 * 1024 * 1024)))
        .route("/servers/:id/v1/attachments/:attach_id",        get(servers::get_attachment))
        // Invites
        .route("/servers/:id/v1/invites",                       get(servers::list_invites).post(servers::create_invite))
        .route("/servers/:id/v1/invites/:code",                 get(servers::validate_invite).delete(servers::delete_invite))
        .route("/servers/:id/v1/invites/:code/redeem",          post(servers::redeem_invite))
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let addr = format!("0.0.0.0:{port}");
    spawn_heartbeat("cloud");

    info!("zcloud listening on {addr}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn ws_upgrade_handler(
    ws: WebSocketUpgrade,
    axum::extract::Path(server_id): axum::extract::Path<Uuid>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| ws::handle_connection(socket, state, server_id))
}

async fn health() -> &'static str {
    "ok"
}

fn spawn_heartbeat(key: &'static str) {
    let url = std::env::var("ZSTATUS_URL")
        .unwrap_or_else(|_| "http://zstatus:8004".to_string());
    let secret = std::env::var("ZSTATUS_SECRET").unwrap_or_default();

    tokio::spawn(async move {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .expect("heartbeat client");
        let endpoint = format!("{}/heartbeat", url);
        loop {
            let body = serde_json::json!({ "key": key, "ok": true });
            let mut req = client.post(&endpoint).json(&body);
            if !secret.is_empty() {
                req = req.header("Authorization", format!("Bearer {}", secret));
            }
            if let Err(e) = req.send().await {
                tracing::warn!("[heartbeat] zstatus unreachable: {e}");
            }
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        }
    });
}

async fn fetch_ed25519_x(zbeam_url: &str) -> String {
    let url = format!("{}/.well-known/jwks.json", zbeam_url.trim_end_matches('/'));
    let client = reqwest::Client::new();
    let mut delay = std::time::Duration::from_secs(1);
    loop {
        match client.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => {
                if let Ok(jwks) = resp.json::<serde_json::Value>().await {
                    if let Some(x) = jwks["keys"][0]["x"].as_str() {
                        info!("Fetched zbeam Ed25519 public key from JWKS");
                        return x.to_string();
                    }
                }
            }
            _ => {}
        }
        error!("Could not fetch zbeam JWKS, retrying in {}s...", delay.as_secs());
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(std::time::Duration::from_secs(16));
    }
}
