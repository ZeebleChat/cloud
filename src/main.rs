use axum::{
    routing::{get, post},
    Router,
    extract::{State, WebSocketUpgrade},
    response::IntoResponse,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_postgres::NoTls;
use tower_http::cors::{Any, CorsLayer, AllowOrigin};
use tower_http::trace::TraceLayer;
use tracing::{info, error};
use uuid::Uuid;

mod servers;
mod voice;
mod ws;

#[derive(Clone)]
pub struct AppState {
    pub db: Arc<tokio_postgres::Client>,
    pub public_url: String,
    pub ed25519_x: String,
    pub connections: Arc<RwLock<Connections>>,
}

#[derive(Default)]
pub struct Connections {
    pub users: HashMap<String, tokio::sync::mpsc::UnboundedSender<String>>,
    pub channel_subs: HashMap<(Uuid, Uuid), HashSet<String>>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("zcloud=info".parse()?)
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

    let (db_client, db_connection) = tokio_postgres::connect(&database_url, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = db_connection.await {
            error!("Database connection error: {}", e);
        }
    });

    init_database(&db_client).await?;
    info!("Database initialized");

    let ed25519_x = fetch_ed25519_x(&zbeam_url).await;

    let state = AppState {
        db: Arc::new(db_client),
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
        // Per-server v1 API (client base URL = https://cloud.zeeble.xyz/servers/:id)
        .route("/servers/:id/health",          get(servers::server_health))
        .route("/servers/:id/v1/server/info",  get(servers::server_info))
        .route("/servers/:id/v1/channels",     get(servers::get_channels_v1))
        .route("/servers/:id/v1/members",      get(servers::get_members_v1))
        .route("/servers/:id/v1/categories",   get(servers::get_categories))
        .route("/servers/:id/v1/custom_roles", get(servers::get_custom_roles))
        .route("/servers/:id/v1/voice/rooms",  get(servers::get_voice_rooms))
        .route("/servers/:id/v1/ws",           get(ws_upgrade_handler))
        .route("/servers/:id/v1/channels/:channel_id/messages", get(servers::get_messages))
        .route("/servers/:id/v1/channels/:channel_id/posts",    get(servers::get_board_posts))
        .route("/servers/:id/v1/channels/:channel_id/posts/:post_id/replies", get(servers::get_post_replies))
        .route("/servers/:id/v1/upload",                        post(servers::upload_file))
        .route("/servers/:id/v1/attachments/:attach_id",        get(servers::get_attachment))
        // Legacy token endpoint
        .route("/livekit/token", post(voice::get_livekit_token))
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let addr = format!("0.0.0.0:{port}");
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

async fn init_database(db: &tokio_postgres::Client) -> Result<(), tokio_postgres::Error> {
    db.batch_execute("
        CREATE TABLE IF NOT EXISTS servers (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name VARCHAR(100) NOT NULL,
            owner_id TEXT NOT NULL,
            about TEXT,
            icon_url TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS channels (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            server_id UUID NOT NULL REFERENCES servers(id) ON DELETE CASCADE,
            name VARCHAR(100) NOT NULL,
            channel_type TEXT NOT NULL DEFAULT 'text',
            position INT NOT NULL DEFAULT 0,
            topic TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS server_members (
            server_id UUID NOT NULL REFERENCES servers(id) ON DELETE CASCADE,
            user_id TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'member',
            joined_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (server_id, user_id)
        );

        CREATE TABLE IF NOT EXISTS messages (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            channel_id UUID NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
            beam_identity TEXT NOT NULL,
            content TEXT NOT NULL,
            title TEXT,
            reply_to UUID REFERENCES messages(id) ON DELETE SET NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            edited_at TIMESTAMPTZ
        );

        ALTER TABLE messages ADD COLUMN IF NOT EXISTS title TEXT;
        ALTER TABLE messages ADD COLUMN IF NOT EXISTS reply_to UUID REFERENCES messages(id) ON DELETE SET NULL;

        CREATE INDEX IF NOT EXISTS idx_messages_channel ON messages(channel_id, created_at);
        CREATE INDEX IF NOT EXISTS idx_messages_reply_to ON messages(reply_to);

        CREATE TABLE IF NOT EXISTS attachments (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            server_id UUID NOT NULL REFERENCES servers(id) ON DELETE CASCADE,
            filename TEXT NOT NULL,
            mime_type TEXT NOT NULL,
            file_size BIGINT NOT NULL,
            file_data BYTEA NOT NULL,
            uploaded_by TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    ").await
}
