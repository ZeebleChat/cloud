# Zcloud — Multi-Tenant Server Management

**Zcloud** (Zcloud service) provides a cloud control plane for managing multiple Zeeble chat servers in a multi-tenant SaaS model. It allows users to create, configure, and access their own dedicated Zeeble servers from a single unified interface.

## Use Cases

- **SaaS Offering**: Users sign up, spin up their own Zeeble instance, and connect via a shared cloud front-end.
- **Enterprise Deployments**: Organization-specific chat servers managed centrally.
- **Developer Sandboxes**: Automated provisioning of test servers.

## Architecture

```
┌────────────┐
│   Client   │  talks to →  https://cloud.zeeble.xyz
└────────────┘
       │
       ▼
┌─────────────────────────────────────────────┐
│            Zcloud (this service)           │
│  — stores server metadata in PostgreSQL   │
│  — validates JWTs (via Zbeam)             │
│  — proxies API/WS to backend PhaseLinks   │
└─────────────────────────────────────────────┘
       │
       ├──→ Server 1 (PhaseLink instance A)
       ├──→ Server 2 (PhaseLink instance B)
       └──→ Server N ...
```

Each "server" in Zcloud represents a separate Zeeble backend (a PhaseLink instance). The cloud service:
- Authenticates users via Zbeam JWTs.
- Stores server ownership and connection details in its own PostgreSQL database.
- Proxies requests to the appropriate backend server (`/servers/:id/v1/...`).
- Provides a unified WebSocket endpoint that upgrades to the backend's WebSocket.

## Tech Stack

- **Language**: Rust + Axum + Tokio
- **Database**: PostgreSQL (via `tokio-postgres`)
- **Auth**: JWT validation against Zbeam's Ed25519 public key (JWKS)
- **CORS**: Configurable per-origin allowlist

## Quick Start

### Prerequisites

- Docker + Docker Compose
- Zbeam (Auth service) running (default: `http://localhost:8001`)
- PostgreSQL (or use included Docker Compose)

### Using Docker Compose

```bash
cd cloud
cp .env.example .env  # Optional: adjust variables
docker compose up -d
```

This starts:
- **PostgreSQL** on port `5433`
- **Zcloud** on port `8003`

Check health:
```bash
curl http://localhost:8003/health
# "ok"
```

### Running from Source

```bash
cd cloud
# Set DATABASE_URL, ZCLOUD_PUBLIC_URL, ZBEAM_URL, CORS_ORIGIN, PORT
cargo run --release
```

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATABASE_URL` | Yes | — | PostgreSQL connection string (e.g., `postgresql://zcloud_user:zcloud_password@localhost:5433/zcloud_db`) |
| `ZCLOUD_PUBLIC_URL` | No | `https://cloud.zeeble.xyz` | Public URL clients use to reach this cloud service. |
| `CORS_ORIGIN` | No | `http://localhost:5173` | Comma-separated allowed origins for CORS (frontend URL). |
| `PORT` | No | `8003` | Port to bind. |
| `ZBEAM_URL` | No | `http://localhost:8001` | Auth service base URL (for JWKS fetch). |
| `LIVEKIT_URL` | No | `http://localhost:7880` | Default LiveKit server URL for generated tokens (per-server can override). |
| `LIVEKIT_API_KEY` | No | `zeeble-dev-key` | LiveKit API key. |
| `LIVEKIT_API_SECRET` | No | `change-me-livekit-secret-min-32-chars` | LiveKit API secret. |
| `RUST_LOG` | No | `zcloud=info,tower_http=debug` | Log filter. |

## API Overview

### Public Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Health check |
| `GET` | `/livekit/token` (POST) | Generate LiveKit token for a server's voice room (legacy endpoint) |

### Cloud Management (Requires Bearer Token)

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/servers` | Create a new server (tenant). Body: `{ "name": "My Server", "owner_id": "uuid", "icon_url?" }`. Returns server record with internal `id` and connection details. |
| `GET` | `/servers` | List all servers the authenticated user owns/manages. *(Not implemented yet)* |
| `DELETE` | `/servers/:id` | Delete a server (owner only). *(Not implemented yet)* |

### Per-Server Proxy Endpoints

All per-server endpoints are prefixed with `/servers/:id` and proxy to the backend PhaseLink instance.

**Example**: To list channels on server `abc123`:
```
GET /servers/abc123/v1/channels
Authorization: Bearer <jwt>
```

The cloud service:
1. Validates the JWT (signature, expiry).
2. Looks up the server record by `:id`.
3. Verifies the user is a member of that server (optional; can be enforced by backend).
4. Forwards the request to `backend_url/v1/...` (backend_url stored in DB).
5. Returns the response.

#### Supported Backend Endpoints

Currently proxied:
- `GET /servers/:id/health` — health of the backend
- `GET /servers/:id/v1/server/info`
- `GET /servers/:id/v1/channels`
- `GET /servers/:id/v1/members`
- `GET /servers/:id/v1/categories`
- `GET /servers/:id/v1/custom_roles`
- `GET /servers/:id/v1/voice/rooms`
- `GET /servers/:id/v1/ws` → WebSocket upgrade
- `GET /servers/:id/v1/channels/:channel_id/messages`
- `GET /servers/:id/v1/channels/:channel_id/posts`
- `GET /servers/:id/v1/channels/:channel_id/posts/:post_id/replies`
- `POST /servers/:id/v1/upload`
- `GET /servers/:id/v1/attachments/:attach_id`

**Note**: As the backend API evolves, this proxy layer may need to be updated. Alternatively, consider using OpenAPI specs to generate proxy code.

## WebSocket Proxy

Connect to: `ws://cloud.zeeble.xyz/servers/:id/v1/ws?server_id=:id`

The cloud service:
- Performs JWT validation (same as REST).
- Establishes a WebSocket connection to the backend PhaseLink.
- Pipes messages bidirectionally.

Clients use the same WebSocket protocol as described in the Server README.

## Voice Tokens

When a client requests a voice token, it typically calls:
```
GET /servers/:id/v1/voice/token?room=<room_name>
```

The cloud forwards this to the backend, which generates a LiveKit token using its configured `LIVEKIT_API_KEY` and `LIVEKIT_API_SECRET`. Alternatively, the cloud itself could generate tokens directly using `LIVEKIT_URL` and `LIVEKIT_API_KEY/SECRET` (see `livekit/token.rs`).

## CORS

Set `CORS_ORIGIN` to your cloud frontend's origin (e.g., `https://app.zeeble.xyz`). Multiple origins can be comma-separated.

## Database Schema

Migrations in `src/migrations/`. Main tables:

- `servers` — server metadata (owner, name, icon, internal `backend_url`)
- `server_members` — user membership with role and nickname
- `channels` — channel definitions (cached or synced from backend)
- `channel_members` — user membership in channels (for DMs)
- `channel_messages` — message history (may be duplicated from backend; ideally, clients talk directly to backend for chat history, cloud only proxies).

**Note**: Some data is cached in the cloud DB to avoid repeated backend calls (e.g., channels list). This cache should be invalidated when the backend changes (consider webhooks or periodic sync).

## Deployment

### Docker

```bash
docker build -t zcloud .
docker run -p 8003:8003 -e DATABASE_URL=... zcloud
```

Or use the provided `docker-compose.yml`.

### Production Checklist

- Use strong PostgreSQL password.
- Set `CORS_ORIGIN` to exact production frontend URL(s).
- Set `ZCLOUD_PUBLIC_URL` to your cloud domain.
- Ensure `ZBEAM_URL` points to the production Auth service.
- Configure LiveKit credentials; ensure `LIVEKIT_URL` is reachable from backend servers.
- Run behind a reverse proxy (Caddy, Nginx) with TLS termination.
- Enable firewall rules: only expose 8003 (or 443 via proxy).
- Back up PostgreSQL regularly.
- Use a managed Redis for session storage if needed (currently not used heavily in cloud, but could be).

## Multi-Tenancy Model

Zcloud uses **schema-per-tenant**? No, it uses a **shared database with tenant_id** on each table. The `servers` table owns the tenant boundary. All queries filter by `server_id`. The backend PhaseLink instances are completely separate (each tenant could even run on a different host/container). The cloud merely stores metadata and proxies.

## Extending the Proxy

To add a new backend endpoint:
1. Add route in `src/main.rs` under `Router::new()`: `.route("/servers/:id/v1/your-endpoint", get(your_handler))`.
2. Implement `your_handler` in `src/servers/mod.rs`:
   - Extract `server_id` path param.
   - Look up server record from DB.
   - Build backend URL: `format!("{}/v1/your-endpoint", server.backend_url)`.
   - Forward query params, headers, body.
   - Return the response (status, headers, body).
3. Update documentation.

Consider using a generic proxy function to reduce boilerplate.

## Status Monitoring

The standalone **Zstatus** service monitors Zcloud along with Auth and DM. See `status/`.

## Testing

No automated tests yet. Manual testing steps:
1. Create a server via `POST /servers` (as an authenticated user).
2. Verify server record in DB.
3. Call `GET /servers/:id/health` — should return `{"status":"ok"}` if backend is reachable.
4. Call `GET /servers/:id/v1/server/info` — should proxy to backend.
5. Connect WebSocket: `ws://localhost:8003/servers/:id/v1/ws?server_id=:id` and ensure auth handshake works.

## License

MIT
