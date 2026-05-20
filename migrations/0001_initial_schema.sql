CREATE TABLE IF NOT EXISTS servers (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name       VARCHAR(100) NOT NULL,
    owner_id   TEXT NOT NULL,
    about      TEXT,
    icon_url   TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS channels (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    server_id    UUID NOT NULL REFERENCES servers(id) ON DELETE CASCADE,
    name         VARCHAR(100) NOT NULL,
    channel_type TEXT NOT NULL DEFAULT 'text',
    position     INT NOT NULL DEFAULT 0,
    topic        TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS server_members (
    server_id    UUID NOT NULL REFERENCES servers(id) ON DELETE CASCADE,
    user_id      TEXT NOT NULL,
    role         TEXT NOT NULL DEFAULT 'member',
    display_name TEXT,
    joined_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (server_id, user_id)
);

CREATE TABLE IF NOT EXISTS messages (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    channel_id    UUID NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    beam_identity TEXT NOT NULL,
    content       TEXT NOT NULL,
    title         TEXT,
    reply_to      UUID REFERENCES messages(id) ON DELETE SET NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    edited_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_messages_channel  ON messages(channel_id, created_at);
CREATE INDEX IF NOT EXISTS idx_messages_reply_to ON messages(reply_to);

CREATE TABLE IF NOT EXISTS invites (
    code       TEXT PRIMARY KEY,
    server_id  UUID NOT NULL REFERENCES servers(id) ON DELETE CASCADE,
    created_by TEXT NOT NULL,
    max_uses   INTEGER,
    use_count  INTEGER NOT NULL DEFAULT 0,
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS attachments (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    server_id   UUID NOT NULL REFERENCES servers(id) ON DELETE CASCADE,
    filename    TEXT NOT NULL,
    mime_type   TEXT NOT NULL,
    file_size   BIGINT NOT NULL,
    file_data   BYTEA NOT NULL,
    uploaded_by TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
