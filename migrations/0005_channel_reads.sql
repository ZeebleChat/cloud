CREATE TABLE IF NOT EXISTS channel_reads (
    server_id    UUID NOT NULL REFERENCES servers(id) ON DELETE CASCADE,
    channel_id   UUID NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    user_id      TEXT NOT NULL,
    last_read_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (server_id, channel_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_channel_reads_user ON channel_reads(server_id, user_id);
