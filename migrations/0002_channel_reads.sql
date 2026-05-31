CREATE TABLE IF NOT EXISTS channel_reads (
    user_id    TEXT NOT NULL,
    channel_id UUID NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    read_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, channel_id)
);

CREATE INDEX IF NOT EXISTS idx_channel_reads_channel ON channel_reads(channel_id);
