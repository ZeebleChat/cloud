CREATE TABLE IF NOT EXISTS message_edit_history (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    message_id  UUID NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    content     TEXT NOT NULL,
    edited_by   TEXT NOT NULL,
    edited_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_edit_history_message ON message_edit_history(message_id, edited_at);
