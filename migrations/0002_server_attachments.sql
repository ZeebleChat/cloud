ALTER TABLE servers
    ADD COLUMN IF NOT EXISTS logo_attachment_id   UUID REFERENCES attachments(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS banner_attachment_id UUID REFERENCES attachments(id) ON DELETE SET NULL;
