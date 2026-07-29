ALTER TABLE alert_outbox
    ADD COLUMN IF NOT EXISTS locked_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS locked_by TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS last_error TEXT NOT NULL DEFAULT '';

ALTER TABLE alert_outbox
    DROP CONSTRAINT IF EXISTS alert_outbox_status_check;

ALTER TABLE alert_outbox
    ADD CONSTRAINT alert_outbox_status_check
        CHECK (status IN (
            'pending', 'processing', 'delivered', 'failed', 'dead_letter'
        ));

CREATE INDEX IF NOT EXISTS alert_outbox_processing_lease_idx
    ON alert_outbox (locked_at)
    WHERE status = 'processing';
