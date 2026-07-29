ALTER TABLE base.wallet_smart_score_snapshots
    ADD COLUMN IF NOT EXISTS source_updated_at_ms UInt64 AFTER source_updated_at;
