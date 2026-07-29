ALTER TABLE base.dex_swap_valuations
    ADD COLUMN IF NOT EXISTS valuation_version LowCardinality(String) AFTER valuation_status;
