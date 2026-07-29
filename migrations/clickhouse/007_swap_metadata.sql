ALTER TABLE base.dex_pool_swaps
    ADD COLUMN IF NOT EXISTS factory_address LowCardinality(String) AFTER pool_address,
    ADD COLUMN IF NOT EXISTS protocol LowCardinality(String) AFTER factory_address,
    ADD COLUMN IF NOT EXISTS protocol_version LowCardinality(String) AFTER protocol,
    ADD COLUMN IF NOT EXISTS token0_address LowCardinality(String) AFTER protocol_family,
    ADD COLUMN IF NOT EXISTS token1_address LowCardinality(String) AFTER token0_address,
    ADD COLUMN IF NOT EXISTS token0_symbol LowCardinality(String) AFTER token1_address,
    ADD COLUMN IF NOT EXISTS token1_symbol LowCardinality(String) AFTER token0_symbol,
    ADD COLUMN IF NOT EXISTS token0_decimals UInt8 AFTER token1_symbol,
    ADD COLUMN IF NOT EXISTS token1_decimals UInt8 AFTER token0_decimals,
    ADD COLUMN IF NOT EXISTS metadata_status LowCardinality(String) AFTER token1_decimals;
