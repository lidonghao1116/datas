CREATE TABLE IF NOT EXISTS base.token_dev_relationships
(
    analysis_version       LowCardinality(String),
    chain_id               UInt64,
    token_address          String,
    primary_deployer       String,
    related_address        String,
    address_kind           LowCardinality(String),
    relation_type          LowCardinality(String),
    direction              LowCardinality(String),
    evidence_count         UInt64,
    confidence_raw         String,
    first_observed_at      DateTime64(3, 'UTC'),
    last_observed_at       DateTime64(3, 'UTC'),
    sample_transaction_hashes Array(String),
    evidence_source        LowCardinality(String),
    calculated_at          DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(calculated_at)
PARTITION BY toYYYYMM(calculated_at)
ORDER BY (
    analysis_version,
    chain_id,
    token_address,
    primary_deployer,
    related_address,
    relation_type,
    direction
);
