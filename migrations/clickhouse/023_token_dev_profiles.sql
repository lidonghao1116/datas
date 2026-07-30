CREATE TABLE IF NOT EXISTS base.token_dev_profiles
(
    analysis_version          LowCardinality(String),
    chain_id                  UInt64,
    token_address             String,
    primary_deployer          String,
    deployment_transaction    String,
    deployment_block          UInt64,
    deployed_at               DateTime64(3, 'UTC'),
    related_address_count     UInt64,
    strong_related_count      UInt64,
    relationship_types       Array(String),
    deployed_token_count      UInt64,
    risky_deployed_token_count UInt64,
    honeypot_token_count      UInt64,
    black_method_token_count  UInt64,
    mint_method_token_count   UInt64,
    proxy_token_count         UInt64,
    risk_score_raw            String,
    risk_level                LowCardinality(String),
    confidence_raw            String,
    source_updated_at         DateTime64(3, 'UTC'),
    calculated_at             DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(calculated_at)
PARTITION BY toYYYYMM(calculated_at)
ORDER BY (analysis_version, chain_id, token_address);
