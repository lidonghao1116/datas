CREATE TABLE IF NOT EXISTS base.chain_reorganizations
(
    chain_id                 UInt64,
    common_ancestor_number   UInt64,
    common_ancestor_hash     String,
    old_head_number          UInt64,
    old_head_hash            String,
    new_branch_first_number  UInt64,
    new_branch_first_hash    String,
    orphaned_block_numbers   Array(UInt64),
    orphaned_block_hashes    Array(String),
    detected_at              DateTime64(3, 'UTC'),
    applied_at               DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(applied_at)
PARTITION BY toYYYYMM(detected_at)
ORDER BY (
    chain_id,
    common_ancestor_number,
    old_head_hash,
    new_branch_first_hash
);
