CREATE TABLE IF NOT EXISTS canonical_block_headers (
    pipeline       text        NOT NULL,
    chain_id       bigint      NOT NULL CHECK (chain_id > 0),
    block_number   bigint      NOT NULL CHECK (block_number >= 0),
    block_hash     text        NOT NULL,
    observed_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline, chain_id, block_number)
);

CREATE INDEX IF NOT EXISTS canonical_block_headers_hash_idx
    ON canonical_block_headers (pipeline, chain_id, block_hash);

INSERT INTO canonical_block_headers (
    pipeline, chain_id, block_number, block_hash, observed_at
)
SELECT pipeline, chain_id, block_number, block_hash, updated_at
FROM ingestion_checkpoints
ON CONFLICT (pipeline, chain_id, block_number) DO NOTHING;
