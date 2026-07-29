CREATE TABLE IF NOT EXISTS ingestion_checkpoints (
    pipeline        text        NOT NULL,
    chain_id        bigint      NOT NULL CHECK (chain_id > 0),
    block_number    bigint      NOT NULL CHECK (block_number >= 0),
    block_hash      text        NOT NULL,
    updated_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline, chain_id)
);

