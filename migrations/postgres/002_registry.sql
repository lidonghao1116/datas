CREATE TABLE IF NOT EXISTS dex_factories (
    chain_id          bigint      NOT NULL CHECK (chain_id > 0),
    factory_address   text        NOT NULL,
    protocol          text        NOT NULL,
    protocol_version  text        NOT NULL,
    protocol_family   text        NOT NULL,
    is_verified       boolean     NOT NULL DEFAULT false,
    source             text        NOT NULL DEFAULT '',
    updated_at         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, factory_address)
);

CREATE TABLE IF NOT EXISTS dex_pools (
    chain_id          bigint      NOT NULL CHECK (chain_id > 0),
    pool_address      text        NOT NULL,
    factory_address   text        NOT NULL DEFAULT '',
    protocol          text        NOT NULL DEFAULT 'unknown',
    protocol_version  text        NOT NULL DEFAULT '',
    protocol_family   text        NOT NULL,
    token0_address    text        NOT NULL,
    token1_address    text        NOT NULL,
    discovered_block  bigint      NOT NULL CHECK (discovered_block >= 0),
    observed_at        timestamptz NOT NULL,
    updated_at         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, pool_address)
);

CREATE TABLE IF NOT EXISTS token_metadata (
    chain_id        bigint      NOT NULL CHECK (chain_id > 0),
    token_address   text        NOT NULL,
    symbol          text        NOT NULL DEFAULT '',
    decimals        smallint    NOT NULL DEFAULT 0 CHECK (decimals BETWEEN 0 AND 255),
    symbol_known    boolean     NOT NULL DEFAULT false,
    decimals_known  boolean     NOT NULL DEFAULT false,
    observed_at      timestamptz NOT NULL,
    updated_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, token_address)
);

INSERT INTO dex_factories (
    chain_id, factory_address, protocol, protocol_version,
    protocol_family, is_verified, source
) VALUES
    (
        8453,
        '0x420dd381b31aef6683db6b902084cb0ffec40da',
        'aerodrome',
        'classic',
        'uniswap_v2_compatible',
        true,
        'https://github.com/aerodrome-finance/contracts'
    ),
    (
        8453,
        '0x5e7bb104d84c7cb9b682aac2f3d509f5f406809a',
        'aerodrome',
        'slipstream',
        'uniswap_v3_compatible',
        true,
        'https://aerodrome.finance/security'
    ),
    (
        8453,
        '0x33128a8fc17869897dce68ed026d694621f6fdfd',
        'uniswap',
        'v3',
        'uniswap_v3_compatible',
        true,
        'https://docs.uniswap.org/contracts/v3/reference/deployments/base-deployments'
    )
ON CONFLICT (chain_id, factory_address) DO UPDATE SET
    protocol = EXCLUDED.protocol,
    protocol_version = EXCLUDED.protocol_version,
    protocol_family = EXCLUDED.protocol_family,
    is_verified = EXCLUDED.is_verified,
    source = EXCLUDED.source,
    updated_at = now();
