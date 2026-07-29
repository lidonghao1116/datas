CREATE VIEW IF NOT EXISTS base.dex_swap_valuations_current AS
SELECT *
FROM base.dex_swap_valuations FINAL
WHERE valuation_version = 'usd-v2';
