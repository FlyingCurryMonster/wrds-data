-- =============================================================================
-- Recompress option_pricing: improve compression on low-ratio columns
-- Strategy: year-by-year migration using two staging tables + final output
-- =============================================================================

-- Step 1: Create staging table for recompressed columns (Gorilla/Delta + ZSTD)
DROP TABLE IF EXISTS option_metrics.option_pricing_staging_compressed;

CREATE TABLE option_metrics.option_pricing_staging_compressed
(
    secid               UInt32,
    date                Date32,
    optionid            UInt64,
    -- Float columns: Gorilla + ZSTD(3)
    delta               Nullable(Float64) CODEC(Gorilla, ZSTD(3)),
    gamma               Nullable(Float64) CODEC(Gorilla, ZSTD(3)),
    theta               Nullable(Float64) CODEC(Gorilla, ZSTD(3)),
    vega                Nullable(Float64) CODEC(Gorilla, ZSTD(3)),
    impl_volatility     Nullable(Float64) CODEC(Gorilla, ZSTD(3)),
    best_bid            Nullable(Float64) CODEC(Gorilla, ZSTD(3)),
    best_offer          Nullable(Float64) CODEC(Gorilla, ZSTD(3)),
    -- Int/date columns: Delta + ZSTD(3)
    last_date           Nullable(Date32)  CODEC(Delta, ZSTD(3)),
    open_interest       Nullable(UInt32)  CODEC(Delta, ZSTD(3)),
    volume              Nullable(UInt32)  CODEC(Delta, ZSTD(3))
)
ENGINE = MergeTree()
ORDER BY (secid, date, optionid);

-- Step 2: Create staging table for passthrough columns (already well-compressed)
DROP TABLE IF EXISTS option_metrics.option_pricing_staging_passthrough;

CREATE TABLE option_metrics.option_pricing_staging_passthrough
(
    secid               UInt32,
    date                Date32,
    optionid            UInt64,
    symbol              String,
    symbol_flag         UInt8,
    exdate              Date32,
    cp_flag             LowCardinality(String),
    strike_price        UInt32,
    cfadj               UInt8,
    am_settlement       UInt8,
    contract_size       Int16,
    ss_flag             UInt8,
    forward_price       Nullable(Float64),
    expiry_indicator    LowCardinality(String),
    root                String,
    suffix              String,
    cusip               String,
    ticker              LowCardinality(String),
    sic                 Nullable(UInt16),
    index_flag          UInt8,
    exchange_d          UInt8,
    class               LowCardinality(String),
    issue_type          LowCardinality(String),
    industry_group      Nullable(UInt16),
    issuer              String,
    div_convention      LowCardinality(String),
    exercise_style      LowCardinality(String),
    am_set_flag         LowCardinality(String)
)
ENGINE = MergeTree()
ORDER BY (secid, date, optionid);

-- Step 3: Create final output table with codecs on target columns
DROP TABLE IF EXISTS option_metrics.option_pricing_v2;

CREATE TABLE option_metrics.option_pricing_v2
(
    secid               UInt32,
    date                Date32,
    symbol              String,
    symbol_flag         UInt8,
    exdate              Date32,
    last_date           Nullable(Date32)  CODEC(Delta, ZSTD(3)),
    cp_flag             LowCardinality(String),
    strike_price        UInt32,
    best_bid            Nullable(Float64) CODEC(Gorilla, ZSTD(3)),
    best_offer          Nullable(Float64) CODEC(Gorilla, ZSTD(3)),
    volume              Nullable(UInt32)  CODEC(Delta, ZSTD(3)),
    open_interest       Nullable(UInt32)  CODEC(Delta, ZSTD(3)),
    impl_volatility     Nullable(Float64) CODEC(Gorilla, ZSTD(3)),
    delta               Nullable(Float64) CODEC(Gorilla, ZSTD(3)),
    gamma               Nullable(Float64) CODEC(Gorilla, ZSTD(3)),
    vega                Nullable(Float64) CODEC(Gorilla, ZSTD(3)),
    theta               Nullable(Float64) CODEC(Gorilla, ZSTD(3)),
    optionid            UInt64,
    cfadj               UInt8,
    am_settlement       UInt8,
    contract_size       Int16,
    ss_flag             UInt8,
    forward_price       Nullable(Float64),
    expiry_indicator    LowCardinality(String),
    root                String,
    suffix              String,
    cusip               String,
    ticker              LowCardinality(String),
    sic                 Nullable(UInt16),
    index_flag          UInt8,
    exchange_d          UInt8,
    class               LowCardinality(String),
    issue_type          LowCardinality(String),
    industry_group      Nullable(UInt16),
    issuer              String,
    div_convention      LowCardinality(String),
    exercise_style      LowCardinality(String),
    am_set_flag         LowCardinality(String)
)
ENGINE = MergeTree()
ORDER BY (secid, date, optionid);

-- =============================================================================
-- Step 4: Year-by-year migration
-- Start with 2000 to validate, then repeat for 2001-2025
-- =============================================================================

-- 4a: Load compressed columns for year
INSERT INTO option_metrics.option_pricing_staging_compressed
SELECT secid, date, optionid,
       delta, gamma, theta, vega, impl_volatility,
       best_bid, best_offer, last_date, open_interest, volume
FROM option_metrics.option_pricing
WHERE toYear(date) = 2000;

-- 4b: Load passthrough columns for year
INSERT INTO option_metrics.option_pricing_staging_passthrough
SELECT secid, date, optionid,
       symbol, symbol_flag, exdate, cp_flag, strike_price,
       cfadj, am_settlement, contract_size, ss_flag, forward_price,
       expiry_indicator, root, suffix, cusip, ticker, sic, index_flag,
       exchange_d, class, issue_type, industry_group, issuer,
       div_convention, exercise_style, am_set_flag
FROM option_metrics.option_pricing
WHERE toYear(date) = 2000;

-- 4c: Join staging tables into final output
INSERT INTO option_metrics.option_pricing_v2
SELECT
    p.secid, p.date, p.symbol, p.symbol_flag, p.exdate,
    c.last_date, p.cp_flag, p.strike_price,
    c.best_bid, c.best_offer, c.volume, c.open_interest,
    c.impl_volatility, c.delta, c.gamma, c.vega, c.theta,
    p.optionid, p.cfadj, p.am_settlement, p.contract_size,
    p.ss_flag, p.forward_price, p.expiry_indicator, p.root,
    p.suffix, p.cusip, p.ticker, p.sic, p.index_flag,
    p.exchange_d, p.class, p.issue_type, p.industry_group,
    p.issuer, p.div_convention, p.exercise_style, p.am_set_flag
FROM option_metrics.option_pricing_staging_passthrough p
INNER JOIN option_metrics.option_pricing_staging_compressed c
    ON p.secid = c.secid AND p.date = c.date AND p.optionid = c.optionid;

-- 4d: Verify row count
-- SELECT toYear(date) as yr, count() FROM option_metrics.option_pricing_v2 GROUP BY yr ORDER BY yr;
-- SELECT toYear(date) as yr, count() FROM option_metrics.option_pricing WHERE toYear(date) = 2000 GROUP BY yr;

-- 4e: Clear staging tables for next year
TRUNCATE TABLE option_metrics.option_pricing_staging_compressed;
TRUNCATE TABLE option_metrics.option_pricing_staging_passthrough;

-- =============================================================================
-- Step 5: Check compression improvement
-- =============================================================================
-- SELECT
--     name,
--     formatReadableSize(data_compressed_bytes) as compressed,
--     round(data_uncompressed_bytes / data_compressed_bytes, 2) as ratio
-- FROM system.columns
-- WHERE database = 'option_metrics' AND table = 'option_pricing_v2'
-- ORDER BY data_uncompressed_bytes DESC;

-- =============================================================================
-- Step 6: After all years loaded and verified
-- =============================================================================
-- RENAME TABLE option_metrics.option_pricing TO option_metrics.option_pricing_old;
-- RENAME TABLE option_metrics.option_pricing_v2 TO option_metrics.option_pricing;
-- DROP TABLE option_metrics.option_pricing_staging_compressed;
-- DROP TABLE option_metrics.option_pricing_staging_passthrough;
-- DROP TABLE option_metrics.option_pricing_old;  -- only after final verification
