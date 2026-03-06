-- =============================================================================
-- Recompress option_pricing v2: drop Greeks, recompress bad columns
-- ORDER BY (optionid, date) for time-series locality
--
-- Previous attempts in recompress_option_pricing.sql showed:
--   - Gorilla doesn't help Greeks regardless of sort order
--   - (optionid, date) ordering dramatically improves bid/offer/OI/volume/last_date
--   - Dropping delta/gamma/theta/vega saves ~101 GiB (recomputable from IV)
-- =============================================================================

-- Step 1: Clean up leftover tables from previous attempts
DROP TABLE IF EXISTS option_metrics.option_pricing_staging_compressed;
DROP TABLE IF EXISTS option_metrics.option_pricing_staging_passthrough;
DROP TABLE IF EXISTS option_metrics.option_pricing_v2;

-- Step 2: Staging table for "bad" columns (ratio <= 8x, excluding Greeks)
CREATE TABLE option_metrics.option_pricing_staging_bad
(
    optionid            UInt64,
    date                Date32,
    -- Float columns: Gorilla + ZSTD(3)
    impl_volatility     Nullable(Float64) CODEC(Gorilla, ZSTD(3)),
    best_bid            Nullable(Float64) CODEC(Gorilla, ZSTD(3)),
    best_offer          Nullable(Float64) CODEC(Gorilla, ZSTD(3)),
    -- Int/date columns: Delta + ZSTD(3)
    last_date           Nullable(Date32)  CODEC(Delta, ZSTD(3)),
    open_interest       Nullable(UInt32)  CODEC(Delta, ZSTD(3)),
    volume              Nullable(UInt32)  CODEC(Delta, ZSTD(3))
)
ENGINE = MergeTree()
ORDER BY (optionid, date);

-- Step 3: Staging table for "good" columns (passthrough, no Greeks)
CREATE TABLE option_metrics.option_pricing_staging_good
(
    optionid            UInt64,
    date                Date32,
    secid               UInt32,
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
ORDER BY (optionid, date);

-- Step 4: Final output table (no Greeks, improved codecs on bad columns)
CREATE TABLE option_metrics.option_pricing_v2
(
    optionid            UInt64,
    date                Date32,
    secid               UInt32,
    symbol              String,
    symbol_flag         UInt8,
    exdate              Date32,
    cp_flag             LowCardinality(String),
    strike_price        UInt32,
    impl_volatility     Nullable(Float64) CODEC(Gorilla, ZSTD(3)),
    best_bid            Nullable(Float64) CODEC(Gorilla, ZSTD(3)),
    best_offer          Nullable(Float64) CODEC(Gorilla, ZSTD(3)),
    last_date           Nullable(Date32)  CODEC(Delta, ZSTD(3)),
    volume              Nullable(UInt32)  CODEC(Delta, ZSTD(3)),
    open_interest       Nullable(UInt32)  CODEC(Delta, ZSTD(3)),
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
ORDER BY (optionid, date);

-- =============================================================================
-- Step 5: Load year 2000
-- =============================================================================

-- 5a: Load bad columns
INSERT INTO option_metrics.option_pricing_staging_bad
SELECT optionid, date,
       impl_volatility, best_bid, best_offer,
       last_date, open_interest, volume
FROM option_metrics.option_pricing
WHERE toYear(date) = 2000;

-- 5b: Load good columns (no Greeks)
INSERT INTO option_metrics.option_pricing_staging_good
SELECT optionid, date, secid,
       symbol, symbol_flag, exdate, cp_flag, strike_price,
       cfadj, am_settlement, contract_size, ss_flag, forward_price,
       expiry_indicator, root, suffix, cusip, ticker, sic, index_flag,
       exchange_d, class, issue_type, industry_group, issuer,
       div_convention, exercise_style, am_set_flag
FROM option_metrics.option_pricing
WHERE toYear(date) = 2000;

-- 5c: Join into final table
INSERT INTO option_metrics.option_pricing_v2
SELECT
    g.optionid, g.date, g.secid, g.symbol, g.symbol_flag, g.exdate,
    g.cp_flag, g.strike_price,
    b.impl_volatility, b.best_bid, b.best_offer,
    b.last_date, b.volume, b.open_interest,
    g.cfadj, g.am_settlement, g.contract_size,
    g.ss_flag, g.forward_price, g.expiry_indicator, g.root,
    g.suffix, g.cusip, g.ticker, g.sic, g.index_flag,
    g.exchange_d, g.class, g.issue_type, g.industry_group,
    g.issuer, g.div_convention, g.exercise_style, g.am_set_flag
FROM option_metrics.option_pricing_staging_good g
INNER JOIN option_metrics.option_pricing_staging_bad b
    ON g.optionid = b.optionid AND g.date = b.date;

-- 5d: Clear staging for next year
TRUNCATE TABLE option_metrics.option_pricing_staging_bad;
TRUNCATE TABLE option_metrics.option_pricing_staging_good;
