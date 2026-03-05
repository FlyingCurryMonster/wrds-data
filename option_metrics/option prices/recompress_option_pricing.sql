-- =============================================================================
-- Recompress option_pricing: improve compression on low-ratio columns
-- Strategy: year-by-year migration using two staging tables + final output
--
-- ATTEMPT 1 (ORDER BY secid, date, optionid) + Gorilla+ZSTD(3):
--   Greeks WORSE: delta 1.31x->1.14x, theta 1.32x->1.15x
--   Reason: optionid is arbitrary, adjacent rows have uncorrelated Greeks.
--   Prices/ints improved: best_bid 3.24x->4.12x, last_date 4.34x->8x
--
-- ATTEMPT 2 (ORDER BY date, optionid) + Gorilla+ZSTD(3):
--   Greeks EVEN WORSE: delta 1.11x, theta 1.12x
--   Cross-sectional ordering: adjacent rows are different contracts on same day.
--   open_interest regressed: 5.84x->2.98x, last_date 4.34x->3.62x
--
-- ATTEMPT 3 (ORDER BY optionid, date) + Gorilla+ZSTD(3):
--   Time-series ordering: adjacent rows are same contract on consecutive days.
--   This should give Gorilla smooth residuals for Greeks.
-- =============================================================================

-- Step 1: Drop old tables
DROP TABLE IF EXISTS option_metrics.option_pricing_staging_compressed;
DROP TABLE IF EXISTS option_metrics.option_pricing_staging_passthrough;
DROP TABLE IF EXISTS option_metrics.option_pricing_v2;

-- Step 2: Create staging table for recompressed columns (Gorilla/Delta + ZSTD)
CREATE TABLE option_metrics.option_pricing_staging_compressed
(
    optionid            UInt64,
    date                Date32,
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
ORDER BY (optionid, date);

-- Step 3: Create staging table for passthrough columns
CREATE TABLE option_metrics.option_pricing_staging_passthrough
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

-- Step 4: Create final output table
CREATE TABLE option_metrics.option_pricing_v2
(
    optionid            UInt64,
    date                Date32,
    secid               UInt32,
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
-- Step 5: Year-by-year migration — year 2000
-- =============================================================================

-- 5a: Load compressed columns
INSERT INTO option_metrics.option_pricing_staging_compressed
SELECT optionid, date,
       delta, gamma, theta, vega, impl_volatility,
       best_bid, best_offer, last_date, open_interest, volume
FROM option_metrics.option_pricing
WHERE toYear(date) = 2000;

-- 5b: Load passthrough columns
INSERT INTO option_metrics.option_pricing_staging_passthrough
SELECT optionid, date, secid,
       symbol, symbol_flag, exdate, cp_flag, strike_price,
       cfadj, am_settlement, contract_size, ss_flag, forward_price,
       expiry_indicator, root, suffix, cusip, ticker, sic, index_flag,
       exchange_d, class, issue_type, industry_group, issuer,
       div_convention, exercise_style, am_set_flag
FROM option_metrics.option_pricing
WHERE toYear(date) = 2000;

-- 5c: Join staging tables into final output
INSERT INTO option_metrics.option_pricing_v2
SELECT
    p.optionid, p.date, p.secid, p.symbol, p.symbol_flag, p.exdate,
    c.last_date, p.cp_flag, p.strike_price,
    c.best_bid, c.best_offer, c.volume, c.open_interest,
    c.impl_volatility, c.delta, c.gamma, c.vega, c.theta,
    p.cfadj, p.am_settlement, p.contract_size,
    p.ss_flag, p.forward_price, p.expiry_indicator, p.root,
    p.suffix, p.cusip, p.ticker, p.sic, p.index_flag,
    p.exchange_d, p.class, p.issue_type, p.industry_group,
    p.issuer, p.div_convention, p.exercise_style, p.am_set_flag
FROM option_metrics.option_pricing_staging_passthrough p
INNER JOIN option_metrics.option_pricing_staging_compressed c
    ON p.optionid = c.optionid AND p.date = c.date;

-- 5d: Clear staging tables for next year
TRUNCATE TABLE option_metrics.option_pricing_staging_compressed;
TRUNCATE TABLE option_metrics.option_pricing_staging_passthrough;
