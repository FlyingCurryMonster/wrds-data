CREATE DATABASE IF NOT EXISTS option_metrics;
DROP TABLE IF EXISTS option_metrics.index_dividend_yield;

CREATE TABLE option_metrics.index_dividend_yield (
    secid           UInt32,
    date            Date32,
    cusip           String,
    ticker          LowCardinality(String),
    sic             Nullable(String),
    index_flag      UInt8,
    exchange_d      Nullable(UInt16),
    class           LowCardinality(String),
    issue_type      LowCardinality(String),
    industry_group  Nullable(String),
    rate            Nullable(Float64),
    expiration      Date32
)
ENGINE = MergeTree()
ORDER BY (secid, date, expiration);
