CREATE DATABASE IF NOT EXISTS option_metrics;
DROP TABLE IF EXISTS option_metrics.forward_price;

CREATE TABLE option_metrics.forward_price (
    secid           UInt32,
    date            Date32,
    expiration      Date32,
    AMSettlement    UInt8,
    ForwardPrice    Nullable(Float64),
    cusip           String,
    ticker          LowCardinality(String),
    sic             Nullable(String),
    index_flag      UInt8,
    exchange_d      Nullable(UInt16),
    class           LowCardinality(String),
    issue_type      LowCardinality(String),
    industry_group  Nullable(String),
    issuer          String
)
ENGINE = MergeTree()
ORDER BY (secid, date, expiration);
