CREATE DATABASE IF NOT EXISTS option_metrics;
DROP TABLE IF EXISTS option_metrics.security_prices;

CREATE TABLE option_metrics.security_prices (
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
    low             Nullable(Float64),
    high            Nullable(Float64),
    open            Nullable(Float64),
    close           Nullable(Float64),
    volume          Nullable(Int64),
    `return`        Nullable(Float64),
    cfadj           Nullable(Float64),
    shrout          Nullable(Int64),
    cfret           Nullable(Float64)
)
ENGINE = MergeTree()
ORDER BY (secid, date);
