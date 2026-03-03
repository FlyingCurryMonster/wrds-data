CREATE DATABASE IF NOT EXISTS option_metrics;
DROP TABLE IF EXISTS option_metrics.option_pricing;

CREATE TABLE option_metrics.option_pricing
(
    secid               UInt32,
    date                Date32,
    symbol              String,
    symbol_flag         UInt8,
    exdate              Date32,
    last_date           Nullable(Date32),
    cp_flag             LowCardinality(String),
    strike_price        UInt32,
    best_bid            Nullable(Float64),
    best_offer          Nullable(Float64),
    volume              Nullable(UInt32),
    open_interest       Nullable(UInt32),
    impl_volatility     Nullable(Float64),
    delta               Nullable(Float64),
    gamma               Nullable(Float64),
    vega                Nullable(Float64),
    theta               Nullable(Float64),
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
