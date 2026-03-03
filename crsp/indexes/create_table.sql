CREATE DATABASE IF NOT EXISTS crsp;
DROP TABLE IF EXISTS crsp.daily_market_returns;

CREATE TABLE crsp.daily_market_returns (
    DlyCalDt    Date32,
    vwretd      Nullable(Float64),
    vwretx      Nullable(Float64),
    vwTotVal    Nullable(Float64),
    vwUsdVal    Nullable(Float64),
    vwTotCnt    Nullable(Int32),
    vwUsdCnt    Nullable(Int32),
    ewretd      Nullable(Float64),
    ewretx      Nullable(Float64),
    ewTotVal    Nullable(Float64),
    ewUsdVal    Nullable(Float64),
    ewTotCnt    Nullable(Int32),
    ewUsdCnt    Nullable(Int32),
    sprtrn      Nullable(Float64),
    spindx      Nullable(Float64)
)
ENGINE = MergeTree()
ORDER BY (DlyCalDt);
