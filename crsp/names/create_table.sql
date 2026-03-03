CREATE DATABASE IF NOT EXISTS crsp;
DROP TABLE IF EXISTS crsp.security_names;

CREATE TABLE crsp.security_names (
    PERMNO              UInt32,
    HdrPrimaryExch      LowCardinality(String),
    NASDIssuno          Nullable(Int32),
    HdrSICCD            Nullable(Int32),
    PERMCO              Nullable(Int32),
    SecInfoStartDt      Date32,
    SecInfoEndDt        Nullable(Date32),
    SecurityBegDt       Nullable(Date32),
    SecurityEndDt       Nullable(Date32),
    CUSIP               Nullable(String),
    Ticker              LowCardinality(String),
    IssuerNm            String,
    ShareClass          LowCardinality(String),
    USIncFlg            LowCardinality(String),
    IssuerType          LowCardinality(String),
    SecurityType        LowCardinality(String),
    SecuritySubType     LowCardinality(String),
    ShareType           LowCardinality(String),
    SecurityActiveFlg   LowCardinality(String),
    SICCD               Nullable(Int32),
    PrimaryExch         LowCardinality(String),
    TradingSymbol       LowCardinality(String),
    NAICS               Nullable(Int32),
    TradingStatusFlg    LowCardinality(String)
)
ENGINE = MergeTree()
ORDER BY (PERMNO, SecInfoStartDt);
