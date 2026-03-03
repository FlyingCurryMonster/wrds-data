CREATE DATABASE IF NOT EXISTS crsp;
DROP TABLE IF EXISTS crsp.quarterly_rebalance;

CREATE TABLE crsp.quarterly_rebalance (
    INDNO               UInt32,
    YYYY                UInt16,
    INDFAM              LowCardinality(String),
    PortNum             Nullable(Int32),
    SecAssignYYYY       Nullable(UInt16),
    SecAssignStartDt    Nullable(Date32),
    SecAssignEndDt      Nullable(Date32),
    SecIssuerAllCnt     Nullable(Int32),
    SecStatType         LowCardinality(String),
    SecLowBreakpoint    Nullable(Float64),
    SecHighBreakpoint   Nullable(Float64),
    SecMinStat          Nullable(Float64),
    SecMaxStat          Nullable(Float64),
    SecMinStatPERMNO    Nullable(Int32),
    SecMinStatIssuerNm  Nullable(String),
    SecMaxStatPERMNO    Nullable(Int32),
    SecMaxStatIssuerNm  Nullable(String),
    SecSecurityAllCnt   Nullable(Int32),
    SecSecurityDropCnt  Nullable(Int32),
    SecSecurityAddCnt   Nullable(Int32)
)
ENGINE = MergeTree()
ORDER BY (INDNO, YYYY);
