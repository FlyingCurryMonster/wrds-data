CREATE DATABASE IF NOT EXISTS crsp;
DROP TABLE IF EXISTS crsp.daily_index_history;

CREATE TABLE crsp.daily_index_history (
    INDNO           UInt32,
    YYYYMMDD        UInt32,
    DlyCalDt        Date32,
    DlyTotRet       Nullable(Float64),
    DlyTotInd       Nullable(Float64),
    DlyPrcRet       Nullable(Float64),
    DlyPrcInd       Nullable(Float64),
    DlyIncRet       Nullable(Float64),
    DlyIncInd       Nullable(Float64),
    DlyUsdCnt       Nullable(Int32),
    DlyUsdVal       Nullable(Float64),
    DlyTotCnt       Nullable(Int32),
    DlyTotVal       Nullable(Float64),
    DlyEligCnt      Nullable(Int32),
    DlyWgtAmt       Nullable(Float64),
    INDFAM          LowCardinality(String),
    IndFamType      LowCardinality(String),
    IndNm           String,
    IndBegDt        Nullable(Date32),
    IndEndDt        Nullable(Date32),
    BaseLvl         Nullable(Float64),
    BaseDt          Nullable(Date32),
    FreqAvail       LowCardinality(String),
    WeightType      LowCardinality(String),
    CntValType      LowCardinality(String),
    PortNum         Nullable(Int32)
)
ENGINE = MergeTree()
ORDER BY (INDNO, DlyCalDt);
