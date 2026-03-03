CREATE DATABASE IF NOT EXISTS crsp;
DROP TABLE IF EXISTS crsp.distributions;

CREATE TABLE crsp.distributions (
    PERMNO              UInt32,
    DisExDt             Date32,
    DisSeqNbr           Nullable(Int32),
    DisOrdinaryFlg      LowCardinality(String),
    DisType             LowCardinality(String),
    DisFreqType         LowCardinality(String),
    DisPaymentType      LowCardinality(String),
    DisDetailType       LowCardinality(String),
    DisTaxType          LowCardinality(String),
    DisOrigCurType      LowCardinality(String),
    DisDivAmt           Nullable(Float64),
    DisFacPr            Nullable(Float64),
    DisFacShr           Nullable(Float64),
    DisDeclareDt        Nullable(Date32),
    DisRecordDt         Nullable(Date32),
    DisPayDt            Nullable(Date32),
    DisPERMNO           Nullable(Int32),
    DisPERMCO           Nullable(Int32),
    DisAmountSourceType LowCardinality(String),
    PrimaryExch         LowCardinality(String),
    SICCD               Nullable(Int32),
    NASDIssuno          Nullable(Int32)
)
ENGINE = MergeTree()
ORDER BY (PERMNO, DisExDt);
