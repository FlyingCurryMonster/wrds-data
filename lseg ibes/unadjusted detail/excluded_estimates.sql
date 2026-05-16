-- IBES Detail History — Announce-with-Timestamp Excluded Estimates (unadjusted)
-- File 11 in IBES Detail History User Guide (Dec 2016)
-- Source CSV: unadjusted detail/unadjusted excluded estimates.csv  (~15.1M rows)
-- Contains analyst-level estimates that were flagged for exclusion from
-- consensus. The EXCFLA / EXCDATS / EXCENDS columns mark the exclusion window.
-- Run from "/home/rakin/market data library/wrds-data/lseg ibes/unadjusted detail":
--   clickhouse-client --multiquery < excluded_estimates.sql

CREATE DATABASE IF NOT EXISTS lseg_ibes;

DROP TABLE IF EXISTS lseg_ibes.excluded_estimates_unadjusted;
CREATE TABLE lseg_ibes.excluded_estimates_unadjusted (
    TICKER    LowCardinality(String) COMMENT 'IBES 6-char ticker',
    CUSIP     String                 COMMENT 'CUSIP/SEDOL',
    OFTIC     LowCardinality(String) COMMENT 'Official exchange ticker',
    CNAME     String                 COMMENT 'Company name',
    ACTDATS   Date32                 COMMENT 'Activation date — when estimate became active in IBES',
    ESTIMATOR String                 COMMENT 'Estimator (broker/firm) numeric code, zero-padded',
    ANALYS    String                 COMMENT 'Analyst numeric code, zero-padded; 000000 = unavailable',
    FPI       LowCardinality(String) COMMENT 'Forecast Period Indicator: 0=FY0/Q0, 1-9 forward periods, A-Z annual',
    MEASURE   LowCardinality(String) COMMENT '3-letter measure code (EPS, SAL, ...)',
    VALUE     Nullable(Float64)      COMMENT 'Analyst estimate value on unadjusted basis',
    EXCFLA    LowCardinality(String) COMMENT 'Exclude flag: X = excluded from consensus, blank = active',
    USFIRM    UInt8                  COMMENT '1 = US firm',
    CURR      LowCardinality(String) COMMENT 'Currency of the estimate',
    FPEDATS   Date32                 COMMENT 'Forecast Period End Date — fiscal period being forecast',
    ACTTIMS   String                 COMMENT 'Activation time (HH:MM:SS)',
    EXCDATS   Date32                 COMMENT 'Exclude date — when estimate was flagged excluded',
    EXCTIMS   String                 COMMENT 'Exclude time',
    EXCENDS   Date32                 COMMENT 'Exclude end date — when the exclusion ended (far-future if still excluded)',
    EXCETIMS  String                 COMMENT 'Exclude end time'
) ENGINE = MergeTree
ORDER BY (TICKER, MEASURE, FPI, FPEDATS, ACTDATS, ESTIMATOR)
COMMENT 'IBES Detail History — Analyst-level estimates flagged for exclusion from consensus';

INSERT INTO lseg_ibes.excluded_estimates_unadjusted
FROM INFILE 'unadjusted excluded estimates.csv'
SETTINGS input_format_null_as_default = 1,
         date_time_input_format = 'best_effort',
         input_format_allow_errors_num = 1000,
         input_format_allow_errors_ratio = 0.001
FORMAT CSVWithNames;
