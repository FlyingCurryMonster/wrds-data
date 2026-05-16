-- IBES Detail History — Restated Actuals (unadjusted)
-- File 2 in IBES Detail History User Guide (Dec 2016)
-- Source CSV: unadjusted detail/unadjusted restated actuals.csv  (~11K rows)
-- Multiple restatements per fiscal period are possible. To avoid look-ahead
-- bias, timestamp by ACTDATS (when the restatement entered IBES), not PENDS.
-- Run from "/home/rakin/market data library/wrds-data/lseg ibes/unadjusted detail":
--   clickhouse-client --multiquery < restated_actuals.sql

CREATE DATABASE IF NOT EXISTS lseg_ibes;

DROP TABLE IF EXISTS lseg_ibes.restated_actuals_unadjusted;
CREATE TABLE lseg_ibes.restated_actuals_unadjusted (
    TICKER   LowCardinality(String) COMMENT 'IBES 6-char ticker',
    CUSIP    String                 COMMENT 'CUSIP/SEDOL',
    OFTIC    LowCardinality(String) COMMENT 'Official exchange ticker',
    CNAME    String                 COMMENT 'Company name',
    PENDS    Date32                 COMMENT 'Period end date — fiscal period being restated',
    MEASURE  LowCardinality(String) COMMENT '3-letter measure code',
    PDICITY  LowCardinality(String) COMMENT 'Periodicity: ANN, QTR, SAN, LTG',
    VALUE    Nullable(Float64)      COMMENT 'Restated actual value, unadjusted basis',
    CURR     LowCardinality(String) COMMENT 'Currency',
    USFIRM   UInt8                  COMMENT '1 = US firm',
    ACTDATS  Date32                 COMMENT 'Activation date — when restatement was recorded by IBES (use for point-in-time)',
    ACTTIMS  String                 COMMENT 'Activation time',
    ANNDATS  Date32                 COMMENT 'Announce date — public restatement announcement',
    ANNTIMS  String                 COMMENT 'Announce time'
) ENGINE = MergeTree
ORDER BY (TICKER, MEASURE, PDICITY, PENDS, ACTDATS)
COMMENT 'IBES Detail History — Restated Actuals (multiple restatements per fiscal period possible)';

INSERT INTO lseg_ibes.restated_actuals_unadjusted
FROM INFILE 'unadjusted restated actuals.csv'
SETTINGS input_format_null_as_default = 1,
         date_time_input_format = 'best_effort',
         input_format_allow_errors_num = 1000,
         input_format_allow_errors_ratio = 0.001
FORMAT CSVWithNames;
