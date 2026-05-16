-- IBES Detail History — Restated Actuals (adjusted)
-- File 2 in IBES Detail History User Guide (Dec 2016)
-- Source CSV: detail/restated actuals.csv  (~22.8K rows)
-- Multiple restatements per fiscal period are possible. Timestamp by ACTDATS
-- (when the restatement entered IBES) to avoid look-ahead bias.
-- Run from "/home/rakin/market data library/wrds-data/lseg ibes/detail":
--   clickhouse-client --multiquery < restated_actuals.sql

CREATE DATABASE IF NOT EXISTS lseg_ibes;

DROP TABLE IF EXISTS lseg_ibes.restated_actuals_adjusted;
CREATE TABLE lseg_ibes.restated_actuals_adjusted (
    TICKER   LowCardinality(String) COMMENT 'IBES 6-char ticker',
    CUSIP    String                 COMMENT 'CUSIP/SEDOL',
    OFTIC    LowCardinality(String) COMMENT 'Official exchange ticker',
    CNAME    String                 COMMENT 'Company name',
    PENDS    Date32                 COMMENT 'Period end date — fiscal period being restated',
    MEASURE  LowCardinality(String) COMMENT '3-letter measure code',
    PDICITY  LowCardinality(String) COMMENT 'Periodicity: ANN, QTR, SAN, LTG',
    VALUE    Nullable(Float64)      COMMENT 'Restated actual value, split-adjusted',
    CURR     LowCardinality(String) COMMENT 'Currency',
    USFIRM   UInt8                  COMMENT '1 = US firm',
    ACTDATS  Date32                 COMMENT 'Activation date — point-in-time timestamp for the restatement',
    ACTTIMS  String                 COMMENT 'Activation time',
    ANNDATS  Date32                 COMMENT 'Announce date — public restatement announcement',
    ANNTIMS  String                 COMMENT 'Announce time'
) ENGINE = MergeTree
ORDER BY (TICKER, MEASURE, PDICITY, PENDS, ACTDATS)
COMMENT 'IBES Detail History — Restated Actuals, split-adjusted';

INSERT INTO lseg_ibes.restated_actuals_adjusted
FROM INFILE 'restated actuals.csv'
SETTINGS input_format_null_as_default = 1,
         date_time_input_format = 'best_effort',
         input_format_allow_errors_num = 100,
         input_format_allow_errors_ratio = 0.001
FORMAT CSVWithNames;
