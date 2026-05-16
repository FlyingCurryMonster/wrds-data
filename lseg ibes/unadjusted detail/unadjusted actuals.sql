-- IBES Detail History — Actuals (unadjusted, analyst-quoted basis)
-- File 1a in IBES Detail History User Guide (Dec 2016)
-- Source CSV: unadjusted detail/unadjusted actuals.csv  (~23.8M rows)
-- Run from "/home/rakin/market data library/wrds-data/lseg ibes/unadjusted detail":
--   clickhouse-client --multiquery < "unadjusted actuals.sql"

CREATE DATABASE IF NOT EXISTS lseg_ibes;

DROP TABLE IF EXISTS lseg_ibes.actuals_unadjusted;
CREATE TABLE lseg_ibes.actuals_unadjusted (
    TICKER   LowCardinality(String) COMMENT 'IBES 6-char ticker',
    CUSIP    String                 COMMENT 'CUSIP/SEDOL',
    OFTIC    LowCardinality(String) COMMENT 'Official exchange ticker',
    CNAME    String                 COMMENT 'Company name',
    PENDS    Date32                 COMMENT 'Period end date',
    MEASURE  LowCardinality(String) COMMENT '3-letter measure code',
    PDICITY  LowCardinality(String) COMMENT 'ANN, QTR, SAN, LTG',
    ANNDATS  Date32                 COMMENT 'Public announce date',
    ANNTIMS  String                 COMMENT 'Public announce time',
    ACTDATS  Date32                 COMMENT 'IBES activation date (point-in-time)',
    ACTTIMS  String                 COMMENT 'IBES activation time',
    VALUE    Nullable(Float64)      COMMENT 'Reported actual on unadjusted (pre-split) basis',
    CURR_ACT LowCardinality(String) COMMENT 'Currency',
    USFIRM   UInt8                  COMMENT '1 = US firm'
) ENGINE = MergeTree
ORDER BY (TICKER, MEASURE, PDICITY, PENDS, ACTDATS)
COMMENT 'IBES Detail History — Actuals, unadjusted (raw values as originally reported)';

INSERT INTO lseg_ibes.actuals_unadjusted
FROM INFILE 'unadjusted actuals.csv'
SETTINGS input_format_null_as_default = 1,
         date_time_input_format = 'best_effort',
         input_format_allow_errors_num = 1000,
         input_format_allow_errors_ratio = 0.001
FORMAT CSVWithNames;
