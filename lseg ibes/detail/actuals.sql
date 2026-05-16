-- IBES Detail History — Actuals (split-adjusted)
-- File 1a in IBES Detail History User Guide (Dec 2016)
-- Source CSV: detail/actuals.csv  (~14.9M rows)
-- Run from "/home/rakin/market data library/wrds-data/lseg ibes/detail":
--   clickhouse-client --multiquery < actuals.sql

CREATE DATABASE IF NOT EXISTS lseg_ibes;

DROP TABLE IF EXISTS lseg_ibes.actuals_adjusted;
CREATE TABLE lseg_ibes.actuals_adjusted (
    TICKER   LowCardinality(String) COMMENT 'IBES 6-char ticker; stable across CUSIP/name changes',
    CUSIP    String                 COMMENT 'CUSIP (US) or SEDOL (non-US)',
    OFTIC    LowCardinality(String) COMMENT 'Official exchange ticker (can change over time)',
    CNAME    String                 COMMENT 'Company name as of record date',
    PENDS    Date32                 COMMENT 'Period end date — fiscal period the actual applies to',
    MEASURE  LowCardinality(String) COMMENT '3-letter measure code (EPS, SAL, NET, EBI, CPS, DPS, BPS, ...)',
    PDICITY  LowCardinality(String) COMMENT 'Periodicity: ANN, QTR, SAN, LTG',
    ANNDATS  Date32                 COMMENT 'Announce date — public earnings/actual announcement date',
    ANNTIMS  String                 COMMENT 'Announce time (HH:MM:SS, EST/EDT)',
    ACTDATS  Date32                 COMMENT 'Activation date — when IBES recorded the actual (use for point-in-time)',
    ACTTIMS  String                 COMMENT 'Activation time (HH:MM:SS)',
    VALUE    Nullable(Float64)      COMMENT 'Reported actual value (per-share for EPS-family, raw units otherwise)',
    CURR_ACT LowCardinality(String) COMMENT 'Currency of the actual value',
    USFIRM   UInt8                  COMMENT '1 = US firm, 0 = non-US'
) ENGINE = MergeTree
ORDER BY (TICKER, MEASURE, PDICITY, PENDS, ACTDATS)
COMMENT 'IBES Detail History — Actuals, split-adjusted (per-share values restated for splits)';

INSERT INTO lseg_ibes.actuals_adjusted
FROM INFILE 'actuals.csv'
SETTINGS input_format_null_as_default = 1,
         date_time_input_format = 'best_effort',
         input_format_allow_errors_num = 1000,
         input_format_allow_errors_ratio = 0.001
FORMAT CSVWithNames;
