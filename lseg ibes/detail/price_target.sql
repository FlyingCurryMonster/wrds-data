-- IBES Detail History — Analyst Price Targets (adjusted)
-- File 13 in IBES Detail History User Guide (Dec 2016)
-- Source CSV: detail/price target.csv  (~7.5M rows)
-- Run from "/home/rakin/market data library/wrds-data/lseg ibes/detail":
--   clickhouse-client --multiquery < price_target.sql

CREATE DATABASE IF NOT EXISTS lseg_ibes;

DROP TABLE IF EXISTS lseg_ibes.price_targets_adjusted;
CREATE TABLE lseg_ibes.price_targets_adjusted (
    TICKER   LowCardinality(String) COMMENT 'IBES 6-char ticker',
    CUSIP    String                 COMMENT 'CUSIP/SEDOL',
    OFTIC    LowCardinality(String) COMMENT 'Official exchange ticker',
    CNAME    String                 COMMENT 'Company name',
    ACTDATS  Date32                 COMMENT 'Activation date — when price target was recorded',
    ESTIMID  LowCardinality(String) COMMENT 'Estimator ID — alphanumeric broker code (e.g., RBCDOMIN)',
    ALYSNAM  String                 COMMENT 'Analyst name',
    HORIZON  UInt16                 COMMENT 'Price target horizon in months (typically 12)',
    VALUE    Nullable(Float64)      COMMENT 'Price target value, split-adjusted',
    ESTCUR   LowCardinality(String) COMMENT 'Estimate currency',
    CURR     LowCardinality(String) COMMENT 'Company-level default currency',
    AMASKCD  String                 COMMENT 'Analyst mask code (anonymized 6-digit id)',
    USFIRM   UInt8                  COMMENT '1 = US firm',
    ACTTIMS  String                 COMMENT 'Activation time',
    ANNDATS  Date32                 COMMENT 'Announce date — when analyst issued the target',
    ANNTIMS  String                 COMMENT 'Announce time'
) ENGINE = MergeTree
ORDER BY (TICKER, ACTDATS, ESTIMID)
COMMENT 'IBES Detail History — Analyst price targets, split-adjusted';

INSERT INTO lseg_ibes.price_targets_adjusted
FROM INFILE 'price target.csv'
SETTINGS input_format_null_as_default = 1,
         date_time_input_format = 'best_effort',
         input_format_allow_errors_num = 1000,
         input_format_allow_errors_ratio = 0.001
FORMAT CSVWithNames;
