-- IBES Detail History — Adjustment Factors
-- File 4 in IBES Detail History User Guide (Dec 2016)
-- Source CSV: detail/adjustments.csv  (~179K rows)
-- Split / corporate-action factors used to convert adjusted ↔ unadjusted
-- per-share values. To unadjust a price/EPS as of a given date, multiply by
-- the product of ADJ factors for splits with SPDATES > that date.
-- Run from "/home/rakin/market data library/wrds-data/lseg ibes/detail":
--   clickhouse-client --multiquery < adjustments.sql

CREATE DATABASE IF NOT EXISTS lseg_ibes;

DROP TABLE IF EXISTS lseg_ibes.adjustments;
CREATE TABLE lseg_ibes.adjustments (
    TICKER  LowCardinality(String) COMMENT 'IBES 6-char ticker',
    CUSIP   String                 COMMENT 'CUSIP/SEDOL',
    OFTIC   LowCardinality(String) COMMENT 'Official exchange ticker',
    CNAME   String                 COMMENT 'Company name',
    SPDATES Date32                 COMMENT 'Split / corporate-action date',
    ADJ     Float64                COMMENT 'Adjustment factor (e.g. 2 for a 2-for-1 split)',
    USFIRM  UInt8                  COMMENT '1 = US firm'
) ENGINE = MergeTree
ORDER BY (TICKER, SPDATES)
COMMENT 'IBES Detail History — Corporate-action / split adjustment factors';

INSERT INTO lseg_ibes.adjustments
FROM INFILE 'adjustments.csv'
SETTINGS input_format_null_as_default = 1,
         date_time_input_format = 'best_effort',
         input_format_allow_errors_num = 100,
         input_format_allow_errors_ratio = 0.001
FORMAT CSVWithNames;
