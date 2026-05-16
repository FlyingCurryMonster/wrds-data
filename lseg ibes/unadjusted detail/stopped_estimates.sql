-- IBES Detail History — Stopped Estimates (unadjusted)
-- File 12 in IBES Detail History User Guide (Dec 2016)
-- Source CSV: unadjusted detail/unadjusted stopped estimate.csv  (~12.8M rows)
-- Stop dates are algorithmically calculated and mark when an analyst's
-- estimate ceased to be active (coverage dropped, restricted list, etc).
-- Run from "/home/rakin/market data library/wrds-data/lseg ibes/unadjusted detail":
--   clickhouse-client --multiquery < stopped_estimates.sql

CREATE DATABASE IF NOT EXISTS lseg_ibes;

DROP TABLE IF EXISTS lseg_ibes.stopped_estimates_unadjusted;
CREATE TABLE lseg_ibes.stopped_estimates_unadjusted (
    TICKER    LowCardinality(String) COMMENT 'IBES 6-char ticker',
    CUSIP     String                 COMMENT 'CUSIP/SEDOL',
    OFTIC     LowCardinality(String) COMMENT 'Official exchange ticker',
    CNAME     String                 COMMENT 'Company name',
    ASTPDATS  Date32                 COMMENT 'Announce Stop Date — when estimate was stopped',
    ESTIMATOR String                 COMMENT 'Estimator numeric code (broker), zero-padded',
    PDICITY   LowCardinality(String) COMMENT 'Periodicity: 1-char code (A=annual, Q=quarter, S=semi, L=LTG)',
    MEASURE   LowCardinality(String) COMMENT '3-letter measure code',
    USFIRM    UInt8                  COMMENT '1 = US firm',
    FPEDATS   Date32                 COMMENT 'Forecast Period End Date — fiscal period the stopped estimate targeted',
    ASTPTIMS  String                 COMMENT 'Announce Stop Time'
) ENGINE = MergeTree
ORDER BY (TICKER, MEASURE, PDICITY, FPEDATS, ASTPDATS, ESTIMATOR)
COMMENT 'IBES Detail History — Stopped Estimates (when analyst forecasts ceased)';

INSERT INTO lseg_ibes.stopped_estimates_unadjusted
FROM INFILE 'unadjusted stopped estimate.csv'
SETTINGS input_format_null_as_default = 1,
         date_time_input_format = 'best_effort',
         input_format_allow_errors_num = 1000,
         input_format_allow_errors_ratio = 0.001
FORMAT CSVWithNames;
