-- IBES Detail History — Stop Price Target
-- File 14 in IBES Detail History User Guide (Dec 2016)
-- Source CSV: detail/stop price target.csv  (~947K rows)
-- Marks when a broker stopped issuing price targets for a security.
-- Run from "/home/rakin/market data library/wrds-data/lseg ibes/detail":
--   clickhouse-client --multiquery < stop_price_target.sql

CREATE DATABASE IF NOT EXISTS lseg_ibes;

DROP TABLE IF EXISTS lseg_ibes.stop_price_targets;
CREATE TABLE lseg_ibes.stop_price_targets (
    TICKER  LowCardinality(String) COMMENT 'IBES 6-char ticker',
    CUSIP   String                 COMMENT 'CUSIP/SEDOL',
    OFTIC   LowCardinality(String) COMMENT 'Official exchange ticker',
    CNAME   String                 COMMENT 'Company name',
    STPDATS Date32                 COMMENT 'Stop date — when broker ceased issuing price targets',
    ESTIMID LowCardinality(String) COMMENT 'Estimator ID — broker code',
    USFIRM  UInt8                  COMMENT '1 = US firm',
    STPTIMS String                 COMMENT 'Stop time'
) ENGINE = MergeTree
ORDER BY (TICKER, STPDATS, ESTIMID)
COMMENT 'IBES Detail History — Stop Price Target (broker coverage drops)';

INSERT INTO lseg_ibes.stop_price_targets
FROM INFILE 'stop price target.csv'
SETTINGS input_format_null_as_default = 1,
         date_time_input_format = 'best_effort',
         input_format_allow_errors_num = 100,
         input_format_allow_errors_ratio = 0.001
FORMAT CSVWithNames;
