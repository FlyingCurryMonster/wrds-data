-- IBES Detail History — Identifier
-- File 3 in IBES Detail History User Guide (Dec 2016)
-- Source CSV: detail/identifier.csv  (~283K rows)
-- Per-ticker identifier history: dilution factor, primary/diluted basis,
-- Canadian-currency / consolidation flags, MSCI Inc Press flag, US firm
-- flag, and the start date of the record.
-- Run from "/home/rakin/market data library/wrds-data/lseg ibes/detail":
--   clickhouse-client --multiquery < identifier.sql

CREATE DATABASE IF NOT EXISTS lseg_ibes;

DROP TABLE IF EXISTS lseg_ibes.identifier;
CREATE TABLE lseg_ibes.identifier (
    TICKER LowCardinality(String) COMMENT 'IBES 6-char ticker',
    CUSIP  String                 COMMENT 'CUSIP/SEDOL',
    OFTIC  LowCardinality(String) COMMENT 'Official exchange ticker',
    CNAME  String                 COMMENT 'Company name',
    DILFAC Float64                COMMENT 'Dilution factor (typically 1.0)',
    PDI    LowCardinality(String) COMMENT 'Primary/Diluted indicator (P or D)',
    CCOPCF LowCardinality(String) COMMENT 'Canadian Currency / Parent-Consolidated flag (non-US)',
    MSCIP  LowCardinality(String) COMMENT 'MSCI Inc Press flag',
    UAI    LowCardinality(String) COMMENT 'Uniform Actuals Indicator',
    USFIRM UInt8                  COMMENT '1 = US firm',
    SDATES Date32                 COMMENT 'Start date — record effective from this date'
) ENGINE = MergeTree
ORDER BY (TICKER, SDATES)
COMMENT 'IBES Detail History — Identifier mapping (ticker history, dilution basis)';

INSERT INTO lseg_ibes.identifier
FROM INFILE 'identifier.csv'
SETTINGS input_format_null_as_default = 1,
         date_time_input_format = 'best_effort',
         input_format_allow_errors_num = 100,
         input_format_allow_errors_ratio = 0.001
FORMAT CSVWithNames;
