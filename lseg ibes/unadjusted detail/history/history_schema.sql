-- IBES Detail History — Detail Estimates with Announce Timestamp (unadjusted)
-- File 10 in IBES Detail History User Guide (Dec 2016).
-- This is the analyst-by-analyst forecast history — the core file for building
-- point-in-time analyst estimate panels.
--
-- Run once, before loading any chunks:
--   clickhouse-client --multiquery < history_schema.sql
--
-- After this, for EACH chunk:
--   1. Edit the INFILE path in history_load_chunk.sql
--   2. clickhouse-client --multiquery < history_load_chunk.sql       -- loads to staging + runs QC
--   3. Inspect the QC output; if good:
--      clickhouse-client --multiquery < history_promote.sql          -- staging -> main
--
-- The same staging table is reused; promote() truncates it.

CREATE DATABASE IF NOT EXISTS lseg_ibes;

-- Main table — final destination, accumulates every chunk
CREATE TABLE IF NOT EXISTS lseg_ibes.history_unadjusted (
    TICKER      LowCardinality(String) COMMENT 'IBES 6-char ticker',
    CUSIP       String                 COMMENT 'CUSIP/SEDOL',
    OFTIC       LowCardinality(String) COMMENT 'Official exchange ticker',
    CNAME       String                 COMMENT 'Company name',
    ACTDATS     Date32                 COMMENT 'Activation date — when estimate became active in IBES (point-in-time)',
    ESTIMATOR   String                 COMMENT 'Estimator (broker) numeric code, zero-padded',
    ANALYS      String                 COMMENT 'Analyst numeric code, zero-padded; 000000 = unavailable',
    CURRFL      LowCardinality(String) COMMENT 'Canadian Currency flag at estimate level (non-US)',
    PDF         LowCardinality(String) COMMENT 'Primary/Diluted flag at estimate level (P or D)',
    FPI         LowCardinality(String) COMMENT 'Forecast Period Indicator: 0=FY0, 1-9 forward periods, A-Z annual',
    MEASURE     LowCardinality(String) COMMENT '3-letter measure code (EPS, SAL, ...)',
    VALUE       Nullable(Float64)      COMMENT 'Analyst estimate value, unadjusted basis',
    CURR        LowCardinality(String) COMMENT 'Currency of the estimate as submitted',
    USFIRM      UInt8                  COMMENT '1 = US firm',
    FPEDATS     Date32                 COMMENT 'Forecast Period End Date — fiscal period being forecast',
    ACTTIMS     String                 COMMENT 'Activation time (HH:MM:SS)',
    REVDATS     Date32                 COMMENT 'Review date — last date analyst confirmed/reviewed estimate',
    REVTIMS     String                 COMMENT 'Review time',
    ANNDATS     Date32                 COMMENT 'Announce date — when analyst first issued estimate',
    ANNTIMS     String                 COMMENT 'Announce time',
    report_curr LowCardinality(String) COMMENT 'Company-level default (report) currency'
) ENGINE = MergeTree
ORDER BY (TICKER, MEASURE, FPI, FPEDATS, ACTDATS, ESTIMATOR)
COMMENT 'IBES Detail History — Detail Estimates (analyst-by-analyst), unadjusted, AT timestamp';

-- Staging table — same schema, used to land one chunk at a time for QC
CREATE TABLE IF NOT EXISTS lseg_ibes.history_unadjusted_staging AS lseg_ibes.history_unadjusted;
