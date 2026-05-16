-- IBES Detail History — load ONE chunk of the unadjusted history file to staging
--
-- Filename convention (the date range encoded in the filename indicates the
-- WRDS filter dimension used):
--   "unadjusted history YYYY -MM to  YYYY-MM.csv"          -- ANNDATS filter (default)
--   "unadjusted history actdats YYYY -MM to  YYYY-MM.csv"  -- ACTDATS filter (legacy)
--
-- Before each run:
--   1. Edit the INFILE path below to point at the current chunk CSV
--   2. Run: clickhouse-client --multiquery < history_load_chunk.sql
--   3. Inspect the QC output — verify row count, NULL fractions, date range
--   4. If clean, run history_promote.sql to move staging -> main
--      (promote does LEFT ANTI JOIN dedupe — safe with overlapping chunks)
--
-- Run from "/home/rakin/market data library/wrds-data/lseg ibes/unadjusted detail/history":
--   clickhouse-client --multiquery < history_load_chunk.sql

-- Reset staging before loading the new chunk
TRUNCATE TABLE lseg_ibes.history_unadjusted_staging;

-- ===== EDIT THIS LINE PER CHUNK =====
INSERT INTO lseg_ibes.history_unadjusted_staging
FROM INFILE 've3doiddgjw8eopk.csv'
SETTINGS input_format_null_as_default = 1,
         date_time_input_format = 'best_effort',
         input_format_allow_errors_num = 1000,
         input_format_allow_errors_ratio = 0.001
FORMAT CSVWithNames;
-- =====================================

-- ===== QC checks on staging =====

-- Row count and date range
SELECT 'row_count_and_range' AS check,
       count()             AS rows,
       min(ACTDATS)        AS min_actdats,
       max(ACTDATS)        AS max_actdats,
       min(FPEDATS)        AS min_fpedats,
       max(FPEDATS)        AS max_fpedats,
       min(ANNDATS)        AS min_anndats,
       max(ANNDATS)        AS max_anndats,
       min(REVDATS)        AS min_revdats,
       max(REVDATS)        AS max_revdats
FROM lseg_ibes.history_unadjusted_staging;

-- NULL profile on key fields
SELECT 'null_profile' AS check,
       round(countIf(VALUE IS NULL)/count(), 4)            AS pct_value_null,
       round(countIf(ACTDATS = toDate32(0))/count(), 4)    AS pct_actdats_zero,
       round(countIf(FPEDATS = toDate32(0))/count(), 4)    AS pct_fpedats_zero,
       round(countIf(empty(ESTIMATOR) OR ESTIMATOR = '000000')/count(), 4) AS pct_estimator_missing,
       round(countIf(empty(ANALYS) OR ANALYS = '000000')/count(), 4)       AS pct_analyst_missing
FROM lseg_ibes.history_unadjusted_staging;

-- VALUE range sanity (look for sentinel values like -99999)
SELECT 'value_extremes' AS check, min(VALUE) AS minv, max(VALUE) AS maxv,
       countIf(VALUE = -99999) AS n_neg99999,
       countIf(abs(VALUE) > 1e12) AS n_extreme
FROM lseg_ibes.history_unadjusted_staging;

-- Distinct MEASURE / FPI / CURR
SELECT 'distinct_codes' AS check,
       uniqExact(MEASURE) AS n_measures,
       uniqExact(FPI)     AS n_fpi,
       uniqExact(CURR)    AS n_curr,
       uniqExact(TICKER)  AS n_tickers
FROM lseg_ibes.history_unadjusted_staging;

-- Duplicate check on the natural key
SELECT 'duplicates' AS check,
       count() - uniqExact(TICKER, MEASURE, FPI, FPEDATS, ESTIMATOR, ANALYS, ACTDATS, ACTTIMS) AS dup_rows,
       count() AS total_rows
FROM lseg_ibes.history_unadjusted_staging;

-- Overlap with what's already in main (so we know if we're double-loading)
SELECT 'overlap_with_main' AS check,
       (SELECT count() FROM lseg_ibes.history_unadjusted_staging s
        WHERE EXISTS (
            SELECT 1 FROM lseg_ibes.history_unadjusted m
            WHERE m.TICKER = s.TICKER AND m.MEASURE = s.MEASURE
              AND m.FPI = s.FPI AND m.FPEDATS = s.FPEDATS
              AND m.ESTIMATOR = s.ESTIMATOR AND m.ANALYS = s.ANALYS
              AND m.ACTDATS = s.ACTDATS AND m.ACTTIMS = s.ACTTIMS
        )) AS rows_already_in_main;
