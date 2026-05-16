-- IBES Detail History — promote staging chunk into main table (deduplicated)
--
-- Run AFTER history_load_chunk.sql has been validated for the current chunk:
--   clickhouse-client --multiquery < history_promote.sql
--
-- This performs a LEFT ANTI JOIN against main on the natural key, so only
-- rows that don't already exist in main are appended. Safe to run with
-- chunks that overlap each other or with previously-loaded chunks (e.g.,
-- ANNDATS-filtered chunks overlapping ACTDATS-filtered chunks).
--
-- Natural key: (TICKER, MEASURE, FPI, FPEDATS, ESTIMATOR, ANALYS,
--               ACTDATS, ACTTIMS, ANNDATS, ANNTIMS)

-- Pre-promote diagnostic
SELECT 'pre_promote' AS check,
       (SELECT count() FROM lseg_ibes.history_unadjusted)         AS main_rows_before,
       (SELECT count() FROM lseg_ibes.history_unadjusted_staging) AS staging_rows;

INSERT INTO lseg_ibes.history_unadjusted
SELECT s.*
FROM lseg_ibes.history_unadjusted_staging AS s
LEFT ANTI JOIN lseg_ibes.history_unadjusted AS m
  ON  s.TICKER    = m.TICKER
  AND s.MEASURE   = m.MEASURE
  AND s.FPI       = m.FPI
  AND s.FPEDATS   = m.FPEDATS
  AND s.ESTIMATOR = m.ESTIMATOR
  AND s.ANALYS    = m.ANALYS
  AND s.ACTDATS   = m.ACTDATS
  AND s.ACTTIMS   = m.ACTTIMS
  AND s.ANNDATS   = m.ANNDATS
  AND s.ANNTIMS   = m.ANNTIMS;

-- Post-promote summary
SELECT 'post_promote' AS check,
       (SELECT count()      FROM lseg_ibes.history_unadjusted) AS main_rows_after,
       (SELECT min(ACTDATS) FROM lseg_ibes.history_unadjusted) AS main_min_actdats,
       (SELECT max(ACTDATS) FROM lseg_ibes.history_unadjusted) AS main_max_actdats,
       (SELECT min(ANNDATS) FROM lseg_ibes.history_unadjusted) AS main_min_anndats,
       (SELECT max(ANNDATS) FROM lseg_ibes.history_unadjusted) AS main_max_anndats;

TRUNCATE TABLE lseg_ibes.history_unadjusted_staging;
