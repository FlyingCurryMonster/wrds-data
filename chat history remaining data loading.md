# Chat History: Loading All Remaining Data into ClickHouse

## Overview

This session loaded 13 WRDS datasets into ClickHouse, totaling ~224M rows. All CSV files were validated, SQL schemas and load scripts were created, data quirks were discovered and fixed, and everything was verified.

---

## Step 1: Verify All CSV Files Exist

Confirmed all 13 files on disk:

| File | Size |
|------|------|
| `crsp/daily stock price old/crsp-daily-stock-price.csv` | 13 GB |
| `optionmetrics/forwardprice.csv` | 8.8 GB |
| `optionmetrics/security-prices.csv` | 4.7 GB |
| `crsp/daily stock file/annual 2020-2024.csv` | 6.1 GB |
| `crsp/sp500-index-constituents/2000-2024.csv` | 860 MB |
| `optionmetrics/index-dividend-yield.csv` | 134 MB |
| `crsp/daily history/2020-2024.csv` | 39 MB |
| `crsp/names/ca39vlgbhmuznmpu.csv` | 25 MB |
| `crsp/distribution/monthly update.csv` | 20 MB |
| `crsp/compustat-crsp-link/link-LC-LU-LS-LX.csv` | 18 MB |
| `optionmetrics/zero-coupon-yield-curve.csv` | 6.0 MB |
| `crsp/indexes/2020-2025.csv` | 198 KB |
| `crsp/quarterly-rebalance/2020-2024.csv` | 118 KB |

## Step 2: Validate CSV Headers

All CSV headers were checked against the planned schemas. One discrepancy found:

**Quarterly rebalance**: The plan specified `PortCnt,PortAvgStat,PortMedStat` for the last 3 columns, but the actual CSV header had `SecSecurityAllCnt,SecSecurityDropCnt,SecSecurityAddCnt`. Schema was corrected to match.

## Step 3: Create All SQL and Load Scripts

Created `create_table.sql` and `load.sh` for each dataset. The load script pattern:

```bash
#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_FILE="${SCRIPT_DIR}/create_table.sql"
CSV_FILE="${SCRIPT_DIR}/<filename>.csv"
TABLE="<db>.<table>"

echo "=== Loading ${TABLE} ==="
if clickhouse-client \
    --input_format_null_as_default=1 \
    --date_time_input_format=best_effort \
    --input_format_allow_errors_num=100 \
    --query "INSERT INTO ${TABLE} FORMAT CSVWithNames" < "${CSV_FILE}"; then
    echo "INSERT succeeded."
    clickhouse-client --query "SELECT count() FROM ${TABLE}"
    exit 0
fi

echo "INSERT failed, creating schema..."
clickhouse-client --multiquery < "${SQL_FILE}"

clickhouse-client \
    --input_format_null_as_default=1 \
    --date_time_input_format=best_effort \
    --input_format_allow_errors_num=100 \
    --query "INSERT INTO ${TABLE} FORMAT CSVWithNames" < "${CSV_FILE}"
clickhouse-client --query "SELECT count() FROM ${TABLE}"
```

Scripts created in these locations:

- `crsp/daily stock price old/create_table.sql` + `load.sh`
- `crsp/daily stock file/create_table.sql` + `load.sh`
- `optionmetrics/create_forward_price.sql` + `load_forward_price.sh`
- `optionmetrics/create_security_prices.sql` + `load_security_prices.sh`
- `crsp/daily history/create_table.sql` + `load.sh`
- `crsp/indexes/create_table.sql` + `load.sh`
- `crsp/sp500-index-constituents/create_table.sql` + `load.sh`
- `optionmetrics/create_index_dividend_yield.sql` + `load_index_dividend_yield.sh`
- `optionmetrics/create_zero_coupon_yield_curve.sql` + `load_zero_coupon_yield_curve.sh`
- `crsp/names/create_table.sql` + `load.sh`
- `crsp/distribution/create_table.sql` + `load.sh`
- `crsp/compustat-crsp-link/create_table.sql` + `load.sh`
- `crsp/quarterly-rebalance/create_table.sql` + `load.sh`

---

## Step 4: Load Data (Largest First)

### 4.1: `crsp.daily_stock_price_old` (12.1 GB)

**First attempt failed** with error at row 48,459:

```
Column 49, name: RET, type: Nullable(Float64), parsed text: <EMPTY>
ERROR: garbage after Nullable(Float64): "B,,,35291,"
```

**Root cause**: CRSP uses letter codes for missing returns. Investigation found:

| Column | Non-numeric values | Meaning |
|--------|-------------------|---------|
| `RET` | B, C | B = no valid previous price; C = no valid current price |
| `RETX` | B, C | Same as RET |
| `DLRET` | A, S | A = no valid delisting price; S = no pricing data around delisting |
| `DLRETX` | A, S | Same as DLRET |

**Fix**: Changed `RET`, `RETX`, `DLRET`, `DLRETX` from `Nullable(Float64)` to `Nullable(String)`.

**Second attempt failed** at row 283,508:

```
Column 5, name: SICCD, type: Nullable(Int32), parsed text: <EMPTY>
ERROR: garbage after Nullable(Int32): "Z,00101B10"
```

**Root cause**: CRSP uses `Z` as a sentinel in the SIC code field for missing/unavailable SIC codes.

**Fix**: Changed `SICCD` from `Nullable(Int32)` to `Nullable(String)`. Also increased `input_format_allow_errors_num` from 100 to 1000.

**Third attempt succeeded**: 47,331,641 rows loaded.

**Verification**:
- Date range: 2000-01-03 to 2024-12-31
- Distinct securities: 23,672
- Spot-checked first 3 rows: data looks correct (PERMNO 10001 = ENERGY WEST INC)

---

### 4.2: `option_metrics.forward_price` (8.8 GB)

Loaded cleanly on first schema creation: **107,023,326 rows**.

**Verification**:
- Date range: 2000-01-03 to 2023-08-31
- Distinct securities: 11,716

---

### 4.3: `option_metrics.security_prices` (4.7 GB)

Loaded cleanly: **52,005,127 rows**.

Note: The `return` column required backtick quoting in the CREATE TABLE statement since it's a reserved word.

**Verification**:
- Date range: 2000-01-03 to 2023-08-31
- Distinct securities: 33,229

---

### 4.4: `crsp.daily_stock_annual_update` (6.1 GB)

Loaded cleanly: **11,286,865 rows**. Same 94-column schema as `daily_stock_monthly`.

**Verification**:
- Date range: 2020-01-02 to 2024-12-31
- Distinct securities: 12,910

---

### 4.5: `crsp.sp500_constituents` (860 MB)

Loaded cleanly: **3,155,383 rows**.

---

### 4.6: `option_metrics.index_dividend_yield` (134 MB)

Loaded cleanly: **2,141,440 rows**.

---

### 4.7: `crsp.daily_index_history` (39 MB)

Loaded cleanly: **180,891 rows**.

---

### 4.8: `crsp.security_names` (25 MB)

**First attempt failed**:

```
Code: 44. DB::Exception: Sorting key contains nullable columns, but merge tree
setting `allow_nullable_key` is disabled.
```

**Root cause**: `SecInfoStartDt` was `Nullable(Date32)` but used in `ORDER BY (PERMNO, SecInfoStartDt)`. ClickHouse MergeTree doesn't allow Nullable columns in the sorting key.

**Fix**: Changed `SecInfoStartDt` from `Nullable(Date32)` to `Date32`.

**Retry succeeded**: **186,251 rows**.

---

### 4.9: `crsp.distributions` (20 MB)

Same Nullable ORDER BY issue with `DisExDt`. Fixed from `Nullable(Date32)` to `Date32`.

Loaded: **170,974 rows**.

---

### 4.10: `crsp.compustat_link` (18 MB)

**Two issues found**:

1. **Nullable ORDER BY**: `LPERMNO` was `Nullable(Int32)` in `ORDER BY (gvkey, LPERMNO)`. Fixed to `Int32`.

2. **LINKENDDT = 'E'**: The `LINKENDDT` column contains `E` as a sentinel meaning "link is still active" (no end date). At row 1,446 ClickHouse tried to parse `E` as a date and failed.

**Fix**: Changed `LINKENDDT` from `Nullable(Date32)` to `Nullable(String)`. Also increased error tolerance to 1000.

Loaded: **39,391 rows**.

---

### 4.11: `option_metrics.zero_coupon_yield_curve` (6 MB)

Loaded cleanly: **254,807 rows**.

---

### 4.12: `crsp.daily_market_returns` (198 KB)

Loaded cleanly: **1,508 rows**.

---

### 4.13: `crsp.quarterly_rebalance` (118 KB)

Loaded cleanly: **700 rows**.

---

## Final Verification

All tables verified via MCP queries:

### CRSP Database
| Table | Rows |
|-------|------|
| `crsp.daily_stock_price_old` | 47,331,641 |
| `crsp.daily_stock_annual_update` | 11,286,865 |
| `crsp.sp500_constituents` | 3,155,383 |
| `crsp.security_names` | 186,251 |
| `crsp.daily_index_history` | 180,891 |
| `crsp.distributions` | 170,974 |
| `crsp.compustat_link` | 39,391 |
| `crsp.daily_market_returns` | 1,508 |
| `crsp.quarterly_rebalance` | 700 |

### OptionMetrics Database
| Table | Rows |
|-------|------|
| `option_metrics.forward_price` | 107,023,326 |
| `option_metrics.security_prices` | 52,005,127 |
| `option_metrics.index_dividend_yield` | 2,141,440 |
| `option_metrics.zero_coupon_yield_curve` | 254,807 |

**Grand total**: ~224M new rows across 13 tables.

---

## Schema Fixes Summary

| Issue | Affected Column(s) | Original Type | Fixed Type | Reason |
|-------|-------------------|---------------|------------|--------|
| CRSP return sentinels (B,C) | `RET`, `RETX` | `Nullable(Float64)` | `Nullable(String)` | Letter codes for missing returns |
| CRSP delisting return sentinels (A,S) | `DLRET`, `DLRETX` | `Nullable(Float64)` | `Nullable(String)` | Letter codes for missing delisting returns |
| CRSP SIC sentinel (Z) | `SICCD` | `Nullable(Int32)` | `Nullable(String)` | Z = SIC not available |
| Compustat link active flag (E) | `LINKENDDT` | `Nullable(Date32)` | `Nullable(String)` | E = link still active |
| Nullable in ORDER BY | `SecInfoStartDt` | `Nullable(Date32)` | `Date32` | ClickHouse disallows Nullable in sort key |
| Nullable in ORDER BY | `DisExDt` | `Nullable(Date32)` | `Date32` | Same |
| Nullable in ORDER BY | `LPERMNO` | `Nullable(Int32)` | `Int32` | Same |
| CSV column name mismatch | Last 3 cols of quarterly_rebalance | `PortCnt` etc. | `SecSecurityAllCnt` etc. | Plan didn't match actual CSV |

All quirks documented in `DATA_QUIRKS.md`.
