# WRDS Data Quirks and Loading Notes

Observations and fixes encountered while loading WRDS datasets into ClickHouse.

---

## CRSP Return Sentinel Codes

**Affected table**: `crsp.daily_stock_price_old`
**Columns**: `RET`, `RETX`, `DLRET`, `DLRETX`

CRSP uses single-letter codes instead of numeric values when returns are missing or can't be calculated. These columns had to be typed as `Nullable(String)` instead of `Nullable(Float64)`.

| Code | Meaning |
|------|---------|
| `B` | No valid previous price to compute return (e.g., stock was halted/delisted then relisted) |
| `C` | No valid current price to compute return |
| `A` | No valid price available for delisting return |
| `S` | CRSP has no pricing data on the security around the delisting date |

- `B` and `C` appear in `RET` and `RETX`
- `A` and `S` appear in `DLRET` and `DLRETX`

**Impact**: When querying returns, you must filter for numeric values or cast:
```sql
SELECT toFloat64OrNull(RET) AS ret_numeric
FROM crsp.daily_stock_price_old
WHERE RET NOT IN ('B', 'C', '')
```

---

## CRSP SIC Code Sentinel

**Affected table**: `crsp.daily_stock_price_old`
**Column**: `SICCD`

The old-format CRSP data uses `Z` as a sentinel value in the SIC code field, indicating the SIC code is not available or not applicable. This column was typed as `Nullable(String)` instead of `Nullable(Int32)`.

---

## Compustat-CRSP Link: LINKENDDT = 'E'

**Affected table**: `crsp.compustat_link`
**Column**: `LINKENDDT`

The Compustat-CRSP link table uses `E` as the `LINKENDDT` value to indicate the link is still active (no end date). This column was typed as `Nullable(String)` instead of `Nullable(Date32)`.

When filtering for active links:
```sql
-- Active links (still valid)
SELECT * FROM crsp.compustat_link WHERE LINKENDDT = 'E' OR LINKENDDT = ''

-- Links with actual end dates
SELECT * FROM crsp.compustat_link WHERE LINKENDDT != 'E' AND LINKENDDT != ''
```

---

## ClickHouse: Nullable Columns Cannot Be in ORDER BY

**Affected tables**: `crsp.security_names`, `crsp.distributions`, `crsp.compustat_link`

ClickHouse's MergeTree engine does not allow `Nullable` columns in the `ORDER BY` key (unless `allow_nullable_key` is explicitly enabled). Any column used in `ORDER BY` must be non-nullable.

**Fix**: Changed these ORDER BY columns from `Nullable(Date32)` / `Nullable(Int32)` to `Date32` / `Int32`:
- `crsp.security_names`: `SecInfoStartDt` (Date32)
- `crsp.distributions`: `DisExDt` (Date32)
- `crsp.compustat_link`: `LPERMNO` (Int32)

---

## CRSP Date Ranges Require Date32

All CRSP date columns must use `Date32` (not `Date`). The standard ClickHouse `Date` type only supports 1970-2149, but CRSP `SecurityBegDt` has dates back to 1925. `Date32` supports 1900-2299.

---

## CRSP Negative Sentinel Values in Integer Fields

CRSP uses negative values as sentinels in some integer fields (e.g., `-5` in `DisPERMCO`). All integer columns that may contain these sentinels must use signed types (`Int32`, `Int64`) rather than unsigned (`UInt32`, `UInt64`).

---

## OptionMetrics `return` is a Reserved Word

**Affected table**: `option_metrics.security_prices`
**Column**: `return`

The column name `return` is a reserved word in ClickHouse (and SQL generally). It must be quoted with backticks in the CREATE TABLE statement:
```sql
`return` Nullable(Float64)
```

---

## Old vs New CRSP Column Naming

The old CRSP daily stock format (`daily_stock_price_old`) and the new CIZ format (`daily_stock_monthly`, `daily_stock_annual_update`) use completely different column names:

| Old Format | New Format | Description |
|-----------|-----------|-------------|
| `date` | `DlyCalDt` | Trading date |
| `PRC` | `DlyPrc` | Daily price |
| `RET` | `DlyRet` | Daily return |
| `RETX` | `DlyRetx` | Daily return ex-dividend |
| `VOL` | `DlyVol` | Volume |
| `SHROUT` | `ShrOut` | Shares outstanding |
| `BID`/`ASK` | `DlyBid`/`DlyAsk` | Bid/Ask |
| `BIDLO`/`ASKHI` | `DlyLow`/`DlyHigh` | Low/High |
| `OPENPRC` | `DlyOpen` | Opening price |
| `CFACPR`/`CFACSHR` | `DlyFacPrc`/`DisFacPr` | Cumulative adj factors |

The old format also includes delisting fields (`DLAMT`, `DLPDT`, `DLSTCD`, `DLRET`, `DLRETX`, `DLPRC`) inline, whereas the new format has distribution sub-fields (`DisExDt`, `DisType`, etc.).

---

## CSV Loading Settings

Required ClickHouse settings for reliable CSV ingestion of WRDS data:

```
--input_format_null_as_default=1     # Treat empty CSV fields as NULL/default
--date_time_input_format=best_effort # Parse various date formats (YYYY-MM-DD, etc.)
--input_format_allow_errors_num=1000 # Allow up to 1000 parse errors (for large files)
```

The `input_format_allow_errors_num=100` (default in most load scripts) is too tight for files with 40M+ rows. The `daily_stock_price_old` load required bumping this to 1000.

---

## Quarterly Rebalance: Column Name Mismatch

**Affected table**: `crsp.quarterly_rebalance`

The plan originally specified columns `PortCnt`, `PortAvgStat`, `PortMedStat` for the last 3 fields, but the actual CSV header has `SecSecurityAllCnt`, `SecSecurityDropCnt`, `SecSecurityAddCnt`. The schema was corrected to match the actual CSV.
