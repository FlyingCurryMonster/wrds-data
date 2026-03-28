# WRDS Data Project — Claude Context

This file is auto-loaded by Claude Code on any machine. It captures the full
project state so a new session can pick up without losing context.

---

## Infrastructure

### Research Machine (primary)
- Ubuntu, ClickHouse 26.1.2 running locally
- `clickhouse-client` for CLI, MCP tools for SELECT queries on `crsp`, `compustat`, `option-metrics` databases
- Limited disk space — not intended for long-running downloads

### Data Feed Machine (always-on)
- Ubuntu 22.04, dedicated to streaming/downloading data, never sleeps
- 8TB USB expansion drive (~5.7TB free) mounted locally
- Both machines have access to the expansion drive
- ClickHouse **not** installed — scripts must not depend on it

### Repo
- Cloned to both machines
- CSV data, logs, and credentials are `.gitignore`d — transferred separately via expansion drive

---

## ClickHouse Tables (Research Machine)

### CRSP Database
| Table | Rows | Date Range | Notes |
|-------|------|------------|-------|
| `crsp.daily_stock_monthly` | 33.4M | 1991-2019 | 94-col new CIZ format |
| `crsp.daily_stock_price_old` | 47.3M | 2000-2024 | 63-col old format, RET/RETX as String |
| `crsp.daily_stock_annual_update` | 11.3M | 2020-2024 | Same 94-col schema |
| `crsp.sp500_constituents` | 3.16M | 2000-2024 | Daily prices for S&P 500 members |
| `crsp.security_names` | 186K | — | PERMNO name/identifier history |
| `crsp.daily_index_history` | 181K | 2020-2024 | Index-level daily returns |
| `crsp.distributions` | 171K | — | Dividend/distribution events |
| `crsp.compustat_link` | 39K | — | GVKEY-PERMNO mapping, LINKENDDT='E' = active |
| `crsp.daily_market_returns` | 1.5K | 2020-2025 | VW/EW market returns + S&P |
| `crsp.quarterly_rebalance` | 700 | 2020-2024 | Index rebalance stats |

### OptionMetrics Database
| Table | Rows | Date Range |
|-------|------|------------|
| `option_metrics.forward_price` | 107M | 2000-2025-08-29 |
| `option_metrics.security_prices` | 52M | 2000-2025-08-29 |
| `option_metrics.option_pricing` | 4.3B | 1996-2025-08-29 |
| `option_metrics.index_dividend_yield` | 2.1M | 2000-2023 |
| `option_metrics.zero_coupon_yield_curve` | 255K | 2000-2023 |

### Compustat Database
| Table | Notes |
|-------|-------|
| `compustat.secd` | Daily security data |

### Key Data Quirks
- **CRSP return sentinels**: RET/RETX use B,C; DLRET/DLRETX use A,S → stored as String
- **CRSP SICCD sentinel**: Old format uses 'Z' for missing SIC → stored as String
- **Compustat link LINKENDDT='E'**: Means link still active → stored as String
- **ClickHouse ORDER BY**: Cannot use Nullable columns; must make them non-nullable
- **OptionMetrics `return`**: Reserved word, needs backtick quoting
- **Old vs New CRSP schemas**: Completely different column names (PRC vs DlyPrc, etc.)
- CRSP uses negative sentinel values (e.g., -5 in DisPERMCO) → always use signed Int types
- `SecurityBegDt` has dates back to 1925 → must use Date32 (not Date)

---

## LSEG Intraday Options Pipeline

**Working directory:** `LSEG datastream/intraday options data/`

### Goal
Download 1-minute OHLC bars for all expired US equity option contracts via the
LSEG Historical Pricing API, covering the full 1-year retention window.

### LSEG API
- **Endpoint**: `https://api.refinitiv.com/data/historical-pricing/v1/views/intraday-summaries/{RIC}`
- **Retention**: 1-year rolling window from today — bars for contracts expired >1 year ago are gone
- **Rate limit**: 25 req/sec for intraday-summaries; script runs at 23 req/sec with adaptive backoff on 429s
- **Pagination**: 10K bars per request; expired contracts typically fit in 1 request (2 weeks = ~3,900 bars)
- **Auth**: Bearer token via `lseg.data` SDK, credentials in `.env`

### LSEG Expired RIC Format (OPRA equity)
Active RIC: `{ROOT}{month_code}{DD}{YY}{strike_5digits}.U`
Expired RIC: `{active_ric}^{month_code}{YY}`

Month codes (calls): A=Jan B=Feb C=Mar D=Apr E=May F=Jun G=Jul H=Aug I=Sep J=Oct K=Nov L=Dec
Month codes (puts):  M=Jan N=Feb O=Mar P=Apr Q=May R=Jun S=Jul T=Aug U=Sep V=Oct W=Nov X=Dec
Suffix always uses the call-side month code regardless of C/P.

Strike encoding: `strike_price // 10` (OM strike is in tenths of a cent; LSEG drops last digit)

Example: NVDA $120 Call exp 2025-06-20 → `NVDAF202512000.U^F25`

### Known RIC Format Issues
- **NDX, SPX, RUT, RUTW, SPXW, XEO, OEX, XND, MXEA**: CBOE-listed index options use a different RIC format — NOT OPRA equity format. Return 0 bars. All skipped (stamped COMPLETE). See `INDEX_RIC_INVESTIGATION.md`.
- **XSP**: RIC format is correct (OPRA) but skipped — 34% zero-bar rate across 71K contracts, took 3+ days without finishing. Stamped COMPLETE.
- **MRUT**: Works correctly with OPRA format despite being an index product (micro-sized, structured differently maybe?).

### Contract Sources (Three Periods)

| Period | Source | File | Contracts |
|--------|--------|------|-----------|
| Mar 25 2025 – Mar 25 2026 | OptionMetrics `option_pricing` table | `expired options search/all_om_contracts.csv` | 4.12M |
| Aug 30 – Dec 4 2025 | CBOE Dec 5 Wayback snapshot + brute-force probe | `expired options search/all_names_gap_rics.csv` | 482K |
| Dec 5 2025 – present | CBOE Dec 5 Wayback snapshot | `expired options search/all_cboe_contracts.csv` | 1.73M (1.09M in window) |

All three files have `base_ric` and `query_ric` columns and require no ClickHouse on the data feed machine.

Note: `all_om_contracts.csv` covers Mar 25 2025 – Mar 25 2026 (scoped to the 1-year LSEG retention window). It includes contracts expiring after Aug 29 2025 that were already listed in OM at snapshot time — these overlap with the gap and CBOE periods.

### Gap Period (Aug 30 – Dec 4 2025)
OptionMetrics ends 2025-08-29. To cover the gap before the CBOE Dec 5 snapshot:
- Used CBOE Dec 5 strike ladders + calendar-generated expiry dates
- Probed 482,160 RICs across 672 names — **96.9% hit rate** (467,336 confirmed)
- 14,824 errors remain to be re-probed (session token expired near end of run)
- Monthly-only names (~4,653 tickers) skipped — their Nov 2025 monthly already in OM
- Files in `LSEG datastream/expired options search/`

### Download Pipeline

**Main script**: `download_om_minute_bars.py TICKER [WORKERS] [--csv PATH]`
- Reads contracts from `data/{TICKER}/contracts.csv` (pre-built per-ticker file, no ClickHouse)
- Downloads full history — no time cap; paginates until LSEG returns no more data
- Column names discovered dynamically from API response headers (not hardcoded)
- Outputs: `data/{TICKER}/om_minute_bars.csv`, `data/{TICKER}/om_bars_log.jsonl` (resume), `data/{TICKER}/om_bars_progress.log`
- Resume: reads `om_bars_log.jsonl` to skip completed contracts — safe to kill/restart

**Per-ticker contract files**: `data/{TICKER}/contracts.csv`
- Built by `build_ticker_contracts.py` — merges all three source files, deduplicates by `base_ric`
- Columns: `base_ric, query_ric, source` (source = om/cboe/gap)
- 6,570 tickers total, 5.9M contracts across all sources
- Run `python build_ticker_contracts.py` to rebuild if source files change

**Orchestrator**: `run_all_om_bars.sh`
- Iterates through `all_tickers.csv` (6,570 tickers ordered by contract count desc, all sources)
- Skips tickers with `COMPLETE` in their `om_run.log`
- Launched via `nohup`, survives terminal/Claude restarts

**Progress check**:
```bash
COMPLETED=$(grep -l "COMPLETE" data/*/om_run.log 2>/dev/null | wc -l)
ACTIVE=$(ps aux | grep "download_om_minute_bars" | grep -v grep | awk '{print $13}')
tail -2 "data/$ACTIVE/om_run.log"
```

### Download Status (as of 2026-03-28)
- **Scale**: 5.9M contracts across 6,570 tickers (OM + CBOE + gap)
- **Storage**: `data/` moved to expansion drive; symlinked from repo — `data/ → /media/datafeed/Expansion/LSEG-data/intraday options data/data/`. All writes go to expansion (~5 TB free).
- **2-week CSVs**: 61 research-machine tickers preserved as `om_minute_bars_2week.csv` in their respective dirs on expansion.
- **Current status**: RUNNING — nohup PID 414062; currently processing IWM (resuming mid-download)
- **Skipped (wrong RIC format, 0 bars)**: NDX, SPX, RUT, RUTW, SPXW, XEO, OEX, XND, MXEA, CBTXW — stamped COMPLETE. XSP also skipped (34% zero-bar rate, too slow). See INDEX_RIC_INVESTIGATION.md.
- **Storage estimate**: ~106 bytes/bar; realistic total ~3 TB (most contracts are short-dated)

### Tick Data (separate from bars)
- Trade tick retention: ~3 months; quote tick retention: ~2.5 weeks
- Much shorter retention window — lower priority than bars
- Script exists: `download_spy_ticks.py` (SPY in progress as of 2026-03-18)

---

## Migration Plan: Research Machine → Data Feed Machine

### Files to transfer via expansion drive
1. `LSEG datastream/expired options search/all_om_contracts.csv` — 4.12M OM contracts + RICs
2. `LSEG datastream/expired options search/all_names_gap_rics.csv` — 482K gap RICs
3. `LSEG datastream/expired options search/all_cboe_contracts.csv` — 1.73M CBOE Dec 5 contracts + RICs (built by `build_cboe_contracts.py`)
4. `LSEG datastream/expired options search/all_om_tickers.csv` — master ticker list
5. All `data/{TICKER}/om_bars_log.jsonl` files — resume checkpoints
6. All `data/{TICKER}/om_minute_bars.csv` files — data downloaded so far
7. `.env` — LSEG credentials

### Scripts (via git clone — already in repo)
- `download_om_minute_bars.py` — refactored (CSV-based, full history, dynamic columns)
- `run_all_om_bars.sh` — orchestrator
- `pregen_om_contracts.py` — no longer needed after migration (contracts pre-generated)
- `build_om_rics.py` — used to add RIC columns to raw ClickHouse export
- `LSEG datastream/expired options search/build_cboe_contracts.py` — used to parse CBOE snapshot

### Script modification status
`download_om_minute_bars.py` has been updated (2026-03-26):
- Reads from CSV (no ClickHouse), supports all three source files via `--csv` flag
- Downloads full history (1-year LSEG retention window, was 2-week limited before)
- Column names discovered dynamically from API response headers

---

## Key File Locations

```
LSEG datastream/
├── intraday options data/
│   ├── download_om_minute_bars.py     # main bar download script
│   ├── run_all_om_bars.sh             # orchestrator (6,570 tickers, all sources)
│   ├── build_ticker_contracts.py      # merges 3 source CSVs into per-ticker contracts.csv
│   ├── all_tickers.csv                # 6,570 tickers ordered by contract count (all sources)
│   ├── pregen_om_contracts.py         # (used to generate all_om_contracts.csv)
│   ├── build_om_rics.py               # (used to add RIC columns)
│   ├── notes.md                       # full API/RIC technical reference
│   ├── HANDOFF_OM_MINUTE_BARS.md      # download job handoff doc
│   ├── INDEX_RIC_INVESTIGATION.md     # SPX/NDX/RUT/RUTW RIC format investigation
│   ├── om_all_run.log                 # global orchestrator log
│   └── data/
│       └── {TICKER}/
│           ├── contracts.csv          # all RICs for this ticker (OM + CBOE + gap)
│           ├── om_minute_bars.csv     # downloaded bar data
│           ├── om_bars_log.jsonl      # resume checkpoint (one entry per contract)
│           ├── om_bars_progress.log   # timestamped throughput log
│           └── om_run.log             # per-ticker run log
└── expired options search/
    ├── all_om_contracts.csv           # 4.12M OM contracts with RICs
    ├── all_om_tickers.csv             # 6,118 OM-only tickers
    ├── all_cboe_contracts.csv         # 1.73M CBOE contracts with LSEG RICs
    ├── all_names_gap_rics.csv         # 482K gap period RIC candidates
    ├── all_names_gap_probe_results.csv # probe results (96.9% hit rate)
    ├── cboe_all_series_20251205.csv   # raw CBOE snapshot, 1.73M rows
    ├── master_gap_rics_all.csv        # confirmed RICs for 16 core names
    ├── build_cboe_contracts.py        # parses CBOE snapshot → RICs
    └── eof scripts/
        └── (gap RIC construction scripts)
```

---

## TODO

### Immediate
- [x] Modify `download_om_minute_bars.py` to read contracts from CSV (ClickHouse removed; full history; columns discovered dynamically from API response)
- [x] Build per-ticker `contracts.csv` files merging all 3 sources (`build_ticker_contracts.py`)
- [x] Transfer files from expansion drive and resume download on data feed machine
- [x] Install Python deps (`pip install requests python-dotenv lseg-data`)
- [x] Download job running on data feed machine
- [x] Re-download the 52 liquid tickers from research machine — reset and queued (2-week CSVs preserved as `om_minute_bars_2wk.csv` on expansion drive)

### Pending
- [ ] Re-probe 14,824 errored rows in `all_names_gap_probe_results.csv`
- [ ] Download bars for gap period contracts (`all_names_gap_rics.csv`)
- [ ] Download bars for CBOE Dec 2025–Mar 2026 contracts (`all_cboe_contracts.csv`)
- [ ] Investigate correct RIC format for NDX, SPX, RUT, RUTW index options (see `INDEX_RIC_INVESTIGATION.md`)
- [ ] Download daily bars (greeks + IV) for all contracts — separate pipeline, retained indefinitely
- [ ] OM data extension: forward_price/security_prices/option_pricing end 2025-08-29, need 2025-08-30 to present from WRDS
- [ ] GICS historical classifications: `comp.co_hgic` table not yet downloaded

### Investigations
- [ ] OM security_prices anomaly Jul 2007–Mar 2008: security count spikes ~11K→15K then drops to ~8K
