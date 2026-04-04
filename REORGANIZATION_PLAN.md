# Repository Reorganization Plan

## Context

The `wrds-data` repo has grown into two unrelated concerns: **WRDS data** (ClickHouse schemas/loaders) and **LSEG Datastream** (API download scripts). We'll split them into two git repos under a shared workspace root (`market-data-library/`) so Claude Code, MCP tools, and project context stay unified.

The existing `wrds-data` repo keeps its full git history — we just remove the LSEG files and commit. The new `lseg` repo gets its history extracted via `git filter-repo`.

---

## Final Structure

```
market-data-library/                  # Workspace root (NOT a git repo)
├── CLAUDE.md                         # Full project context (both sides)
├── .claude/                          # Claude config, memory, plans
├── .mcp.json                         # MCP tools (ClickHouse access)
├── market-data-library.code-workspace
├── bbg/                              # Bloomberg validation (standalone)
│
├── wrds-data/                        # git repo 1 (EXISTING repo, history preserved)
│   ├── .git/
│   ├── .gitignore
│   ├── DATA_QUIRKS.md
│   ├── chat history remaining data loading.md
│   ├── crsp/                         # (10 subdirs, unchanged internally)
│   ├── compustat/                    # (3 subdirs, unchanged internally)
│   ├── option_metrics/               # (8+ subdirs, unchanged internally)
│   └── ravenpack/                    # (placeholder, from WRDS)
│
└── lseg/                             # git repo 2 (NEW, history via filter-repo)
    ├── .git/
    ├── .gitignore
    ├── .env                          # LSEG credentials (gitignored)
    ├── BOND_DATA_RESEARCH.md
    │
    ├── shared/                       # Shared utilities across sub-projects
    │   ├── __init__.py               # Package init with ENV_PATH helper
    │   └── lseg_rest_api.py          # REST client (imported by 14 scripts)
    │
    ├── dividend_derivatives/         # Div futures + options (SDA/SDI/FEXD)
    │   ├── enumerate_div_contracts.py        # (was enumerate_instruments.py)
    │   ├── enumerate_div_contracts_v2.py     # (was enumerate_instruments_v2.py)
    │   ├── enumerate_div_contracts_v3.py     # (was enumerate_instruments_v3.py)
    │   ├── enumerate_expired_div.py          # (was enumerate_expired.py)
    │   ├── build_div_master.py               # (was build_instrument_master.py)
    │   ├── build_expired_div_options.py      # (was build_expired_options_master.py)
    │   ├── build_secmaster.py                # (unchanged — links LSEG RICs to CRSP PERMNOs)
    │   ├── download_div_futures.py           # (was download_futures_retry.py)
    │   ├── download_div_options.py           # (was download_options_prices.py)
    │   ├── find_expired_div_options.py       # (was find_expired_options.py)
    │   ├── explore_div_fields.py             # (was explore_fields.py)
    │   ├── explore_div_fields_v2.py          # (was explore_fields_v2.py)
    │   ├── explore_div_fields_v3.py          # (was explore_fields_v3.py)
    │   ├── explore_ric_history.py            # (unchanged)
    │   ├── explore_secmaster.py              # (unchanged)
    │   ├── explore_symbology.py              # (unchanged)
    │   ├── explore_symbology_v2.py           # (unchanged)
    │   ├── test_dividend_futures.py          # (unchanged)
    │   ├── test_expired_option_rics.py       # (unchanged)
    │   └── NOTES.md
    │
    ├── equity_options/               # Intraday options pipeline
    │   ├── download_om_minute_bars.py
    │   ├── download_trades.py
    │   ├── download_minute_bars.py
    │   ├── download_option_ticks.py
    │   ├── download_spy_ticks.py
    │   ├── build_ticker_contracts.py
    │   ├── build_om_rics.py
    │   ├── pregen_om_contracts.py
    │   ├── explore_option_chains.py
    │   ├── fetch_intraday_options.py
    │   ├── get_current_prices.py
    │   ├── probe_expired_trades.py
    │   ├── run_all_om_bars.sh
    │   ├── run_all_trades.sh
    │   ├── run_minute_bars_chain.sh
    │   ├── download_all_tickers.sh
    │   ├── wait_then_run_all.sh
    │   ├── notes.md
    │   ├── HANDOFF_OM_MINUTE_BARS.md
    │   ├── INDEX_RIC_INVESTIGATION.md
    │   ├── TODO.md
    │   ├── data -> (symlink to expansion drive)
    │   └── expired_options_search/   # (renamed from "expired options search")
    │       ├── build_cboe_contracts.py
    │       └── eof_scripts/          # (renamed from "eof scripts")
    │           └── (16 probe/build/parse scripts)
    │
    ├── credit/                       # Bond/credit market data
    │   ├── download_bond_master.py
    │   ├── test_bond_pricing.py
    │   ├── test_bond_expired.py
    │   ├── test_bond_expired2.py
    │   ├── test_bond_scope.py
    │   ├── test_bond_scope2.py
    │   ├── test_bond_search.py
    │   ├── test_bond_history_depth.py
    │   └── test_bond_master_run.py
    │
    ├── tests/                        # General LSEG connectivity tests
    │   ├── test_lseg.py
    │   └── test_rest_api.py
    │
    └── archive/                      # Deprecated DSWS scripts
        ├── download_futures_prices.py
        ├── test_dsws.py
        └── python web service docs.pdf
```

---

## Dividend Derivative Renames

These files currently have generic names ("instruments", "options", "futures") that are ambiguous alongside the equity options pipeline. Renaming adds `div_` to make the domain clear:

| Current name | New name | Reason |
|---|---|---|
| `enumerate_instruments.py` | `enumerate_div_contracts.py` | Enumerates SDA/SDI/FEXD contracts |
| `enumerate_instruments_v2.py` | `enumerate_div_contracts_v2.py` | Follow-up for FEXD options |
| `enumerate_instruments_v3.py` | `enumerate_div_contracts_v3.py` | Refined FEXD queries |
| `enumerate_expired.py` | `enumerate_expired_div.py` | Expired div futures search |
| `build_instrument_master.py` | `build_div_master.py` | Builds clean div futures/options masters |
| `build_expired_options_master.py` | `build_expired_div_options.py` | Brute-force expired div option RICs |
| `download_futures_retry.py` | `download_div_futures.py` | Daily OHLCV for div futures |
| `download_options_prices.py` | `download_div_options.py` | Daily prices for div options |
| `find_expired_options.py` | `find_expired_div_options.py` | REST search for expired div options |
| `explore_fields.py` | `explore_div_fields.py` | Div futures field exploration |
| `explore_fields_v2.py` | `explore_div_fields_v2.py` | Field exploration v2 |
| `explore_fields_v3.py` | `explore_div_fields_v3.py` | Field exploration v3 |

Files that keep their names (already clear or generic utilities):
- `build_secmaster.py` — links LSEG RICs to CRSP PERMNOs (not div-specific)
- `explore_ric_history.py`, `explore_secmaster.py`, `explore_symbology.py`, `explore_symbology_v2.py` — generic LSEG utilities
- `test_dividend_futures.py`, `test_expired_option_rics.py` — already well-named

---

## Datafeed Machine Safety Analysis

### Why equity_options/ is safe to move

All scripts use **relative paths from `__file__`** to locate data:

**Python scripts** (`download_om_minute_bars.py`, `download_trades.py`):
```python
script_dir = os.path.dirname(os.path.abspath(__file__))
base_dir   = os.path.join(script_dir, "data", ticker)
```
Log files (`om_bars_log.jsonl`, `trades_log.jsonl`), CSVs, and progress logs are all under `data/{TICKER}/` relative to the script. Moving the script directory preserves these relationships.

**Shell orchestrators** (`run_all_om_bars.sh`, `run_all_trades.sh`):
```bash
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"
```
Then reference `data/$ticker/om_run.log`, `all_tickers.csv`, etc. All relative to `$SCRIPT_DIR`.

**The `data/` symlink** points to the expansion drive:
```
data -> /media/datafeed/Expansion/LSEG-data/intraday options data/data
```
The symlink itself is a tracked file — `git mv` preserves it. The target on the expansion drive does NOT change.

**Conclusion:** As long as the directory structure within `equity_options/` is preserved (scripts + `data/` symlink + gitignored CSVs like `all_tickers.csv` in the same directory), everything works from any absolute location on disk.

### One path that needs updating

`build_ticker_contracts.py` has:
```python
SEARCH_DIR = os.path.join(SCRIPT_DIR, "..", "expired options search")
```
After the move, `expired options search/` will be a subdirectory of `equity_options/` (renamed `expired_options_search/`), so this becomes:
```python
SEARCH_DIR = os.path.join(SCRIPT_DIR, "expired_options_search")
```

### What needs to happen on the datafeed machine

1. Stop running orchestrators (between tickers — resume-safe, logs track completion)
2. `git pull` in the `lseg/` repo (or fresh clone)
3. Copy `.env` to `lseg/.env`
4. Ensure `data/` symlink exists at `lseg/equity_options/data`
5. Copy gitignored files to new locations (see Phase 6 below)
6. Restart: `cd lseg/equity_options && nohup ./run_all_om_bars.sh 8 &`

---

## Code Changes Required

### 1. Import updates for `lseg_rest_api.py` (14 scripts)

Each changes from:
```python
from lseg_rest_api import LSEGRestClient
```
to:
```python
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from shared.lseg_rest_api import LSEGRestClient
```

**Affected scripts:**
- **dividend_derivatives/**: `build_secmaster.py`, `explore_ric_history.py`, `find_expired_div_options.py`, `test_expired_option_rics.py`
- **credit/**: `test_bond_pricing.py`, `test_bond_expired.py`, `test_bond_expired2.py`, `test_bond_scope.py`, `test_bond_scope2.py`, `test_bond_search.py`, `test_bond_history_depth.py`, `test_bond_master_run.py`
- **equity_options/**: `fetch_intraday_options.py`
- **tests/**: `test_rest_api.py`

### 2. Hardcoded `.env` path updates (5 eof scripts)

Change from: `load_dotenv(dotenv_path="/home/rakin/wrds-data/LSEG datastream/.env")`
Change to: `load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))`

Scripts: `probe_nvda_alt_dates.py`, `probe_master_gap_rics.py`, `probe_ric_file.py`, `probe_weekly_rics.py`, `probe_rics.py`

### 3. `download_bond_master.py` .env path

Change from: `load_dotenv(os.path.join(OUTDIR, ".env"))`
Change to: `load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))`

### 4. `build_ticker_contracts.py` SEARCH_DIR

Change from: `SEARCH_DIR = os.path.join(SCRIPT_DIR, "..", "expired options search")`
Change to: `SEARCH_DIR = os.path.join(SCRIPT_DIR, "expired_options_search")`

### 5. `lseg-data.config.json` generation

Several scripts write this to `os.path.dirname(os.path.abspath(__file__))` at runtime. After the move, they'll write it in their respective subdirectories. This is fine — each script creates and uses its own config file.

### 6. Create `shared/__init__.py`

```python
import os
SHARED_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SHARED_DIR)
ENV_PATH = os.path.join(REPO_ROOT, ".env")
```

---

## Migration Sequence

### Phase 1: Create workspace root & move wrds-data into it

```bash
mkdir ~/market-data-library
mv ~/wrds-data ~/market-data-library/wrds-data
cd ~/market-data-library/wrds-data
```

Move root-level non-WRDS files out to workspace root:
- `CLAUDE.md` → `market-data-library/CLAUDE.md`
- `.claude/` → `market-data-library/.claude/`
- `.mcp.json` → `market-data-library/.mcp.json`
- `wrds-data.code-workspace` → rename to `market-data-library.code-workspace`
- `bbg/` → `market-data-library/bbg/`
- `.vscode/` → `market-data-library/.vscode/`
- `.jupyter_ystore.db` → `market-data-library/` (if needed)

### Phase 2: Extract LSEG repo with history via git filter-repo

```bash
cd ~/market-data-library
git clone wrds-data lseg
cd lseg
git filter-repo --path "LSEG datastream/" --path "LSEG_BOND_DATA_RESEARCH.md"
```

This produces a repo with only commits that touched LSEG files, preserving blame history. Paths still have the `LSEG datastream/` prefix at this point.

### Phase 3: Clean LSEG files out of wrds-data

```bash
cd ~/market-data-library/wrds-data
git rm -r "LSEG datastream/"
git rm "LSEG_BOND_DATA_RESEARCH.md"
git rm CLAUDE.md .mcp.json wrds-data.code-workspace .jupyter_ystore.db
git rm -r bbg/ .vscode/
git rm -r LSEG/   # empty legacy dir
git commit -m "Remove LSEG datastream and non-WRDS files (moved to market-data-library workspace)"
```

### Phase 4: Reorganize within lseg repo

**4a. Flatten "LSEG datastream/" prefix:**
```bash
cd ~/market-data-library/lseg
git mv "LSEG datastream/"* .
rmdir "LSEG datastream"
```

**4b. Create shared/ package:**
```bash
mkdir shared
git mv lseg_rest_api.py shared/
# Write shared/__init__.py (new file)
```

**4c. Create dividend_derivatives/ with renames:**
```bash
mkdir dividend_derivatives
git mv enumerate_instruments.py dividend_derivatives/enumerate_div_contracts.py
git mv enumerate_instruments_v2.py dividend_derivatives/enumerate_div_contracts_v2.py
git mv enumerate_instruments_v3.py dividend_derivatives/enumerate_div_contracts_v3.py
git mv enumerate_expired.py dividend_derivatives/enumerate_expired_div.py
git mv build_instrument_master.py dividend_derivatives/build_div_master.py
git mv build_expired_options_master.py dividend_derivatives/build_expired_div_options.py
git mv build_secmaster.py dividend_derivatives/
git mv download_futures_retry.py dividend_derivatives/download_div_futures.py
git mv download_options_prices.py dividend_derivatives/download_div_options.py
git mv find_expired_options.py dividend_derivatives/find_expired_div_options.py
git mv explore_fields.py dividend_derivatives/explore_div_fields.py
git mv explore_fields_v2.py dividend_derivatives/explore_div_fields_v2.py
git mv explore_fields_v3.py dividend_derivatives/explore_div_fields_v3.py
git mv explore_ric_history.py dividend_derivatives/
git mv explore_secmaster.py dividend_derivatives/
git mv explore_symbology.py dividend_derivatives/
git mv explore_symbology_v2.py dividend_derivatives/
git mv test_dividend_futures.py dividend_derivatives/
git mv test_expired_option_rics.py dividend_derivatives/
git mv NOTES.md dividend_derivatives/
```

**4d. Create equity_options/ from "intraday options data/":**
```bash
git mv "intraday options data" equity_options
git mv "expired options search" equity_options/expired_options_search
git mv equity_options/expired_options_search/"eof scripts" equity_options/expired_options_search/eof_scripts
```

**4e. Create credit/:**
```bash
mkdir credit
git mv download_bond_master.py credit/
git mv test_bond_pricing.py credit/
git mv test_bond_expired.py credit/
git mv test_bond_expired2.py credit/
git mv test_bond_scope.py credit/
git mv test_bond_scope2.py credit/
git mv test_bond_search.py credit/
git mv test_bond_history_depth.py credit/
git mv test_bond_master_run.py credit/
```

**4f. Create tests/:**
```bash
mkdir tests
git mv test_lseg.py tests/
git mv test_rest_api.py tests/
```

**4g. Move remaining files:**
```bash
git mv LSEG_BOND_DATA_RESEARCH.md BOND_DATA_RESEARCH.md
# archive/ already exists from old structure
```

**4h. Commit reorganization:**
```bash
git add -A
git commit -m "Reorganize LSEG pipeline: shared/, dividend_derivatives/, equity_options/, credit/"
```

### Phase 5: Code changes within lseg repo

Apply all changes from the "Code Changes Required" section:
- Update 14 `lseg_rest_api` imports
- Update 5 hardcoded `.env` paths in eof scripts
- Update `download_bond_master.py` .env path
- Update `build_ticker_contracts.py` SEARCH_DIR
- Create `shared/__init__.py`
- Create `lseg/.gitignore`

```bash
git commit -m "Update imports and paths for new directory structure"
```

### Phase 6: Move gitignored files manually

These are NOT in git. Must be moved by hand on the machine where they exist.

**`.env`** → `lseg/.env`

**Bond data** → `lseg/credit/`:
- `bond_security_master.csv` (5 GB), `bond_master_log.jsonl`, `bond_master_progress.log`, `bond_master_nohup.log`

**Dividend derivative CSVs** → `lseg/dividend_derivatives/`:
- `instrument_master_futures.csv`, `instrument_master_options.csv`, `instrument_master_expired_options.csv`
- `enumerated_*.csv` (6 files), `futures_daily_prices.csv`, `options_daily_prices.csv`, `expired_options_daily_prices.csv`
- `sample_*.csv` (5 files), `failed_rics_*.csv` (2 files), `sample_secmaster_sp500.csv`

**Expired options search CSVs** → `lseg/equity_options/expired_options_search/`:
- `all_om_contracts.csv` (283 MB), `all_cboe_contracts.csv` (105 MB), `all_names_gap_rics.csv` (19 MB)
- `all_om_tickers.csv`, `all_names_gap_summary.csv`, `all_names_gap_probe_results.csv`
- `cboe_all_series_20251205.csv` (76 MB), `master_gap_rics_*.csv` (16 files)
- Various probe results and intermediate CSVs

**Equity options gitignored files** → `lseg/equity_options/`:
- `all_tickers.csv` (**critical** — used by `run_all_om_bars.sh`)
- `all_option_contracts.csv`, `option_rics.csv`, `intraday_option_prices.csv`
- `om_all_run.log`, `all_tickers_output.log`, `pregen_om_contracts.log`
- `lseg-data.config.json`
- `data/` symlink moves via git (already handled in Phase 4d)

### Phase 7: Datafeed machine setup

1. Stop running orchestrators (between tickers — resume-safe)
2. Clone `lseg/` repo to datafeed machine (or copy directory)
3. Copy `.env` to `lseg/.env`
4. Verify `data/` symlink at `lseg/equity_options/data` → expansion drive
5. Move gitignored files from old locations to new paths
6. Restart: `cd lseg/equity_options && nohup ./run_all_om_bars.sh 8 &`

### Phase 8: Cleanup & docs

1. Update `market-data-library/CLAUDE.md` with all new paths
2. Delete leftover empty dirs
3. Remove old wrds-data clone on datafeed machine after confirming lseg repo works

---

## Verification

After Phase 5, on research machine:
```bash
find lseg/ -name "*.py" -exec python -c "import py_compile; py_compile.compile('{}', doraise=True)" \;
```

After Phase 7, on datafeed machine:
```bash
cd lseg/equity_options
ls -la data/                          # symlink works?
head -1 data/SPY/contracts.csv        # data accessible?
python -c "from dotenv import load_dotenv; load_dotenv('../.env'); import os; print(os.getenv('DSWS_APPKEY')[:4])"
nohup ./run_all_om_bars.sh 8 &
tail -f om_all_run.log
```

---

## Risks

| Risk | Mitigation |
|------|------------|
| Datafeed jobs break during transition | Phase 7 pauses jobs; orchestrators are resume-safe; all paths are relative to `__file__` |
| `.env` not found after move | Bare `load_dotenv()` walks up dirs; hardcoded paths updated in Phase 5 |
| `all_tickers.csv` missing after move | Gitignored — Phase 6 checklist covers it explicitly |
| `git filter-repo` path issues | Spaces in "LSEG datastream/" need quoting; test on throwaway clone first |
| Gitignored files forgotten | Phase 6 has comprehensive file-by-file checklist |
| Scripts write `lseg-data.config.json` in wrong place | Each script writes relative to `__file__` — works from any directory |
