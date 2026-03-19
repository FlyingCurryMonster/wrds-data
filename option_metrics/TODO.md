# OptionMetrics TODO

## Current Tables & Status

| Table | Rows | Date Range | Status |
|-------|------|------------|--------|
| `option_pricing` | 4.25B | 2000-01-03 – 2025-08-29 | Complete |
| `option_pricing_v2` | 2.59B | partial | Recompression in progress (~61%) |
| `forward_price` | 123M | 2000-01-03 – 2025-08-29 | Complete (~147K dupes to clean) |
| `security_prices` | 57M | 2000-01-03 – 2025-08-29 | Complete |
| `index_dividend_yield` | 2.4M | 2000-01-03 – 2025-08-29 | Complete (see notes) |
| `zero_coupon_yield_curve` | 260K | 2000-01-03 – 2025-08-29 | **Schema change in 2022** (see notes) |
| `security_names` | 272K | 1994-09-01 – 2025-10-22 | Complete |
| `crsp_link` | 122K | — | Lookup table, OK |

### Notes
- **`index_dividend_yield`**: Securities dropped from ~129 (2005) to ~33 (2020+). Likely reflects index coverage changes, not data loss. Worth verifying against WRDS.
- **`zero_coupon_yield_curve`**: Major change in 2022 — tenors dropped from ~2,600 to just 11, rows per year dropped from ~11K to ~2.7K. Likely a methodology/format change by OptionMetrics. Need to verify this is expected.
- **`security_names`**: Covers 120K securities from 1994–2025. This is the name history table (tracks ticker/issuer changes over time).

## Downloads Needed from WRDS

### Gap-filling
- [x] `security_prices`: 2023-09-01 to present ✓ loaded 2025-08-29

### Deduplication
- [ ] `forward_price`: ~147K duplicate rows (by secid, date, expiration), heaviest in 2016-2023

### New Datasets to Evaluate
- [ ] Historical volatility (`HIST_VOL` / `optvol.hist_ivol_*`)
- [ ] Option volume (`OPTION_VOLUME` / `optvol.option_volume`)
- [ ] Standardized options (`STD_OPTIONS` / `optionm.std_*`)
- [ ] Volatility surface (`VOL_SURFACE` / `optionm.vsurfd` / `optionm.vsurfm`)
- [ ] Dividend distribution history (`optionm.distrd`)
- [ ] Exchange listing history (`optionm.exchgd`)
- [ ] Security name history — already have `security_names` but verify it matches `optionm.secnmd` on WRDS

## Investigations
- [ ] `security_prices` anomaly (Jul 2007 – Mar 2008): security count spikes from ~11K to ~16K then drops to ~8K in 2009
- [ ] `zero_coupon_yield_curve` schema change in 2022: tenors dropped from ~2,600 to 11 — verify against WRDS documentation
- [ ] `index_dividend_yield` securities decline over time — verify coverage is expected

## Recompression
- [ ] Finish `option_pricing_v2` migration (drop Greeks, reorder by (optionid, date), Gorilla+ZSTD(3) on floats, Delta+ZSTD(3) on ints). Currently ~61% complete. Target: ~50 GB compressed vs 176 GB original.
