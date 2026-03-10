#!/usr/bin/env bash
# =============================================================================
# Dump option_metrics.option_pricing (old table with greeks) to cold storage.
#
# Usage:
#   ./dump_option_pricing.sh START_DATE END_DATE [OPTIONS]
#
# Options:
#   --format parquet   Parquet with internal zstd compression (default)
#   --format native    ClickHouse Native piped through zstd -19
#   --dry-run          Show row count only, don't dump
#
# Reload commands:
#   Parquet: clickhouse-client --query "INSERT INTO option_metrics.option_pricing FORMAT Parquet" < file.parquet
#   Native:  zstd -d < file.native.zst | clickhouse-client --query "INSERT INTO option_metrics.option_pricing FORMAT Native"
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DUMP_DIR="${SCRIPT_DIR}/dumps"
TABLE="option_metrics.option_pricing"
FORMAT="parquet"
DRY_RUN=false
GREEKS_ONLY=false

# --- Parse arguments ---
if [[ $# -lt 2 ]]; then
    echo "Usage: $0 START_DATE END_DATE [--format parquet|native] [--dry-run]"
    exit 1
fi

START_DATE="$1"
END_DATE="$2"
shift 2

while [[ $# -gt 0 ]]; do
    case "$1" in
        --format)
            FORMAT="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --greeks-only)
            GREEKS_ONLY=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# --- Validate format ---
if [[ "$FORMAT" != "parquet" && "$FORMAT" != "native" ]]; then
    echo "Error: --format must be 'parquet' or 'native'"
    exit 1
fi

# --- Build filename ---
START_COMPACT="${START_DATE//-/}"
END_COMPACT="${END_DATE//-/}"

PREFIX="option_pricing"
if $GREEKS_ONLY; then
    PREFIX="option_pricing_greeks_only"
fi

if [[ "$FORMAT" == "parquet" ]]; then
    OUTPUT_FILE="${DUMP_DIR}/${PREFIX}_${START_COMPACT}_${END_COMPACT}.parquet"
else
    OUTPUT_FILE="${DUMP_DIR}/${PREFIX}_${START_COMPACT}_${END_COMPACT}.native.zst"
fi

# --- Row count ---
echo "Counting rows in ${TABLE} for ${START_DATE} to ${END_DATE}..."
ROW_COUNT=$(clickhouse-client --query \
    "SELECT count() FROM ${TABLE} WHERE date >= '${START_DATE}' AND date <= '${END_DATE}'")
echo "Row count: ${ROW_COUNT}"

if $DRY_RUN; then
    echo "(dry run — no dump performed)"
    exit 0
fi

# --- Dump ---
mkdir -p "$DUMP_DIR"
echo "Dumping to ${OUTPUT_FILE} (format: ${FORMAT})..."

if $GREEKS_ONLY; then
    COLUMNS="optionid, date, delta, gamma, theta, vega"
else
    COLUMNS="*"
fi
QUERY="SELECT ${COLUMNS} FROM ${TABLE} WHERE date >= '${START_DATE}' AND date <= '${END_DATE}'"

if [[ "$FORMAT" == "parquet" ]]; then
    clickhouse-client \
        --query "$QUERY" \
        --format Parquet \
        --output_format_parquet_compression_method zstd \
        --output_format_parquet_row_group_size 1000000 \
        > "$OUTPUT_FILE"
else
    clickhouse-client \
        --query "$QUERY" \
        --format Native \
        | zstd -19 > "$OUTPUT_FILE"
fi

# --- Summary ---
FILE_SIZE=$(du -h "$OUTPUT_FILE" | cut -f1)
echo "Done. File: ${OUTPUT_FILE}"
echo "Size: ${FILE_SIZE}"
echo ""
echo "To reload:"
if [[ "$FORMAT" == "parquet" ]]; then
    echo "  clickhouse-client --query \"INSERT INTO ${TABLE} FORMAT Parquet\" < ${OUTPUT_FILE}"
else
    echo "  zstd -d < ${OUTPUT_FILE} | clickhouse-client --query \"INSERT INTO ${TABLE} FORMAT Native\""
fi
