#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_FILE="${SCRIPT_DIR}/create_table.sql"
CSV_FILE="${SCRIPT_DIR}/2020-2024.csv"
TABLE="crsp.daily_index_history"

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
