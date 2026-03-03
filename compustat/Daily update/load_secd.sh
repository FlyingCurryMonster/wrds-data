#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_FILE="${SCRIPT_DIR}/create_secd.sql"
CSV_FILE="${SCRIPT_DIR}/2020-2024.csv"
TABLE="compustat.secd"

echo "=== Compustat Daily Security Data Loader ==="
echo "CSV:   ${CSV_FILE}"
echo "Table: ${TABLE}"
echo ""

# Step 1: Try inserting into existing table
echo "[1/3] Attempting INSERT into ${TABLE}..."
if clickhouse-client --query "INSERT INTO ${TABLE} FORMAT CSVWithNames" < "${CSV_FILE}" 2>/tmp/ch_insert_err.log; then
    echo "INSERT succeeded."
    ROW_COUNT=$(clickhouse-client --query "SELECT count() FROM ${TABLE}")
    echo "Table ${TABLE} now has ${ROW_COUNT} rows."
    exit 0
fi

echo "INSERT failed. Error:"
cat /tmp/ch_insert_err.log
echo ""

# Step 2: Create schema (drop table if it exists, then create)
echo "[2/3] Creating schema from ${SQL_FILE}..."
if ! clickhouse-client --multiquery < "${SQL_FILE}" 2>/tmp/ch_create_err.log; then
    echo "Schema creation failed. Error:"
    cat /tmp/ch_create_err.log
    exit 1
fi
echo "Schema created successfully."

# Step 3: Retry the insert
echo "[3/3] Retrying INSERT into ${TABLE}..."
if ! clickhouse-client --query "INSERT INTO ${TABLE} FORMAT CSVWithNames" < "${CSV_FILE}" 2>/tmp/ch_insert2_err.log; then
    echo "INSERT failed again. Error:"
    cat /tmp/ch_insert2_err.log
    exit 1
fi

ROW_COUNT=$(clickhouse-client --query "SELECT count() FROM ${TABLE}")
echo "Done. Table ${TABLE} now has ${ROW_COUNT} rows."
