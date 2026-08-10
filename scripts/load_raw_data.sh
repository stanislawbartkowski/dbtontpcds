#!/usr/bin/env bash
# Load TPC-DS pipe-delimited .dat files into the raw schema tables of a dbt-duckdb database.
# Existing rows in each raw table are cleared before loading so the script is safe to re-run.
#
# Usage: ./load_raw_data.sh <dat_directory> [duckdb_path]
#   dat_directory  Directory containing the *.dat files (e.g. DSGen-software-code-4.0.0/dat)
#   duckdb_path    Path to the target .duckdb file (default: dev.duckdb in the project root)

set -euo pipefail

DAT_DIR="${1:?Usage: $0 <dat_directory> [duckdb_path]}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DB_PATH="${2:-${SCRIPT_DIR}/../dev.duckdb}"

if [[ ! -d "$DAT_DIR" ]]; then
  echo "Error: dat directory not found: $DAT_DIR" >&2
  exit 1
fi

TABLES=(
  call_center catalog_page catalog_returns catalog_sales customer
  customer_address customer_demographics date_dim dbgen_version
  household_demographics income_band inventory item promotion reason
  ship_mode store store_returns store_sales time_dim warehouse
  web_page web_returns web_sales web_site
)

SQL_FILE="$(mktemp)"
trap 'rm -f "$SQL_FILE"' EXIT

for t in "${TABLES[@]}"; do
  dat_file="${DAT_DIR}/${t}.dat"
  if [[ ! -f "$dat_file" ]]; then
    echo "Error: missing .dat file for table '${t}': $dat_file" >&2
    exit 1
  fi
  echo "TRUNCATE raw.${t};" >> "$SQL_FILE"
done

for t in "${TABLES[@]}"; do
  echo "COPY raw.${t} FROM '${DAT_DIR}/${t}.dat' (DELIMITER '|', HEADER false);" >> "$SQL_FILE"
done

duckdb "$DB_PATH" < "$SQL_FILE"
