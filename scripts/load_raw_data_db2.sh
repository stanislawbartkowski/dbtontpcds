#!/usr/bin/env bash
# Load TPC-DS pipe-delimited .dat files into the raw schema tables of a
# dbt-db2 database using the Db2 command line processor (db2 CLP). Existing
# rows in each raw table are cleared before loading so the script is safe to
# re-run.
#
# Like Postgres, Db2's IMPORT utility rejects the trailing '|' at the end of
# each row that dsdgen emits (it reads as an extra, unmatched column), so
# each .dat file is copied to a temp file with the trailing delimiter
# stripped before being imported. IMPORT commits every 10000 rows
# (COMMITCOUNT) so large tables (store_sales, catalog_sales, web_sales at
# higher -SCALE factors) don't fill the transaction log in one long
# transaction.
#
# Usage: ./load_raw_data_db2.sh <dat_directory> [db2_database]
#   dat_directory  Directory containing the *.dat files (e.g. DSGen-software-code-4.0.0/dat)
#   db2_database   Db2 database alias to connect to (default: TPC_DATA,
#                  matching the dev_db2 profile target). The connection user
#                  comes from DB2_USER (default: db2inst1) and password from
#                  DB2_PASSWORD (default: secret).

set -euo pipefail

DAT_DIR="${1:?Usage: $0 <dat_directory> [db2_database]}"
DB2_DATABASE="${2:-TPC_DATA}"
DB2_USER="${DB2_USER:-db2inst1}"
DB2_PASSWORD="${DB2_PASSWORD:-secret}"

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

for t in "${TABLES[@]}"; do
  dat_file="${DAT_DIR}/${t}.dat"
  if [[ ! -f "$dat_file" ]]; then
    echo "Error: missing .dat file for table '${t}': $dat_file" >&2
    exit 1
  fi
done

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

db2 CONNECT TO "$DB2_DATABASE" USER "$DB2_USER" USING "$DB2_PASSWORD"

for t in "${TABLES[@]}"; do
  echo "Loading raw.${t}..."
  tmp_file="${TMP_DIR}/${t}.dat"
  sed 's/|$//' "${DAT_DIR}/${t}.dat" > "$tmp_file"
  db2 "TRUNCATE TABLE raw.${t} IMMEDIATE"
  db2 "IMPORT FROM ${tmp_file} OF DEL MODIFIED BY COLDEL| COMMITCOUNT 10000 INSERT INTO raw.${t}"
done

db2 CONNECT RESET

echo "Loaded ${#TABLES[@]} tables into schema 'raw'."
