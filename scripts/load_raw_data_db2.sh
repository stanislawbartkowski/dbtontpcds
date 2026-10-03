#!/usr/bin/env bash
# Load TPC-DS pipe-delimited .dat files into the raw schema tables of a
# dbt-db2 database using the Db2 LOAD utility via the command line processor
# (db2 CLP), then collect statistics. Each table is loaded with REPLACE, so
# the script is safe to re-run.
#
# LOAD runs on the Db2 server and reads the .dat files there, so the
# directory must be readable by the Db2 instance owner (db2inst1). It is far
# faster than IMPORT, which inserts row by row (about 17k rows/s here, i.e.
# hours at SCALE 10 and more than a day at SCALE 100). LOAD also tolerates
# the trailing '|' dsdgen ends each row with, so no stripped copy is needed.
# NONRECOVERABLE keeps the table spaces out of backup-pending state if the
# database has forward recovery enabled.
#
# LOAD doesn't update statistics, and without them the Db2 optimizer plans
# as if the tables were tiny, so each table gets a RUNSTATS afterwards (the
# equivalent of Postgres's auto-analyze).
#
# Usage: ./load_raw_data_db2.sh <dat_directory> [db2_database]
#   dat_directory  Directory containing the *.dat files (e.g. DSGen-software-code-4.0.0/dat)
#   db2_database   Db2 database alias to connect to (default: $DBT_DB2_DATABASE,
#                  or TPC_DATA). The user and password come from DBT_DB2_USER and
#                  DBT_DB2_PASSWORD (required) - the same env vars the dev_db2
#                  profile target reads; source .env first.

set -euo pipefail

DAT_DIR="${1:?Usage: $0 <dat_directory> [db2_database]}"
DB2_DATABASE="${2:-${DBT_DB2_DATABASE:-TPC_DATA}}"
DB2_USER="${DBT_DB2_USER:?DBT_DB2_USER is not set - source .env first}"
DB2_PASSWORD="${DBT_DB2_PASSWORD:?DBT_DB2_PASSWORD is not set - source .env first}"

if [[ ! -d "$DAT_DIR" ]]; then
  echo "Error: dat directory not found: $DAT_DIR" >&2
  exit 1
fi
# The server resolves the path, so it must be absolute.
DAT_DIR="$(cd "$DAT_DIR" && pwd)"

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

db2 CONNECT TO "$DB2_DATABASE" USER "$DB2_USER" USING "$DB2_PASSWORD"

for t in "${TABLES[@]}"; do
  echo "Loading raw.${t}... ($(date +%T))"
  # CLP exits 2 on warnings, which every LOAD prints; only >= 4 is a failure.
  rc=0
  out="$(db2 "LOAD FROM ${DAT_DIR}/${t}.dat OF DEL MODIFIED BY COLDEL| REPLACE INTO raw.${t} NONRECOVERABLE")" || rc=$?
  echo "$out" | grep -E "Number of rows (read|loaded|rejected)"
  rejected="$(echo "$out" | sed -n 's/.*Number of rows rejected *= *//p')"
  if (( rc >= 4 )) || [[ "${rejected:-1}" != "0" ]]; then
    echo "$out" >&2
    echo "Error: LOAD of raw.${t} failed (exit $rc, rows rejected: ${rejected:-unknown})" >&2
    exit 1
  fi
done

for t in "${TABLES[@]}"; do
  echo "Collecting statistics on raw.${t}... ($(date +%T))"
  db2 "RUNSTATS ON TABLE raw.${t} WITH DISTRIBUTION AND SAMPLED DETAILED INDEXES ALL" > /dev/null
done

db2 CONNECT RESET

echo "Loaded ${#TABLES[@]} tables into schema 'raw'."
