#!/usr/bin/env bash
# Load TPC-DS pipe-delimited .dat files into the raw schema tables of a
# dbt-postgres database. Existing rows in each raw table are cleared before
# loading so the script is safe to re-run.
#
# The .dat files end each row with a trailing '|', which DuckDB's CSV reader
# tolerates but Postgres's COPY rejects ("extra data after last expected
# column"), so each line has its trailing delimiter stripped before being
# streamed into COPY FROM STDIN. Empty fields are loaded as NULL (NULL '').
#
# Usage: ./load_raw_data_postgres.sh <dat_directory> [psql_arg ...]
#   dat_directory  Directory containing the *.dat files (e.g. DSGen-software-code-4.0.0/dat)
#   psql_arg ...   Optional connection arguments passed through to psql
#                  (default: -h localhost -U tpc -d tpc_data, matching the
#                  dev_postgres profile target)

set -euo pipefail

DAT_DIR="${1:?Usage: $0 <dat_directory> [psql connection args...]}"
shift
PSQL_ARGS=("$@")
if [[ ${#PSQL_ARGS[@]} -eq 0 ]]; then
  PSQL_ARGS=(-h localhost -U tpc -d tpc_data)
fi
export PGPASSWORD="${PGPASSWORD:-secret}"

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

for t in "${TABLES[@]}"; do
  echo "Loading raw.${t}..."
  psql "${PSQL_ARGS[@]}" -v ON_ERROR_STOP=1 -q -c "TRUNCATE raw.${t};"
  sed 's/|$//' "${DAT_DIR}/${t}.dat" \
    | psql "${PSQL_ARGS[@]}" -v ON_ERROR_STOP=1 -q \
        -c "\\copy raw.${t} FROM STDIN WITH (FORMAT text, DELIMITER '|', NULL '')"
done

echo "Loaded ${#TABLES[@]} tables into schema 'raw'."
