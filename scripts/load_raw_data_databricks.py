#!/usr/bin/env python3
"""Load TPC-DS pipe-delimited .dat files into the tpc_raw schema in Databricks.

Mirrors scripts/load_raw_data_spark.py for the Databricks target set up by
scripts/create_raw_schema_databricks.py: each .dat file is uploaded into a
Unity Catalog volume (`<catalog>.tpc_raw.raw_stage`) via the SQL connector's
staging endpoint, then bulk-loaded with COPY INTO. Each table's column names
and types are read back from the catalog (so they always match what the
create script created, including its TIME -> STRING adjustment) and drive
explicit casts in the COPY INTO select list. The trailing `|` dsdgen emits
at the end of every row simply parses as one extra empty column, which the
select list never references, so the files are uploaded as-is.

Each table is TRUNCATEd first and COPY INTO runs with 'force' = 'true'
(otherwise it skips files it has already loaded once), so it's safe to
re-run.

Connection settings come from the same environment variables dbt uses
(source .env first): DBT_DATABRICKS_HOST, DBT_DATABRICKS_HTTP_PATH,
DBT_DATABRICKS_TOKEN, DBT_CATALOG.

Usage: ./load_raw_data_databricks.py <dat_directory>
  dat_directory     Directory containing the *.dat files (e.g. DSGen-software-code-4.0.0/dat)
"""
import os
import sys

SCHEMA = "tpc_raw"
VOLUME = "raw_stage"

TABLES = [
    "call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer",
    "customer_address", "customer_demographics", "date_dim", "dbgen_version",
    "household_demographics", "income_band", "inventory", "item", "promotion", "reason",
    "ship_mode", "store", "store_returns", "store_sales", "time_dim", "warehouse",
    "web_page", "web_returns", "web_sales", "web_site",
]


def table_columns(cursor, table: str) -> list[tuple[str, str]]:
    cursor.execute(f"DESCRIBE TABLE {table}")
    columns = []
    for name, data_type, _comment in cursor.fetchall():
        if not name or name.startswith("#"):
            break
        columns.append((name, data_type))
    return columns


def main() -> None:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <dat_directory>", file=sys.stderr)
        sys.exit(1)
    dat_dir = os.path.abspath(sys.argv[1])

    if not os.path.isdir(dat_dir):
        print(f"Error: dat directory not found: {dat_dir}", file=sys.stderr)
        sys.exit(1)

    dat_paths = {}
    for table in TABLES:
        dat_path = os.path.join(dat_dir, f"{table}.dat")
        if not os.path.isfile(dat_path):
            print(f"Error: missing .dat file for table '{table}': {dat_path}", file=sys.stderr)
            sys.exit(1)
        dat_paths[table] = dat_path

    try:
        host = os.environ["DBT_DATABRICKS_HOST"]
        http_path = os.environ["DBT_DATABRICKS_HTTP_PATH"]
        token = os.environ["DBT_DATABRICKS_TOKEN"]
    except KeyError as missing:
        print(f"Missing environment variable {missing} - source .env first.", file=sys.stderr)
        sys.exit(1)
    catalog = os.environ.get("DBT_CATALOG", "main")

    from databricks import sql

    with sql.connect(
        server_hostname=host,
        http_path=http_path,
        access_token=token,
        staging_allowed_local_path=dat_dir,
    ) as conn, conn.cursor() as cursor:
        cursor.execute(f"USE {catalog}.{SCHEMA}")
        cursor.execute(f"CREATE VOLUME IF NOT EXISTS {VOLUME}")
        volume_dir = f"/Volumes/{catalog}/{SCHEMA}/{VOLUME}"

        for table in TABLES:
            staged = f"{volume_dir}/{table}.dat"
            print(f"Uploading {table}.dat...", flush=True)
            cursor.execute(f"PUT '{dat_paths[table]}' INTO '{staged}' OVERWRITE")

            select_list = ", ".join(
                f"CAST(_c{i} AS {data_type}) AS {name}"
                for i, (name, data_type) in enumerate(table_columns(cursor, table))
            )
            print(f"Loading {catalog}.{SCHEMA}.{table}...", flush=True)
            cursor.execute(f"TRUNCATE TABLE {table}")
            cursor.execute(
                f"""
                COPY INTO {table}
                FROM (SELECT {select_list} FROM '{staged}')
                FILEFORMAT = CSV
                FORMAT_OPTIONS ('sep' = '|', 'header' = 'false')
                COPY_OPTIONS ('force' = 'true')
                """
            )
            rows = cursor.fetchone()
            print(f"  {rows.num_inserted_rows} rows", flush=True)

    print(f"Loaded {len(TABLES)} tables into schema '{catalog}.{SCHEMA}'.")


if __name__ == "__main__":
    main()
