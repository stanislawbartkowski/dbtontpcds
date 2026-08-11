#!/usr/bin/env python3
"""Load TPC-DS pipe-delimited .dat files into the tpc_raw database of a Spark Connect server.

Mirrors scripts/load_raw_data.sh for the Spark target set up by
scripts/create_raw_schema_spark.py: each table's schema is read from Spark's
catalog (so column names, types, and order always match what that script
created, including its TIME -> STRING adjustment) and the table's contents
are overwritten from the matching pipe-delimited .dat file, so it's safe to
re-run.

Usage: ./load_raw_data_spark.py <dat_directory> [spark_remote_url]
  dat_directory     Directory containing the *.dat files (e.g. DSGen-software-code-4.0.0/dat)
  spark_remote_url  Spark Connect remote URL (default: sc://localhost:15002)
"""
import os
import sys

DATABASE = "tpc_raw"

TABLES = [
    "call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer",
    "customer_address", "customer_demographics", "date_dim", "dbgen_version",
    "household_demographics", "income_band", "inventory", "item", "promotion", "reason",
    "ship_mode", "store", "store_returns", "store_sales", "time_dim", "warehouse",
    "web_page", "web_returns", "web_sales", "web_site",
]


def main() -> None:
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <dat_directory> [spark_remote_url]", file=sys.stderr)
        sys.exit(1)
    dat_dir = sys.argv[1]
    remote_url = sys.argv[2] if len(sys.argv) > 2 else "sc://localhost:15002"

    if not os.path.isdir(dat_dir):
        print(f"Error: dat directory not found: {dat_dir}", file=sys.stderr)
        sys.exit(1)

    dat_paths = {}
    for table in TABLES:
        dat_path = os.path.abspath(os.path.join(dat_dir, f"{table}.dat"))
        if not os.path.isfile(dat_path):
            print(f"Error: missing .dat file for table '{table}': {dat_path}", file=sys.stderr)
            sys.exit(1)
        dat_paths[table] = dat_path

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.remote(remote_url).getOrCreate()

    for table in TABLES:
        schema = spark.table(f"{DATABASE}.{table}").schema
        print(f"Loading {DATABASE}.{table}...")
        df = (
            spark.read.option("delimiter", "|")
            .option("header", "false")
            .schema(schema)
            .csv(dat_paths[table])
        )
        df.write.insertInto(f"{DATABASE}.{table}", overwrite=True)

    print(f"Loaded {len(TABLES)} tables into database '{DATABASE}'.")


if __name__ == "__main__":
    main()
