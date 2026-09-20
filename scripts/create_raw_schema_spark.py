#!/usr/bin/env python3
"""Create the TPC-DS raw tables in a remote Spark Connect server from tpcds.sql.

Parses the CREATE TABLE statements out of the TPC-DS tools' tpcds.sql DDL file
and runs them against a Spark Connect server, with a few adjustments:
  - the constraint-only `primary key (...)` clause is dropped, since Spark's
    default managed (parquet) tables don't support declaring it
  - the `time` column type is mapped to `string`, since parquet can't persist
    Spark's TIME type ([UNSUPPORTED_TIME_TYPE])
  - table names are qualified into the `tpc_raw` database

Connection settings come from the same environment variables the
dev_spark dbt profile uses (source .env first): DBT_SPARK_REMOTE
(optional, defaults to sc://localhost:15002).

Usage: ./create_raw_schema_spark.py <tpcds_sql_path>
  tpcds_sql_path  Path to tpcds.sql (e.g.
                  /home/dbt/tpc/DSGen-software-code-4.0.0/tools/tpcds.sql)
"""
import os
import re
import sys

DATABASE = "tpc_raw"

CREATE_TABLE_RE = re.compile(r"create\s+table\s+(\w+)", re.IGNORECASE)
PRIMARY_KEY_RE = re.compile(r",?\s*primary\s+key\s*\([^)]*\)", re.IGNORECASE)
COMMENT_RE = re.compile(r"^\s*--.*$", re.MULTILINE)
TIME_TYPE_RE = re.compile(r"\btime\b", re.IGNORECASE)


def parse_statements(ddl_path: str) -> list[tuple[str, str]]:
    text = COMMENT_RE.sub("", open(ddl_path).read())
    statements = []
    for raw_stmt in text.split(";"):
        stmt = raw_stmt.strip()
        if not stmt:
            continue
        match = CREATE_TABLE_RE.match(stmt)
        if not match:
            raise ValueError(f"unrecognized statement (expected CREATE TABLE): {stmt[:80]!r}")
        table = match.group(1)
        stmt = PRIMARY_KEY_RE.sub("", stmt)
        stmt = TIME_TYPE_RE.sub("string", stmt)
        stmt = CREATE_TABLE_RE.sub(
            f"CREATE TABLE IF NOT EXISTS {DATABASE}.{table}", stmt, count=1
        )
        statements.append((table, stmt))
    return statements


def main() -> None:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <tpcds_sql_path>", file=sys.stderr)
        sys.exit(1)
    ddl_path = sys.argv[1]
    remote_url = os.environ.get("DBT_SPARK_REMOTE", "sc://localhost:15002")

    statements = parse_statements(ddl_path)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.remote(remote_url).getOrCreate()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")

    for table, stmt in statements:
        print(f"Creating {DATABASE}.{table}...")
        spark.sql(stmt)

    print(f"Created {len(statements)} tables in database '{DATABASE}'.")


if __name__ == "__main__":
    main()
