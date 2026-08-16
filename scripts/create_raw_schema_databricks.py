#!/usr/bin/env python3
"""Create the TPC-DS raw tables in a Databricks workspace from tpcds.sql.

Parses the CREATE TABLE statements out of the TPC-DS tools' tpcds.sql DDL file
and runs them against a Databricks SQL warehouse, with a few adjustments:
  - the constraint-only `primary key (...)` clause is dropped, since Unity
    Catalog primary keys are informational-only and require extra syntax
  - the `time` column type is mapped to `string`, since Databricks has no
    TIME type
  - table names are qualified into the `<catalog>.tpc_raw` schema

Connection settings come from the same environment variables dbt uses
(source .env first):
  DBT_DATABRICKS_HOST       workspace hostname, without https://
  DBT_DATABRICKS_HTTP_PATH  SQL warehouse HTTP path
  DBT_DATABRICKS_TOKEN      personal access token
  DBT_CATALOG               Unity Catalog name (default: main)

Usage: ./create_raw_schema_databricks.py <tpcds_sql_path>
  tpcds_sql_path    Path to tpcds.sql (e.g.
                     /home/dbt/tpc/DSGen-software-code-4.0.0/tools/tpcds.sql)
"""
import os
import re
import sys

SCHEMA = "tpc_raw"

CREATE_TABLE_RE = re.compile(r"create\s+table\s+(\w+)", re.IGNORECASE)
PRIMARY_KEY_RE = re.compile(r",?\s*primary\s+key\s*\([^)]*\)", re.IGNORECASE)
COMMENT_RE = re.compile(r"^\s*--.*$", re.MULTILINE)
TIME_TYPE_RE = re.compile(r"\btime\b", re.IGNORECASE)


def parse_statements(ddl_path: str, catalog: str) -> list[tuple[str, str]]:
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
            f"CREATE TABLE IF NOT EXISTS {catalog}.{SCHEMA}.{table}", stmt, count=1
        )
        statements.append((table, stmt))
    return statements


def main() -> None:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <tpcds_sql_path>", file=sys.stderr)
        sys.exit(1)
    ddl_path = sys.argv[1]

    try:
        host = os.environ["DBT_DATABRICKS_HOST"]
        http_path = os.environ["DBT_DATABRICKS_HTTP_PATH"]
        token = os.environ["DBT_DATABRICKS_TOKEN"]
    except KeyError as missing:
        print(f"Missing environment variable {missing} - source .env first.", file=sys.stderr)
        sys.exit(1)
    catalog = os.environ.get("DBT_CATALOG", "main")

    statements = parse_statements(ddl_path, catalog)

    from databricks import sql

    with sql.connect(
        server_hostname=host, http_path=http_path, access_token=token
    ) as conn, conn.cursor() as cursor:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{SCHEMA}")
        for table, stmt in statements:
            print(f"Creating {catalog}.{SCHEMA}.{table}...")
            cursor.execute(stmt)

    print(f"Created {len(statements)} tables in schema '{catalog}.{SCHEMA}'.")


if __name__ == "__main__":
    main()
