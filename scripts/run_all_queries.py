#!/usr/bin/env python3
"""Run all TPC-DS query models for a given dbt target and report each
query's actual SELECT * execution time as an Excel file in the result
directory.

First runs `dbt run --select queries` to (re)create the query views. Then,
for each query model, runs the benchmark_one_query macro
(macros/benchmark_queries.sql) - one dbt run-operation invocation per
query, each its own connection/transaction - which executes
`select * from <view>` and logs its wall-clock time. This measures real
query execution, unlike dbt run's own execution_time - for a view
materialization (the default here), that only times the CREATE VIEW
statement, which is metadata-only on every engine here and never scans
the underlying data.

Each query gets its own dbt invocation (rather than looping over all of
them inside one shared connection) so that one query timing out or
erroring can't abort the rest of the benchmark - Jinja has no try/except
to recover from that within a single run-operation. On Postgres, the
macro sets `statement_timeout` (--timeout, default 3600s/1h) before running
the query, so a runaway query - a few TPC-DS queries are known to hang
indefinitely on Postgres, see benchmark_queries.sql - gets cancelled
server-side instead of blocking the run forever; this script catches that
and records "TIMEOUT" for the query instead of a time. A --timeout-margin
(default 120s) on top of --timeout backstops non-Postgres targets, which
don't get the statement_timeout enforcement (killing the subprocess can't
cancel the query server-side there, only abandon it).

The RESULT_SIZE environment variable (default: "1", for the SCALE 1
dataset) selects the output subdirectory: result/{RESULT_SIZE}/query_<target>_<timestamp>.xlsx
and is recorded, along with SQL_ENGINE_DESCRIPTION (optional), in two info
rows at the top of the sheet.

If <target> is omitted, it falls back to the DBT_TARGET environment variable.

Usage:
    .venv/bin/python scripts/run_all_queries.py [target] [--select queries] [--profiles-dir .]
        [--result-dir result] [--exclude query_1 ...] [--timeout 3600]
"""
import argparse
import datetime
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BENCHMARK_LINE = re.compile(r"BENCHMARK\|(?P<name>query_\w+)\|(?P<seconds>[\d.]+)")
TIMEOUT_MARKERS = ("statement timeout", "57014")


def dbt_executable() -> str:
    candidate = Path(sys.executable).parent / "dbt"
    return str(candidate) if candidate.exists() else "dbt"


def run_dbt(target: str, select: str, profiles_dir: str) -> None:
    cmd = [
        dbt_executable(), "run",
        "--target", target,
        "--select", select,
        "--profiles-dir", profiles_dir,
        "--threads", "1",
    ]
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=PROJECT_ROOT, check=False)
    if result.returncode != 0:
        sys.exit(f"dbt run failed (exit {result.returncode}) - fix the failing model(s) before benchmarking.")


def list_query_models(target: str, select: str, profiles_dir: str) -> list[str]:
    cmd = [
        dbt_executable(), "--quiet", "ls",
        "--target", target,
        "--select", select,
        "--resource-type", "model",
        "--output", "name",
        "--profiles-dir", profiles_dir,
    ]
    result = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        sys.exit(f"dbt ls failed (exit {result.returncode}):\n{result.stderr}")
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def query_sort_key(name: str):
    m = re.match(r"query_(\d+)([a-z]?)", name)
    return (int(m.group(1)), m.group(2)) if m else (float("inf"), name)


def format_hms(seconds: float) -> str:
    total = int(round(seconds))
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def benchmark_query(target: str, profiles_dir: str, query_name: str, timeout_seconds: int, timeout_margin: int) -> str:
    """Run one query in its own dbt process/connection. Returns the
    Execution Time cell value: an "HH:MM:SS" duration, or "TIMEOUT"."""
    cmd = [
        dbt_executable(), "run-operation", "benchmark_one_query",
        "--target", target,
        "--profiles-dir", profiles_dir,
        "--args", json.dumps({"query_name": query_name, "timeout_seconds": timeout_seconds}),
    ]
    print(f"Starting {query_name}")
    try:
        result = subprocess.run(
            cmd, cwd=PROJECT_ROOT, capture_output=True, text=True,
            timeout=timeout_seconds + timeout_margin,
        )
    except subprocess.TimeoutExpired:
        print(f"{query_name} TIMEOUT - subprocess exceeded {timeout_seconds + timeout_margin}s (statement_timeout backstop)")
        return "TIMEOUT"

    output = result.stdout + result.stderr
    print(output, end="")

    m = BENCHMARK_LINE.search(output)
    if m:
        elapsed = format_hms(float(m.group("seconds")))
        print(f"{query_name} completed - elapsed {elapsed}")
        return elapsed

    if result.returncode != 0 and any(marker in output.lower() for marker in TIMEOUT_MARKERS):
        print(f"{query_name} TIMEOUT - cancelled after {timeout_seconds}s (statement_timeout)")
        return "TIMEOUT"

    print(f"{query_name} ERROR (exit {result.returncode}) - see output above", file=sys.stderr)
    return "ERROR"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("target", nargs="?", help="dbt target to run against (e.g. dev_duckdb, dev_databricks); defaults to DBT_TARGET")
    parser.add_argument("--select", default="queries", help="dbt --select expression (default: queries)")
    parser.add_argument("--profiles-dir", default=".", help="dbt --profiles-dir (default: .)")
    parser.add_argument("--result-dir", default="result", help="output directory for the Excel report (default: result)")
    parser.add_argument("--exclude", nargs="*", default=[], metavar="QUERY",
                         help="query model name(s) to skip entirely, e.g. --exclude query_1 query_81")
    parser.add_argument("--timeout", type=int, default=3600, metavar="SECONDS",
                         help="per-query timeout in seconds (default: 3600 = 1h). Enforced server-side via "
                              "Postgres statement_timeout; other adapters only get the subprocess-level backstop")
    parser.add_argument("--timeout-margin", type=int, default=120, metavar="SECONDS",
                         help="extra seconds on top of --timeout before the subprocess itself is force-killed as "
                              "a backstop, in case statement_timeout doesn't fire (default: 120)")
    args = parser.parse_args()

    target = args.target or os.environ.get("DBT_TARGET")
    if not target:
        sys.exit("No target given and DBT_TARGET is not set - pass a target or export DBT_TARGET.")

    sys.stdout.reconfigure(line_buffering=True)

    run_dbt(target, args.select, args.profiles_dir)
    query_names = [q for q in list_query_models(target, args.select, args.profiles_dir) if q not in args.exclude]
    query_names.sort(key=query_sort_key)

    rows = []
    for query_name in query_names:
        execution_time = benchmark_query(target, args.profiles_dir, query_name, args.timeout, args.timeout_margin)
        rows.append({"Query": query_name, "Target": target, "Execution Time": execution_time})

    df = pd.DataFrame(rows, columns=["Query", "Target", "Execution Time"])

    result_size = os.environ.get("RESULT_SIZE", "1")
    sql_engine_description = os.environ.get("SQL_ENGINE_DESCRIPTION", "")

    result_dir = PROJECT_ROOT / args.result_dir / result_size
    result_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = result_dir / f"query_{target}_{timestamp}.xlsx"

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, startrow=2, sheet_name="Sheet1")
        sheet = writer.sheets["Sheet1"]
        sheet.cell(row=1, column=1, value=f"SQL engine: {sql_engine_description}")
        sheet.cell(row=2, column=1, value=f"Data size: SCALE {result_size}")

    print(f"Wrote {len(df)} query timings to {out_path}")


if __name__ == "__main__":
    main()
