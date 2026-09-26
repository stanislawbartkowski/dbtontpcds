#!/usr/bin/env python3
"""Run all TPC-DS query models for a given dbt target and report each
query's actual SELECT * execution time as an Excel file in the result
directory.

First runs `dbt run --select queries` to (re)create the query views, then
runs the benchmark_queries macro (macros/benchmark_queries.sql), which
executes `select * from <view>` against every one from a single dbt
connection and logs its wall-clock time. This measures real query
execution, unlike dbt run's own execution_time - for a view materialization
(the default here), that only times the CREATE VIEW statement, which is
metadata-only on every engine here and never scans the underlying data.

The RESULT_SIZE environment variable (default: "1", for the SCALE 1
dataset) selects the output subdirectory: result/{RESULT_SIZE}/query_<target>_<timestamp>.xlsx
and is recorded, along with SQL_ENGINE_DESCRIPTION (optional), in two info
rows at the top of the sheet.

If <target> is omitted, it falls back to the DBT_TARGET environment variable.

Usage:
    .venv/bin/python scripts/run_all_queries.py [target] [--select queries] [--profiles-dir .] [--result-dir result]
"""
import argparse
import datetime
import os
import re
import subprocess
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BENCHMARK_LINE = re.compile(r"BENCHMARK\|(?P<name>query_\w+)\|(?P<seconds>[\d.]+)")
STARTING_LINE = re.compile(r"STARTING\|(?P<name>query_\w+)\|(?P<timestamp>\S+)")


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


def print_progress_line(line: str) -> None:
    """Reformat this macro's STARTING/BENCHMARK log lines into readable
    progress output as they stream in; pass everything else through as-is
    so warnings/errors from dbt are still visible live."""
    m = STARTING_LINE.search(line)
    if m:
        print(f"Starting {m.group('name')} at {m.group('timestamp')}")
        return
    m = BENCHMARK_LINE.search(line)
    if m:
        print(f"{m.group('name')} completed - elapsed {format_hms(float(m.group('seconds')))}")
        return
    print(line, end="")


def run_benchmark(target: str, profiles_dir: str) -> str:
    cmd = [
        dbt_executable(), "run-operation", "benchmark_queries",
        "--target", target,
        "--profiles-dir", profiles_dir,
    ]
    print(f"Running: {' '.join(cmd)}")
    process = subprocess.Popen(
        cmd, cwd=PROJECT_ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, bufsize=1,
    )
    lines = []
    for line in process.stdout:
        lines.append(line)
        print_progress_line(line)
    process.wait()
    output = "".join(lines)
    if process.returncode != 0:
        sys.exit(f"benchmark_queries failed (exit {process.returncode}) - a query's SELECT * likely errored; see output above.")
    return output


def query_sort_key(name: str):
    m = re.match(r"query_(\d+)([a-z]?)", name)
    return (int(m.group(1)), m.group(2)) if m else (float("inf"), name)


def format_hms(seconds: float) -> str:
    total = int(round(seconds))
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def load_query_timings(target: str, benchmark_output: str) -> pd.DataFrame:
    rows = [
        {
            "Query": m.group("name"),
            "Target": target,
            "Execution Time": format_hms(float(m.group("seconds"))),
        }
        for m in BENCHMARK_LINE.finditer(benchmark_output)
    ]
    if not rows:
        sys.exit("No BENCHMARK lines found in run-operation output - see above for what dbt actually printed.")
    rows.sort(key=lambda r: query_sort_key(r["Query"]))
    return pd.DataFrame(rows, columns=["Query", "Target", "Execution Time"])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("target", nargs="?", help="dbt target to run against (e.g. dev_duckdb, dev_databricks); defaults to DBT_TARGET")
    parser.add_argument("--select", default="queries", help="dbt --select expression (default: queries)")
    parser.add_argument("--profiles-dir", default=".", help="dbt --profiles-dir (default: .)")
    parser.add_argument("--result-dir", default="result", help="output directory for the Excel report (default: result)")
    args = parser.parse_args()

    target = args.target or os.environ.get("DBT_TARGET")
    if not target:
        sys.exit("No target given and DBT_TARGET is not set - pass a target or export DBT_TARGET.")

    run_dbt(target, args.select, args.profiles_dir)
    benchmark_output = run_benchmark(target, args.profiles_dir)
    df = load_query_timings(target, benchmark_output)

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
