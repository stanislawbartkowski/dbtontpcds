#!/usr/bin/env python3
"""Run all TPC-DS query models for a given dbt target and report each
query's actual SELECT * execution time as an Excel file in the result
directory.

First runs `dbt run --select staging queries` to (re)create the staging and
query views. Then,
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
macro sets `statement_timeout` (--timeout, default 60 minutes) before running
the query, so a runaway query - a few TPC-DS queries are known to hang
indefinitely on Postgres, see benchmark_queries.sql - gets cancelled
server-side instead of blocking the run forever; this script catches that
and records "TIMEOUT" for the query instead of a time. Every query also
runs under the `timeout` command at --timeout plus --timeout-margin
(default 120s), which is what enforces the limit on other targets. Killing
the dbt client doesn't stop its query on a Spark Connect server, so on
Spark targets the script then kills the orphaned jobs through the Spark UI
(--spark-ui-url); on other non-Postgres targets the query is abandoned.

The RESULT_SIZE environment variable (default: "1", for the SCALE 1
dataset) selects the output subdirectory: result/{RESULT_SIZE}/query_<target>_<timestamp>.xlsx
and is recorded, along with SQL_ENGINE_DESCRIPTION (optional), in two info
rows at the top of the sheet.

If <target> is omitted, it falls back to the DBT_TARGET environment variable.

Usage:
    .venv/bin/python scripts/run_all_queries.py [target] [--select queries] [--profiles-dir .]
        [--result-dir result] [--exclude query_1 ...] [--timeout 60|01:00:00]
"""
import argparse
import datetime
import json
import os
import re
import subprocess
import sys
import time
import urllib.request
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
        # staging too, not just the query views: they select from the staging
        # views, which don't exist yet on a freshly loaded target.
        "--select", "staging", select,
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


def parse_timeout(value: str) -> int:
    """Accepts either a plain number of minutes (e.g. "60") or an
    HH:MM:SS duration (e.g. "01:30:00"); returns seconds."""
    m = re.match(r"^(\d+):(\d{2}):(\d{2})$", value)
    if m:
        h, mm, s = (int(g) for g in m.groups())
        return h * 3600 + mm * 60 + s
    try:
        return int(value) * 60
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"invalid timeout {value!r} - expected a number of minutes (e.g. 60) or HH:MM:SS (e.g. 01:30:00)"
        )


def kill_running_spark_jobs(spark_ui_url: str) -> None:
    """Kill every running job on the Spark Connect server via its UI.

    Killing the dbt client doesn't stop its query on a Spark Connect server
    (dbt-spark's session cancel() is a no-op), so a timed-out query would keep
    competing with the next one. The benchmark runs one query at a time, so
    anything still running at this point is the orphan."""
    try:
        with urllib.request.urlopen(f"{spark_ui_url}/api/v1/applications", timeout=10) as r:
            apps = json.load(r)
        for app in apps:
            with urllib.request.urlopen(f"{spark_ui_url}/api/v1/applications/{app['id']}/jobs?status=running", timeout=10) as r:
                jobs = json.load(r)
            for job in jobs:
                req = urllib.request.Request(f"{spark_ui_url}/jobs/job/kill/?id={job['jobId']}", method="POST")
                urllib.request.urlopen(req, timeout=10).close()
                print(f"  killed orphaned Spark job {job['jobId']}")
    except OSError as exc:
        print(f"  could not kill orphaned Spark jobs via {spark_ui_url}: {exc}", file=sys.stderr)


def benchmark_query(target: str, profiles_dir: str, query_name: str, timeout_seconds: int, timeout_margin: int,
                    spark_ui_url: str) -> str:
    """Run one query in its own dbt process/connection. Returns the
    Execution Time cell value: an "HH:MM:SS" duration, "TIMEOUT", or "ERROR"."""
    limit = timeout_seconds + timeout_margin
    cmd = [
        # SIGINT first so dbt shuts down as on Ctrl-C; SIGKILL 30s later if it hasn't.
        "timeout", "-s", "INT", "-k", "30", str(limit),
        dbt_executable(), "run-operation", "benchmark_one_query",
        "--target", target,
        "--profiles-dir", profiles_dir,
        "--args", json.dumps({"query_name": query_name, "timeout_seconds": timeout_seconds}),
    ]
    print(f"Starting {query_name}")
    start = time.monotonic()
    result = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True)
    elapsed_wall = time.monotonic() - start

    output = result.stdout + result.stderr
    print(output, end="")

    if result.returncode in (124, 137) and elapsed_wall >= limit:
        print(f"{query_name} TIMEOUT - killed by timeout after {limit}s")
        if "Registered adapter: spark" in output:
            kill_running_spark_jobs(spark_ui_url)
        return "TIMEOUT"

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
    parser.add_argument("--timeout", type=parse_timeout, default=os.environ.get("QUERY_TIMEOUT", "01:00:00"),
                         metavar="MINUTES|HH:MM:SS",
                         help="per-query timeout: a number of minutes (e.g. 60) or an HH:MM:SS duration "
                              "(e.g. 01:30:00); defaults to the QUERY_TIMEOUT env var (HH:MM:SS), or 01:00:00 "
                              "if that's unset. Enforced server-side via Postgres statement_timeout; other "
                              "adapters only get the subprocess-level backstop")
    parser.add_argument("--timeout-margin", type=int, default=120, metavar="SECONDS",
                         help="extra seconds on top of --timeout before the `timeout` command stops the dbt "
                              "process, so Postgres's own statement_timeout gets to fire first (default: 120)")
    parser.add_argument("--spark-ui-url", default=os.environ.get("SPARK_UI_URL", "http://localhost:4040"),
                         help="Spark UI used to kill a timed-out query's orphaned jobs on Spark targets "
                              "(default: $SPARK_UI_URL or http://localhost:4040)")
    parser.add_argument("--resume", metavar="PATH",
                         help="path to a previous run's .xlsx (e.g. from an interrupted run) - already-recorded "
                              "queries are skipped and new results are appended to the same file instead of "
                              "starting a fresh one")
    args = parser.parse_args()

    target = args.target or os.environ.get("DBT_TARGET")
    if not target:
        sys.exit("No target given and DBT_TARGET is not set - pass a target or export DBT_TARGET.")

    sys.stdout.reconfigure(line_buffering=True)

    result_size = os.environ.get("RESULT_SIZE", "1")
    sql_engine_description = os.environ.get("SQL_ENGINE_DESCRIPTION", "")

    if args.resume:
        out_path = Path(args.resume)
        rows = pd.read_excel(out_path, skiprows=3).to_dict("records")
        print(f"Resuming {out_path} - {len(rows)} queries already recorded")
    else:
        result_dir = PROJECT_ROOT / args.result_dir / result_size
        result_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = result_dir / f"query_{target}_{timestamp}.xlsx"
        rows = []

    already_done = {r["Query"] for r in rows}

    run_dbt(target, args.select, args.profiles_dir)
    query_names = [q for q in list_query_models(target, args.select, args.profiles_dir)
                   if q not in args.exclude and q not in already_done]
    query_names.sort(key=query_sort_key)

    for query_name in query_names:
        execution_time = benchmark_query(target, args.profiles_dir, query_name, args.timeout, args.timeout_margin,
                                         args.spark_ui_url)
        rows.append({"Query": query_name, "Target": target, "Execution Time": execution_time})
        # Written after every query (not just at the end) so a killed/crashed run
        # - a real risk over a many-hour, 100+-query run - still leaves a report
        # with whatever completed so far instead of losing everything.
        write_excel(rows, out_path, result_size, sql_engine_description, args.timeout)

    print(f"Wrote {len(rows)} query timings to {out_path}")


def write_excel(rows: list[dict], out_path: Path, result_size: str, sql_engine_description: str, timeout_seconds: int) -> None:
    df = pd.DataFrame(rows, columns=["Query", "Target", "Execution Time"])
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, startrow=3, sheet_name="Sheet1")
        sheet = writer.sheets["Sheet1"]
        sheet.cell(row=1, column=1, value=f"SQL engine: {sql_engine_description}")
        sheet.cell(row=2, column=1, value=f"Data size: SCALE {result_size}")
        sheet.cell(row=3, column=1, value=f"Per-query timeout: {format_hms(timeout_seconds)}")


if __name__ == "__main__":
    main()
