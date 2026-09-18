#!/usr/bin/env python3
"""Run all TPC-DS query models for a given dbt target and report per-query
execution time as an Excel file in the result directory.

Usage:
    .venv/bin/python scripts/run_all_queries.py <target> [--select queries] [--profiles-dir .] [--result-dir result]
"""
import argparse
import datetime
import json
import re
import subprocess
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def dbt_executable() -> str:
    candidate = Path(sys.executable).parent / "dbt"
    return str(candidate) if candidate.exists() else "dbt"


def run_dbt(target: str, select: str, profiles_dir: str) -> None:
    cmd = [
        dbt_executable(), "run",
        "--target", target,
        "--select", select,
        "--profiles-dir", profiles_dir,
    ]
    print(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, cwd=PROJECT_ROOT, check=False)


def query_sort_key(name: str):
    m = re.match(r"query_(\d+)([a-z]?)", name)
    return (int(m.group(1)), m.group(2)) if m else (float("inf"), name)


def format_hms(seconds: float) -> str:
    total = int(round(seconds))
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def load_query_timings(target: str, run_results_path: Path) -> pd.DataFrame:
    results = json.loads(run_results_path.read_text())["results"]
    rows = []
    for result in results:
        name = result["unique_id"].split(".")[-1]
        if not name.startswith("query_"):
            continue
        rows.append({
            "Query": name,
            "Target": target,
            "Execution Time": format_hms(result["execution_time"]),
        })
    rows.sort(key=lambda r: query_sort_key(r["Query"]))
    return pd.DataFrame(rows, columns=["Query", "Target", "Execution Time"])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("target", help="dbt target to run against (e.g. dev_duckdb, dev_databricks)")
    parser.add_argument("--select", default="queries", help="dbt --select expression (default: queries)")
    parser.add_argument("--profiles-dir", default=".", help="dbt --profiles-dir (default: .)")
    parser.add_argument("--result-dir", default="result", help="output directory for the Excel report (default: result)")
    args = parser.parse_args()

    run_dbt(args.target, args.select, args.profiles_dir)

    run_results_path = PROJECT_ROOT / "target" / "run_results.json"
    if not run_results_path.exists():
        sys.exit(f"No run_results.json found at {run_results_path} - dbt run may have failed to start.")

    df = load_query_timings(args.target, run_results_path)

    result_dir = PROJECT_ROOT / args.result_dir
    result_dir.mkdir(exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = result_dir / f"query_execution_{args.target}_{timestamp}.xlsx"
    df.to_excel(out_path, index=False, engine="openpyxl")

    print(f"Wrote {len(df)} query timings to {out_path}")


if __name__ == "__main__":
    main()
