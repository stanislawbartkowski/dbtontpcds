#!/usr/bin/env bash
# Run a TPC-DS query file against a dbt-duckdb database.
# TPC-DS query templates use a few SQL Server-isms and have a couple of known
# template bugs that DuckDB does not accept as-is, so they are rewritten
# before executing:
#   - "select top n"          -> "select" with a matching "limit n" appended
#                                 before the statement's closing semicolon
#   - "<date-expr> +/- n days" -> "<date-expr> +/- INTERVAL n days"
#   - "c_last_review_date_sk"  -> "c_last_review_date" (query_30 references a
#                                 column that doesn't exist in the TPC-DS DDL;
#                                 a known upstream template bug)
#   - bare aliases that collide with DuckDB keywords ("returns", "at") get an
#     explicit "as" inserted (query_77, query_90)
#
# Usage: ./run_query.sh <query_file.sql> [duckdb_path]
#   query_file.sql  Path to the TPC-DS query file (e.g. queries/query_1.sql)
#   duckdb_path     Path to the target .duckdb file (default: dev.duckdb in the project root)

set -euo pipefail

QUERY_FILE="${1:?Usage: $0 <query_file.sql> [duckdb_path]}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DB_PATH="${2:-${SCRIPT_DIR}/../dev.duckdb}"

if [[ ! -f "$QUERY_FILE" ]]; then
  echo "Error: query file not found: $QUERY_FILE" >&2
  exit 1
fi

SQL_FILE="$(mktemp)"
trap 'rm -f "$SQL_FILE"' EXIT

python3 - "$QUERY_FILE" "$SQL_FILE" <<'PY'
import re
import sys

src, dst = sys.argv[1], sys.argv[2]
text = open(src).read()

def rewrite_top(stmt):
    m = re.search(r'\bselect\s+top\s+(\d+)\b', stmt, re.IGNORECASE)
    if not m:
        return stmt
    n = m.group(1)
    stmt = stmt[:m.start()] + 'select' + stmt[m.end():]
    return stmt.rstrip() + f' limit {n}'

def rewrite_date_arith(stmt):
    return re.sub(r'([+-])\s*(\d+)\s+days\b', r'\1 INTERVAL \2 days', stmt, flags=re.IGNORECASE)

def rewrite_known_column_typo(stmt):
    return re.sub(r'\bc_last_review_date_sk\b', 'c_last_review_date', stmt, flags=re.IGNORECASE)

def rewrite_keyword_alias(stmt):
    return re.sub(r'\)\s+(returns|at)\b(?!\s*\()', r') as "\1"', stmt, flags=re.IGNORECASE)

def rewrite_statement(stmt):
    stmt = rewrite_date_arith(stmt)
    stmt = rewrite_known_column_typo(stmt)
    stmt = rewrite_keyword_alias(stmt)
    stmt = rewrite_top(stmt)
    return stmt

statements = text.split(';')
out = [rewrite_statement(s) for s in statements]
open(dst, 'w').write(';\n'.join(out))
PY

duckdb "$DB_PATH" < "$SQL_FILE"
