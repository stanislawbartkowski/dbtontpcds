{% macro benchmark_one_query(query_name, timeout_seconds=3600) %}
{#
  Runs `select * from <view>` for a single query_* model and logs its
  wall-clock time as a "BENCHMARK|<name>|<seconds>" line, unlike `dbt
  run`'s own execution_time (which only times the CREATE VIEW statement -
  metadata only, no data scan on most engines).

  Called once per query - from the loop in scripts/run_all_queries.py,
  one dbt run-operation invocation per query - rather than looping over
  every query_* model inside one shared connection/transaction like an
  earlier version of this macro did. That saved 103x dbt process-startup
  overhead, but a cancelled or errored query aborts whatever transaction
  it's in, and Jinja has no try/except to catch that and move on to the
  next query within the same run-operation - one bad query took the whole
  benchmark down with it (see query_1/query_81 below). Isolating each
  query to its own process/connection costs a couple of extra seconds
  each, but means that can't happen anymore.

  On Postgres, sets `statement_timeout` (session GUC, milliseconds) before
  running the query. A handful of TPC-DS queries (query_1, query_81, ...)
  share a CTE-referenced-twice idiom (once directly, once in a correlated
  `avg(...)*1.2` subquery) that Postgres's planner handles badly - the CTE
  is materialized once but then re-scanned per outer row, turning a cheap
  query into a multi-hour (or effectively unbounded) one. statement_timeout
  cancels it server-side instead: the query errors out with Postgres error
  57014 ("canceling statement due to statement timeout"), which
  scripts/run_all_queries.py catches and records as "TIMEOUT" in the Excel
  report rather than letting it hang the rest of the run. Other adapters
  don't get this enforced yet (no equivalent GUC wired up) - they only get
  the process-level timeout scripts/run_all_queries.py applies as a
  backstop, which can't cancel the query server-side, only abandon it.
#}
{% if target.type == 'postgres' %}
  {% do run_query("set statement_timeout = '" ~ (timeout_seconds * 1000) ~ "'") %}
{% endif %}
{% set rel = ref(query_name) %}
{% set start = modules.datetime.datetime.now() %}
{% do run_query('select * from ' ~ rel) %}
{% set elapsed = (modules.datetime.datetime.now() - start).total_seconds() %}
{{ log('BENCHMARK|' ~ query_name ~ '|' ~ elapsed, info=True) }}
{% endmacro %}
