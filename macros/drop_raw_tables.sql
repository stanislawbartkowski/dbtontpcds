{% macro drop_raw_tables() %}
{#
  Drops all 25 TPC-DS raw tables for whichever target dbt is pointed at
  (--target, or the DBT_TARGET env var if profiles.yml's default target
  reads it), to reclaim disk/storage space - e.g. before reloading a
  different scale, or just to free up space (the raw schema at scale 100
  runs 100+ GB on Postgres once create_postgres_indexes has run).

  One macro instead of a drop macro per adapter, since the only real
  difference between targets is how the raw table is qualified - this
  mirrors how models/staging/sources.yml resolves the same raw tables:
    - postgres, ibmdb2, duckdb: schema "raw"        -> raw.<table>
    - spark:                    database "tpc_raw"  -> tpc_raw.<table>
    - databricks:                <catalog>.tpc_raw   -> <catalog>.tpc_raw.<table>
                                 (catalog = target.database, i.e. DBT_CATALOG)

  Postgres additionally gets `CASCADE` on the DROP - the other adapters
  either don't support it on DROP TABLE (Spark/Databricks reserve CASCADE
  for DROP SCHEMA; Db2 doesn't accept it at all) or don't need it.

  Destructive - the schema is left empty afterward. Recreate the 25 tables
  from tpcds.sql (see README's "Create the tables in the raw schema", or
  the target-specific create_raw_schema_*.py scripts for Spark/Databricks)
  before loading data again.

  Usage:
    dbt run-operation drop_raw_tables --target dev_postgres --profiles-dir .
  or, relying on DBT_TARGET instead of --target:
    dbt run-operation drop_raw_tables --target "$DBT_TARGET" --profiles-dir .
#}
{% set tables = [
  'call_center', 'catalog_page', 'catalog_returns', 'catalog_sales', 'customer',
  'customer_address', 'customer_demographics', 'date_dim', 'dbgen_version',
  'household_demographics', 'income_band', 'inventory', 'item', 'promotion', 'reason',
  'ship_mode', 'store', 'store_returns', 'store_sales', 'time_dim', 'warehouse',
  'web_page', 'web_returns', 'web_sales', 'web_site'
] %}

{% if target.type in ('spark', 'databricks') %}
  {% set schema = 'tpc_raw' %}
  {% set database = target.database if target.type == 'databricks' else none %}
{% else %}
  {% set schema = 'raw' %}
  {% set database = 'dev' if target.type == 'duckdb' else none %}
{% endif %}

{% for table in tables %}
  {% set relation = api.Relation.create(database=database, schema=schema, identifier=table) %}
  {% set drop_sql = 'DROP TABLE IF EXISTS ' ~ relation ~ (' CASCADE' if target.type == 'postgres' else '') %}
  {% do run_query(drop_sql) %}
  {{ log('DROPPED|' ~ table, info=True) }}
{% endfor %}

{{ log('Dropped ' ~ tables | length ~ ' tables from the raw schema on target ' ~ target.name ~ '.', info=True) }}
{% endmacro %}
