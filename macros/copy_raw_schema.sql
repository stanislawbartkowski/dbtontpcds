{% macro copy_raw_schema() %}
{#
  Copies every table in <catalog>.tpc_raw into <catalog>.tpc_raw_<RESULT_SIZE>
  via CREATE TABLE ... AS SELECT, so the currently loaded scale's raw data is
  preserved as a snapshot before the next load overwrites tpc_raw with a
  different scale.

  Databricks/Unity Catalog only (tpc_raw is the spark/databricks raw schema
  name; other targets use a plain "raw" schema with no catalog).

  Usage (RESULT_SIZE must be set, e.g. via `source .env`):
    dbt run-operation copy_raw_schema --target dev_databricks --profiles-dir .
#}
{% if target.type != 'databricks' %}
  {{ exceptions.raise_compiler_error("copy_raw_schema only supports the databricks target, got: " ~ target.type) }}
{% endif %}

{% set result_size = env_var('RESULT_SIZE') %}
{% set catalog = target.database %}
{% set src_schema = 'tpc_raw' %}
{% set dst_schema = 'tpc_raw_' ~ result_size %}

{% set tables = [
  'call_center', 'catalog_page', 'catalog_returns', 'catalog_sales', 'customer',
  'customer_address', 'customer_demographics', 'date_dim', 'dbgen_version',
  'household_demographics', 'income_band', 'inventory', 'item', 'promotion', 'reason',
  'ship_mode', 'store', 'store_returns', 'store_sales', 'time_dim', 'warehouse',
  'web_page', 'web_returns', 'web_sales', 'web_site'
] %}

{% do run_query('CREATE SCHEMA IF NOT EXISTS ' ~ catalog ~ '.' ~ dst_schema) %}

{% for table in tables %}
  {% set start = modules.datetime.datetime.now() %}
  {% do run_query(
    'CREATE OR REPLACE TABLE ' ~ catalog ~ '.' ~ dst_schema ~ '.' ~ table
    ~ ' AS SELECT * FROM ' ~ catalog ~ '.' ~ src_schema ~ '.' ~ table
  ) %}
  {% set elapsed = (modules.datetime.datetime.now() - start).total_seconds() %}
  {{ log('COPIED|' ~ table ~ '|' ~ elapsed, info=True) }}
{% endfor %}

{{ log('Copied ' ~ tables | length ~ ' tables from ' ~ catalog ~ '.' ~ src_schema ~ ' to ' ~ catalog ~ '.' ~ dst_schema, info=True) }}
{% endmacro %}
