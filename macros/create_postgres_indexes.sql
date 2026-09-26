{% macro create_postgres_indexes() %}
{#
  Adds the secondary indexes the TPC-DS query models (models/queries/*.sql)
  actually need on the raw schema tables, beyond what tpcds.sql's own
  `primary key` constraints already give for free.

  tpcds.sql only indexes each table's own primary key - one surrogate key
  per dimension table (e.g. i_item_sk), or a composite (item_sk, order/ticket
  number) per fact table. That leaves every *other* foreign key on a fact
  table unindexed, so joins like `ss_sold_date_sk = d_date_sk` or
  `cs_bill_customer_sk = c_customer_sk` force a sequential scan of the fact
  table (hundreds of millions of rows at scale 100) for every query. Grepping
  models/queries/*.sql for `<col>_sk = <col>_sk` join patterns shows these
  fact-table foreign keys dominate: ss_sold_date_sk/ss_store_sk/ss_item_sk
  alone account for 90+ joins across the 99 queries, with the *_customer_sk,
  *_addr_sk, *_cdemo_sk, *_hdemo_sk, *_promo_sk, *_reason_sk, *_call_center_sk,
  *_catalog_page_sk, *_web_page_sk, *_web_site_sk, *_ship_mode_sk, and
  *_warehouse_sk columns covering the rest. Indexing those turns those joins
  into index/bitmap scans instead.

  Also indexes a handful of dimension columns that queries filter on
  directly rather than joining (i_category, ca_state, ca_zip,
  cd_marital_status/cd_education_status) - skipped for tables tiny enough
  (store, date_dim) that a sequential scan already beats an index lookup.

  Postgres only - other targets (Databricks, Db2, DuckDB) either manage
  indexes/clustering differently or aren't the target this was written for.

  Not run automatically as part of `dbt run`: the raw tables are created and
  loaded outside of dbt (see README's "Postgres" section), so this is a
  one-off run-operation against them, safe to re-run (IF NOT EXISTS). Expect
  it to take a while at scale 100 - these are plain `CREATE INDEX`, not
  `CONCURRENTLY` (which can't run inside dbt's transaction), so each one
  holds a lock on its table until built.

  Usage:
    dbt run-operation create_postgres_indexes --target dev_postgres --profiles-dir .
#}
{% if target.type != 'postgres' %}
  {{ exceptions.raise_compiler_error("create_postgres_indexes only supports the postgres target, got: " ~ target.type) }}
{% endif %}

{% set indexes = [
  ('store_sales', ['ss_sold_date_sk', 'ss_store_sk', 'ss_customer_sk', 'ss_cdemo_sk', 'ss_hdemo_sk', 'ss_addr_sk', 'ss_promo_sk']),
  ('store_returns', ['sr_returned_date_sk', 'sr_customer_sk', 'sr_store_sk', 'sr_reason_sk', 'sr_cdemo_sk', 'sr_hdemo_sk', 'sr_addr_sk']),
  ('catalog_sales', ['cs_sold_date_sk', 'cs_ship_date_sk', 'cs_bill_customer_sk', 'cs_bill_cdemo_sk', 'cs_bill_hdemo_sk', 'cs_bill_addr_sk', 'cs_ship_customer_sk', 'cs_ship_addr_sk', 'cs_call_center_sk', 'cs_catalog_page_sk', 'cs_ship_mode_sk', 'cs_warehouse_sk', 'cs_promo_sk']),
  ('catalog_returns', ['cr_returned_date_sk', 'cr_returning_customer_sk', 'cr_refunded_customer_sk', 'cr_call_center_sk', 'cr_catalog_page_sk', 'cr_reason_sk']),
  ('web_sales', ['ws_sold_date_sk', 'ws_ship_date_sk', 'ws_bill_customer_sk', 'ws_bill_cdemo_sk', 'ws_bill_addr_sk', 'ws_ship_customer_sk', 'ws_ship_addr_sk', 'ws_web_page_sk', 'ws_web_site_sk', 'ws_ship_mode_sk', 'ws_warehouse_sk', 'ws_promo_sk']),
  ('web_returns', ['wr_returned_date_sk', 'wr_returning_customer_sk', 'wr_refunded_customer_sk', 'wr_web_page_sk', 'wr_reason_sk']),
  ('inventory', ['inv_item_sk', 'inv_warehouse_sk']),
  ('item', ['i_category']),
  ('customer_address', ['ca_state', 'ca_zip']),
  ('customer_demographics', ['cd_marital_status', 'cd_education_status']),
] %}

{% for table, columns in indexes %}
  {% for column in columns %}
    {% set index_name = 'ix_' ~ table ~ '_' ~ column %}
    {% set start = modules.datetime.datetime.now() %}
    {% do run_query(
      'CREATE INDEX IF NOT EXISTS ' ~ index_name ~ ' ON raw.' ~ table ~ ' (' ~ column ~ ')'
    ) %}
    {% set elapsed = (modules.datetime.datetime.now() - start).total_seconds() %}
    {{ log('INDEXED|' ~ table ~ '.' ~ column ~ '|' ~ elapsed, info=True) }}
  {% endfor %}
{% endfor %}

{{ log('Created indexes on ' ~ indexes | length ~ ' raw tables.', info=True) }}
{% endmacro %}
