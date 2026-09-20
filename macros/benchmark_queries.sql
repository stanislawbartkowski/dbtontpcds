{% macro benchmark_queries() %}
{#
  Runs `select * from <view>` against every query_* model and logs its
  wall-clock time. Unlike `dbt run`'s own execution_time (which only times
  the CREATE VIEW statement - metadata only, no data scan on most engines),
  this measures the actual cost of reading the view's data. All queries run
  from this one macro invocation, sharing a single connection, so 103
  queries don't pay 103x dbt process-startup overhead.

  Output is parsed by scripts/run_all_queries.py, which greps stdout for
  the "BENCHMARK|<name>|<seconds>" lines this macro logs.
#}
{% set query_nodes = [] %}
{% for node in graph.nodes.values() %}
  {% if node.resource_type == 'model' and node.name.startswith('query_') %}
    {% do query_nodes.append(node) %}
  {% endif %}
{% endfor %}

{% for node in query_nodes %}
  {% set rel = ref(node.name) %}
  {% set start = modules.datetime.datetime.now() %}
  {% do run_query('select * from ' ~ rel) %}
  {% set elapsed = (modules.datetime.datetime.now() - start).total_seconds() %}
  {{ log('BENCHMARK|' ~ node.name ~ '|' ~ elapsed, info=True) }}
{% endfor %}
{% endmacro %}
