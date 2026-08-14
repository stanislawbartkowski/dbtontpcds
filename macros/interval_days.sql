{% macro interval_days(n) %}
{%- if target.type == 'postgres' -%}
INTERVAL '{{ n }} days'
{%- else -%}
INTERVAL {{ n }} days
{%- endif -%}
{% endmacro %}
