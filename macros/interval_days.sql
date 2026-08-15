{% macro interval_days(n) %}
{%- if target.type == 'postgres' -%}
INTERVAL '{{ n }} days'
{%- elif target.type == 'ibmdb2' -%}
{{ n }} days
{%- else -%}
INTERVAL {{ n }} days
{%- endif -%}
{% endmacro %}
