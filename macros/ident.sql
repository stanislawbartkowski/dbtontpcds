{% macro ident(name) %}
{%- if target.type in ('spark', 'databricks') -%}
`{{ name }}`
{%- else -%}
"{{ name }}"
{%- endif -%}
{% endmacro %}
