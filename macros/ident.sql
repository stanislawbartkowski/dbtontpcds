{% macro ident(name) %}
{%- if target.type == 'spark' -%}
`{{ name }}`
{%- else -%}
"{{ name }}"
{%- endif -%}
{% endmacro %}
