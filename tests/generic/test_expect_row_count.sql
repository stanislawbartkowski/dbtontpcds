{% test expect_row_count(model, row_count_1, row_count_10, row_count_100) %}

{% set result_size = env_var('RESULT_SIZE', '1') %}
{% set expected = row_count_100 if result_size == '100' else (row_count_10 if result_size == '10' else row_count_1) %}

select count(*) as actual_row_count
from {{ model }}
having count(*) != {{ expected }}

{% endtest %}
