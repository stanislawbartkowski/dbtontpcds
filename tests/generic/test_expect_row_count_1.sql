{% test expect_row_count_1(model, row_count_1) %}

select count(*) as actual_row_count
from {{ model }}
having count(*) != {{ row_count_1 }}

{% endtest %}
