{% test expect_row_count(model, row_count) %}

select count(*) as actual_row_count
from {{ model }}
having count(*) != {{ row_count }}

{% endtest %}
