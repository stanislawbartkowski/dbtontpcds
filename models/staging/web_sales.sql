select *
from {{ source('raw_data', 'web_sales') }}
