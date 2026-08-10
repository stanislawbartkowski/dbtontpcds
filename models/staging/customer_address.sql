select *
from {{ source('raw_data', 'customer_address') }}
