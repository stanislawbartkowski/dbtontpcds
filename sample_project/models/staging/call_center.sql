select *
from {{ source('raw_data', 'call_center') }}
