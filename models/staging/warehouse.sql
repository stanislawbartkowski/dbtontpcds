select *
from {{ source('raw_data', 'warehouse') }}
