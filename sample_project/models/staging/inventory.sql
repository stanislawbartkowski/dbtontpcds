select *
from {{ source('raw_data', 'inventory') }}
