select *
from {{ source('raw_data', 'ship_mode') }}
