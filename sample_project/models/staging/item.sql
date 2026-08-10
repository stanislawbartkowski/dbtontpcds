select *
from {{ source('raw_data', 'item') }}
