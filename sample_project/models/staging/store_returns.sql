select *
from {{ source('raw_data', 'store_returns') }}
