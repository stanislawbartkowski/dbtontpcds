select *
from {{ source('raw_data', 'income_band') }}
