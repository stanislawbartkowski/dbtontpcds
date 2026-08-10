select *
from {{ source('raw_data', 'time_dim') }}
