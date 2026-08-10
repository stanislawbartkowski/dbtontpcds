select *
from {{ source('raw_data', 'date_dim') }}
