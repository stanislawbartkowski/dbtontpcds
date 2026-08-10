select *
from {{ source('raw_data', 'promotion') }}
