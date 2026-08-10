select *
from {{ source('raw_data', 'dbgen_version') }}
