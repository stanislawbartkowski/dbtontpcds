select *
from {{ source('raw_data', 'household_demographics') }}
