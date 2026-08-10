select *
from {{ source('raw_data', 'catalog_sales') }}
