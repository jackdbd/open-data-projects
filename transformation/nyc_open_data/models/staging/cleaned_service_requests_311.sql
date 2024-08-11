select
  unique_key,
  created_date,
  agency
from {{ source('nyc_open_data', 'service_requests_311') }}
