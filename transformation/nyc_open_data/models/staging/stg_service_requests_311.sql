SELECT
    unique_key,
    created_date,
    closed_date,
    agency,
    complaint_type,
    descriptor,
    -- location_type,
    -- incident_zip,
    -- incident_address,
    -- street_name,
    -- status,
    -- bbl,
    borough
-- latitude,
-- longitude,
-- vehicle_type
FROM {{ source('nyc_open_data', 'service_requests_311') }}
