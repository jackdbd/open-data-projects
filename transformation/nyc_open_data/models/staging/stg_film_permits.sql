WITH source AS (
    SELECT * FROM {{ source('nyc_open_data', 'film_permits') }}
),

renamed AS (
    SELECT
        borough,
        category,
        country,
        enddatetime AS end_datetime,
        enteredon AS entered_on,
        eventagency AS event_agency,
        eventid AS event_id,
        eventtype AS event_type,
        startdatetime AS start_datetime,
        zipcode_s AS zipcode
    FROM source
)

SELECT * FROM renamed
