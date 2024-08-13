SELECT
  eventid,
  eventtype,
  startdatetime,
  enddatetime,
  enteredon,
  eventagency,
  borough,
  category,
  zipcode_s
FROM {{ source('nyc_open_data', 'film_permits') }}