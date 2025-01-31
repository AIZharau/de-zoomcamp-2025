{{ config(materialized='view') }}

WITH tripdata AS 
(
  SELECT *,
    row_number() OVER (PARTITION BY vendorid, tpep_pickup_datetime) AS rn
  FROM {{ source('staging', 'yellow_tripdata') }}
  WHERE vendorid IS NOT NULL
)
SELECT
    -- identifiers
    cityHash64(toString(vendorid), toString(tpep_pickup_datetime)) AS tripid,
    CAST(vendorid AS Int32) AS vendorid,
    CAST(ratecodeid AS Int32) AS ratecodeid,
    CAST(pulocationid AS Int32) AS pickup_locationid,
    CAST(dolocationid AS Int32) AS dropoff_locationid,
    
    -- timestamps
    CAST(tpep_pickup_datetime AS DateTime) AS pickup_datetime,
    CAST(tpep_dropoff_datetime AS DateTime) AS dropoff_datetime,
    
    -- travel info
    store_and_fwd_flag,
    CAST(passenger_count AS Int32) AS passenger_count,
    CAST(trip_distance AS Float64) AS trip_distance,
    -- yellow cabs are always street-hail
    1 AS trip_type,

    -- payment info
    CAST(fare_amount AS Float64) AS fare_amount,
    CAST(extra AS Float64) AS extra,
    CAST(mta_tax AS Float64) AS mta_tax,
    CAST(tip_amount AS Float64) AS tip_amount,
    CAST(tolls_amount AS Float64) AS tolls_amount,
    CAST(ehail_fee AS Float64) AS ehail_fee,
    CAST(improvement_surcharge AS Float64) AS improvement_surcharge,
    CAST(total_amount AS Float64) AS total_amount,
    coalesce(CAST(payment_type AS Int32), 0) AS payment_type,
    {{ get_payment_type_description("payment_type") }} AS payment_type_description
FROM tripdata
WHERE rn = 1

-- limitation for test run
-- dbt build --select <model.sql> --vars '{'is_test_run: false}'
{% if var('is_test_run', default=true) %}
LIMIT 100
{% endif %}