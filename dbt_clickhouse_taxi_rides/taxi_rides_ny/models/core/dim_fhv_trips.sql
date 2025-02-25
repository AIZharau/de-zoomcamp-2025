{{
    config(
        schema='core',
        order_by='tripid',
        engine='ReplacingMergeTree',
        materialized='incremental'
    )
}}


WITH transformed_fhv_tripdata AS (
    SELECT 
        *,
        toYear(pickup_datetime) AS year,
        toMonth(pickup_datetime) AS month
    FROM {{ ref('fhv_tripdata') }}
    WHERE dispatching_base_num IS NOT NULL
),
dim_zones AS (
    SELECT * FROM {{ ref('dim_zones') }}
    WHERE borough != 'Unknown'
)
SELECT 
    dispatching_base_num String,
    pickup_datetime String,
    dropoff_datetime String,
    pu_location_id String,
    do_location_id String,
    sr_flag String,
    affiliated_base_number String,
    custom_date Date DEFAULT toDate(now())
FROM transformed_fhv_tripdata
    INNER JOIN dim_zones
    AS pickup_zone
    ON transformed_fhv_tripdata.pickup_locationid = pickup_zone.locationid
    INNER JOIN dim_zones as dropoff_zone
    ON transformed_fhv_tripdata.dropoff_locationid = dropoff_zone.locationid
    