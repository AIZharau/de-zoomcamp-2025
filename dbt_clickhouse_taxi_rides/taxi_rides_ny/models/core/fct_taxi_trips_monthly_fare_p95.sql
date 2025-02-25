{{
    config(
        schema='core',
        order_by='tripid',
        engine='ReplacingMergeTree',
        materialized='incremental'
    )
}}


WITH fact_trips_clean AS (
    SELECT 
        service_type,
        toYear(pickup_datetime) AS year,
        toMonth(pickup_datetime) AS month,
        fare_amount,
        trip_distance,
        payment_type_description
    FROM core.fact_trips
    WHERE 
        fare_amount > 0
        AND trip_distance > 0
        AND lower(payment_type_description) in ('cash', 'credit card')
),

fare_amount_percentile AS (
    SELECT
        service_type,
        year,
        month,
        quantile(0.97)(fare_amount) OVER (PARTITION BY service_type, year, month) AS p97,
        quantile(0.95)(fare_amount) OVER (PARTITION BY service_type, year, month) AS p95,
        quantile(0.90)(fare_amount) OVER (PARTITION BY service_type, year, month) AS p90
    FROM fact_trips_clean
)

SELECT * FROM fare_amount_percentile
WHERE year = 2020 and month = 4