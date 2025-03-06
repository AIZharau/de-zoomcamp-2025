/* Manua test models */
-- GREEN: stg_green_tripdata.sql -model
CREATE TABLE IF NOT EXISTS staging.stg_green_tripdata
(
    tripid String,
    vendorid Nullable(Int32),
    ratecodeid Nullable(Int32),
    pickup_locationid Nullable(Int32),
    dropoff_locationid Nullable(Int32),
    pickup_datetime DateTime,
    dropoff_datetime DateTime,
    store_and_fwd_flag String,
    passenger_count Nullable(Int32),
    trip_distance Float64,
    trip_type Nullable(Int32),
    fare_amount Float64,
    extra Float64,
    mta_tax Float64,
    tip_amount Float64,
    tolls_amount Float64,
    ehail_fee Float64,
    improvement_surcharge Float64,
    total_amount Float64,
    payment_type Nullable(Int32),
    payment_type_description String,
    custom_date Date
)
ENGINE = ReplacingMergeTree()
ORDER BY tripid;

INSERT INTO staging.stg_green_tripdata
WITH tripdata AS 
(
    SELECT *,
        row_number() OVER (PARTITION BY vendor_id, lpep_pickup_datetime) AS rn
    FROM staging.green_taxi
    WHERE vendor_id IS NOT NULL
)
SELECT
    -- identifiers
    lower(hex(MD5(toString(coalesce(cast(vendor_id as String), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(lpep_pickup_datetime as String), '_dbt_utils_surrogate_key_null_'))))) AS tripid,
    CAST(vendor_id AS Nullable(Int32)) AS vendorid,
    CAST(ratecode_id AS Nullable(Int32)) AS ratecodeid,
    CAST(pu_location_id AS Nullable(Int32)) AS pickup_locationid,
    CAST(do_location_id AS Nullable(Int32)) AS dropoff_locationid,
    
    -- timestamps
    parseDateTimeBestEffort(lpep_pickup_datetime) AS pickup_datetime,
    parseDateTimeBestEffort(lpep_dropoff_datetime) AS dropoff_datetime,
    
    -- travel info
    store_and_fwd_flag,
    CAST(passenger_count AS Nullable(Int32)) AS passenger_count,
    CAST(trip_distance AS Float64) AS trip_distance,
    CAST(trip_type AS Nullable(Int32)) AS trip_type,

    -- payment info
    CAST(fare_amount AS Float64) AS fare_amount,
    CAST(extra AS Float64) AS extra,
    CAST(mta_tax AS Float64) AS mta_tax,
    CAST(tip_amount AS Float64) AS tip_amount,
    CAST(tolls_amount AS Float64) AS tolls_amount,
    CAST(
        CASE 
            WHEN ehail_fee = '' OR ehail_fee IS NULL THEN '0'  -- Обработка пустых строк и NULL
            ELSE ehail_fee
        END AS Float64
    ) AS ehail_fee,
    CAST(
        CASE 
            WHEN improvement_surcharge = '' OR improvement_surcharge IS NULL THEN '0'  -- Обработка пустых строк и NULL
            ELSE improvement_surcharge
        END AS Float64
    ) AS improvement_surcharge,
    CAST(total_amount AS Float64) AS total_amount,
    coalesce(CAST(payment_type AS Nullable(Int32)), 0) AS payment_type,
    CASE 
        WHEN payment_type = 1 THEN 'Credit card'
        WHEN payment_type = 2 THEN 'Cash'
        WHEN payment_type = 3 THEN 'No charge'
        WHEN payment_type = 4 THEN 'Dispute'
        WHEN payment_type = 5 THEN 'Unknown'
        WHEN payment_type = 6 THEN 'Voided trip'
        ELSE 'EMPTY'
    END AS payment_type_description,
    custom_date
FROM tripdata
WHERE rn = 1;


-- YELLOW:  stg_yellow_tripdata.sql -model
CREATE TABLE IF NOT EXISTS staging.stg_yellow_tripdata
(
    tripid String,
    vendorid Nullable(Int32),
    ratecodeid Nullable(Int32),
    pickup_locationid Nullable(Int32),
    dropoff_locationid Nullable(Int32),
    pickup_datetime DateTime,
    dropoff_datetime DateTime,
    store_and_fwd_flag String,
    passenger_count Nullable(Int32),
    trip_distance Float64,
    trip_type Int32,
    fare_amount Float64,
    extra Float64,
    mta_tax Float64,
    tip_amount Float64,
    tolls_amount Float64,
    ehail_fee Float64,
    improvement_surcharge Float64,
    total_amount Float64,
    payment_type Nullable(Int32),
    payment_type_description String,
    custom_date Date
)
ENGINE = ReplacingMergeTree()
ORDER BY tripid;

SET max_memory_usage = 10000000000; -- 10 GB

INSERT INTO staging.stg_yellow_tripdata
SETTINGS 
  async_insert=1, 
  wait_for_async_insert=1,
  max_insert_block_size=10000,
  min_insert_block_size_rows=5000,
  min_insert_block_size_bytes=10000000,
  max_threads=4,
  max_read_buffer_size=10000000
WITH tripdata AS 
(
    SELECT *,
        row_number() OVER (PARTITION BY vendor_id, tpep_pickup_datetime) AS rn
    FROM staging.yellow_taxi
    WHERE vendor_id IS NOT NULL
    	AND custom_date IN ['2020-10-01', '2020-11-01', '2020-12-01'] -- changing partitions manually for loading big dataset
)
SELECT
    -- identifiers
    hex(MD5(
    toString(
        coalesce(cast(vendor_id as String), '_dbt_utils_surrogate_key_null_') || '-' || 
        coalesce(cast(tpep_pickup_datetime as String), '_dbt_utils_surrogate_key_null_')
	    )
	)) AS tripid,
 	CAST(vendor_id AS Nullable(Int32)) AS vendorid,
    CAST(ratecode_id AS Nullable(Int32)) AS ratecodeid,
    CAST(pu_location_id AS Nullable(Int32)) AS pickup_locationid,
    CAST(do_location_id AS Nullable(Int32)) AS dropoff_locationid,
    
    -- timestamps
    parseDateTimeBestEffort(tpep_pickup_datetime) AS pickup_datetime,
    parseDateTimeBestEffort(tpep_dropoff_datetime) AS dropoff_datetime,
    
    -- travel info
    store_and_fwd_flag,
    CAST(passenger_count AS Nullable(Int32)) AS passenger_count,
    CAST(trip_distance AS Float64) AS trip_distance,
    -- yellow cabs are always street-hail
    1 AS trip_type,

    -- payment info
    CAST(fare_amount AS Float64) AS fare_amount,
    CAST(extra AS Float64) AS extra,
    CAST(mta_tax AS Float64) AS mta_tax,
    CAST(tip_amount AS Float64) AS tip_amount,
    CAST(tolls_amount AS Float64) AS tolls_amount,
    CAST(0 AS Float64) AS ehail_fee,
    CAST(improvement_surcharge AS Float64) AS improvement_surcharge,
    CAST(total_amount AS Float64) AS total_amount,
    coalesce(CAST(payment_type AS Nullable(Int32)), 0) AS payment_type,
	CASE
	    WHEN payment_type = 1 THEN 'Credit Card'
	    WHEN payment_type = 2 THEN 'Cash'
	    WHEN payment_type = 3 THEN 'No Charge'
	    WHEN payment_type = 4 THEN 'Dispute'
	    WHEN payment_type = 5 THEN 'Unknown'
	    ELSE 'Other'
	END AS payment_type_description,
    custom_date
FROM tripdata
WHERE rn = 1;

-- FHV:  stg_fhv_trips.sql -model
CREATE TABLE IF NOT EXISTS staging.stg_fhv_tripdata
(
	tripid String,
    dispatching_base_num String,
    pickup_datetime DateTime,
    dropoff_datetime DateTime,
    pickup_locationid Int32,
    dropoff_locationid Int32,
    sr_flag String,
    affiliated_base_number String,
    custom_date Date
)
ENGINE = ReplacingMergeTree()
ORDER BY tripid;

INSERT INTO staging.stg_fhv_tripdata
WITH tripdata AS 
(
    SELECT 
        *
    FROM staging.fhv_taxi
    WHERE dispatching_base_num IS NOT NULL
)
SELECT
    -- identifiers
    lower(hex(MD5(toString(coalesce(cast(dispatching_base_num as String), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(pickup_datetime as String), '_dbt_utils_surrogate_key_null_'))))) AS tripid,
    CAST(dispatching_base_num AS String) AS dispatching_base_num,
    CAST(p_ulocation_id AS Int32) AS pickup_locationid,
    CAST(d_olocation_id AS Int32) AS dropoff_locationid,
    
    -- timestamps
    parseDateTimeBestEffort(pickup_datetime) AS pickup_datetime,
    parseDateTimeBestEffort(drop_off_datetime) AS dropoff_datetime,
    
    -- travel info
    sr_flag,
    CAST(affiliated_base_number AS String) AS affiliated_base_number,
    custom_date
FROM tripdata;
