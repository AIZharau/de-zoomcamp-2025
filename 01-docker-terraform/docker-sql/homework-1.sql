-- Prepare Postgres
-- Run Postgres and load data as shown in the videos
-- We'll use the green taxi trips from October 2019.

-- Question 3. Trip Segmentation Count
/*
During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive),
how many trips, respectively, happened:
1. Up to 1 mile
2. In between 1 (exclusive) and 3 miles (inclusive),
3. In between 3 (exclusive) and 7 miles (inclusive),
4. In between 7 (exclusive) and 10 miles (inclusive),
5. Over 10 miles
*/
-- 1: To get an answer to points 2, 3, 4, 5 you just need to change limits in the WHERE condition.
select count(1)
from green_taxi_data
/* 2: where trip_distance > 1 and trip_distance <= 3, 3: trip_distance > 3 and trip_distance <= 7 etc. */
where trip_distance <= 1
    and lpep_dropoff_datetime::date
    between '2019-10-01' and '2019-10-31';
-- answer: 104802 / 198924 / 109603 / 27678 / 35189
----------------------------------------------------------------------------------------------------------------

-- Question 4. Longest trip for each day
/*
Which was the pick up day with the longest trip distance? Use the pick up time for your calculations.
Tip: For every day, we only care about one single trip with the longest distance.
*/
select lpep_pickup_datetime::date
from green_taxi_data
order by trip_distance desc
limit 1;
-- answer: 2019-10-31
----------------------------------------------------------------------------------------------------------------

-- Question 5. Three biggest pickup zones
/*
Which were the top pickup locations with over 13,000 in total_amount (across all trips) for 2019-10-18?
Consider only lpep_pickup_datetime when filtering by date.
*/
with fltr as (
    select "PULocationID"
    from green_taxi_data
    where lpep_pickup_datetime::date = '2019-10-18'
    group by "PULocationID"
    having sum(total_amount) > 13000
)
select "Zone"
from taxi_zone_lookup
where "LocationID" in (select * from fltr);
-- answer: East Harlem North, East Harlem South, Morningside Heights
----------------------------------------------------------------------------------------------------------------

-- Question 6. Largest tip
/*
For the passengers picked up in Ocrober 2019 in the zone name "East Harlem North"
which was the drop off zone that had the largest tip?
Note: it's tip , not trip
We need the name of the zone, not the ID.
*/
with puz as (
    select "LocationID"
    from taxi_zone_lookup
    where "Zone" = 'East Harlem North'
),
tips as (
    select "DOLocationID"
    from green_taxi_data
    where "PULocationID" in (select * from puz)
    order by tip_amount desc
    limit 1
)
select tz."Zone"
from taxi_zone_lookup tz
join tips t on t."DOLocationID" = tz."LocationID";
-- answer: JFK Airport