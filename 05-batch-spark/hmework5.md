# Module 5 Homework

In this homework we'll put what we learned about Spark in practice.

For this homework we will be using the Yellow 2024-10 data from the official website: 

```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-10.parquet
```


## Question 1: Install Spark and PySpark

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?

> [!NOTE]
> To install PySpark follow this [guide](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/05-batch/setup/pyspark.md)


```python
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

print(spark.version)

```
### Answer: 3.5.5

## Question 2: Yellow October 2024

Read the October 2024 Yellow into a Spark Dataframe.

Repartition the Dataframe to 4 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

```python
df = spark.read.parquet("yellow_tripdata_2024-10.parquet")
df = df.repartition(4)
df.write.parquet('yellow/2024/10')
```
### Answer: 22.4 -> 25MB

## Question 3: Count records 

How many taxi trips were there on the 15th of October?
Consider only trips that started on the 15th of October.

```python
from pyspark.sql.functions import col, to_date

df.filter(
    to_date(col("tpep_pickup_datetime")) == "2024-10-15"
    ).count()
```
### Answer: 125,567

## Question 4: Longest trip

What is the length of the longest trip in the dataset in hours?

```python
SQL_QUERY = """
    select MAX(timestampdiff(
            HOUR, 
            tpep_pickup_datetime, 
            tpep_dropoff_datetime
        )) from trips_data
"""

df.registerTempTable('trips_data')
spark.sql(SQL_QUERY).show()

```
### Answer: 162

## Question 5: User Interface

Spark’s User Interface which shows the application's dashboard runs on which local port?

### Answer: 4040


## Question 6: Least frequent pickup location zone

Load the zone lookup data into a temp view in Spark:

```bash
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Using the zone lookup data and the Yellow October 2024 data, what is the name of the LEAST frequent pickup location Zone?

```python
zone = spark.read \
    .option("header", "true") \
    .csv('taxi_zone_lookup.csv')

zone = zone.withColumn("LocationID", col("LocationID").cast("int"))
zone.registerTempTable('lu_zones')

SQL_QUERY_JOIN = """
    select z.Zone,
           count(*) as trip_count,
    from tirps_data td join lu_zones z
        on td.PULocationID = z.LocatopnID
    group by 1
    order by 2
    limit 1
"""

spark.sql(SQL_QUERY_JOIN).show(truncate=False)

```

### Answer: Governor's Island/Ellis Island/Liberty Island
