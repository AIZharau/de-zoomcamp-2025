### Running Postgres with Docker

#### Linux and MacOS

```shell
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:13
```

If you see that `ny_taxi_postgres_data` is empty after running the container, try these:

- Deleting the folder and running Docker again (Docker will re-create the folder)
- Adjust the permissions of the folder by running `sudo chmod a+rwx ny_taxi_postgres_data`

### CLI for Postgres

Installing `pgcli`
```shell
pip install pgcli
```

If you have problems installing `pgcli` with the command above, try this:
```shell
conda install -c conda-forge pgcli
pip install -U mycli
```

Using `pgcli` to connect to Postgres
```shell
pgcli -h localhost -p 5432 -u root -d ny_taxi
```

### NY Trips Dataset

Dataset:
- [https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf)


> According to the [TLC data website](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page), from 05/13/2022, the data will be in `.parquet` format instead of `.csv` The website has provided a useful [link](https://www1.nyc.gov/assets/tlc/downloads/pdf/working_parquet_format.pdf) with sample steps to read `.parquet` file and convert it to Pandas data frame.
> 
> You can use the csv backup located here, [https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz), to follow along with the video.

```
$ aws s3 ls s3://nyc-tlc
                           PRE csv_backup/
                           PRE misc/
                           PRE trip data/
```

You can refer the `data-loading-parquet.ipynb` and `data-loading-parquet.py` for code to handle both csv and paraquet files. (The lookup zones table which is needed later in this course is a csv file)
> Note: You will need to install the `pyarrow` library. (add it to your Dockerfile)

### Running Postgres and pgAdmin together

Create a network
```shell
docker network create pg-network
```

Run Postgres
```shell
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network=pg-network \
  --name pg-database \
  postgres:13
```

Run pgAdmin
```shell
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  --network=pg-network \
  --name pgadmin \
  dpage/pgadmin4
```

### Data ingestion
Running locally

```shell
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

python ingest-data.py \
  --user=root \
  --password=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=green_taxi_trips \
  --url=${URL}
```

Build the image
```shell
docker build -t taxi_ingest:v001 .
```

### Docker-Compose

Run it:

```shell
docker-compose up
```

Run in detached mode:
```shell
docker-compose up -d
```

Shutting it down:
```shell
docker-compose down
```

Note: to make pgAdmin configuration persistent, create a folder `data_pgadmin`. Change its permission via
```shell
sudo chown 5050:5050 data_pgadmin
```

and mount it to the `/var/lib/pgadmin` folder:
