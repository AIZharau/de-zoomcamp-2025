# flake8: noqa
import os
from typing import Iterator
import pandas as pd
from dlt.sources.helpers import requests
from dlt.common.libs.pyarrow import pyarrow as pa
import gzip
import dlt
from dlt.sources import TDataItems
from dlt.sources.filesystem import FileItemDict, filesystem, readers, read_csv
import io
import datetime

BUCKET_NAME = "zoomcamp"


def get_taxi_data_url(taxi_type, year, month):
    """Generates URL for downloading data."""
    base_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
    return f"{base_url}/{taxi_type}/{taxi_type}_tripdata_{year}-{month:02d}.csv.gz"


@dlt.source(name="ny_taxi")
def ny_taxi_source(
    taxi_type: str,
    year: str,
    month: str,
):
    @dlt.resource(name=f"taxi_data_{taxi_type}", file_format="csv")
    def taxi_data_chunker():
        """Downloads data, processes it and loads it into an Iceberg table."""
        url = get_taxi_data_url(taxi_type, year, month)

        # Download and unzip the file
        response = requests.get(url, stream=True)
        response.raise_for_status()

        # Unpacking .gz file in memory
        with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz_file:
            # df = pd.read_csv(gz_file)
            dtype = {
                "VendorID": "float64",
                "passenger_count": "float64",
                "trip_distance": "float64",
                "RatecodeID": "float64",
                "store_and_fwd_flag": "object",
                "PULocationID": "float64",
                "DOLocationID": "float64",
                "payment_type": "float64",
                "fare_amount": "float64",
                "extra": "float64",
                "mta_tax": "float64",
                "tip_amount": "float64",
                "tolls_amount": "float64",
                "improvement_surcharge": "float64",
                "total_amount": "float64",
                "congestion_surcharge": "float64",
            }
            df = pd.read_csv(gz_file, dtype=dtype, low_memory=False)

        # Adding a date column
        df["custom_date"] = datetime.date(year, month, 1)

        yield df

    return taxi_data_chunker


if __name__ == "__main__":
    # Loading environment inclusion from .env file
    from dotenv import load_dotenv

    load_dotenv()

    # Creating a Configuration for MinIO
    s3_config = {
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "endpoint_url": os.getenv("ENDPOINT_URL"),
        "bucket_url": os.getenv("BUCKET_URL"),
        "region": os.getenv("REGION"),
        "use_ssl": False,
    }

    # Creating a pipeline with MinIO configuration
    pipeline = dlt.pipeline(
        pipeline_name="ny_taxi_pipeline",
        destination="filesystem",  # Using filesystem for S3
        dataset_name="nyc_taxi",
    )

    # Setting filesystem s3 MinIO
    filesystem_config = {
        "credentials": s3_config,
    }

    # Loading data
    data = ny_taxi_source(taxi_type="yellow", year=2019, month=1)
    load_info = pipeline.run(
        data,
        table_name="yellow_taxi_data",
        loader_file_format="csv",
        **filesystem_config,
    )
    print(load_info)
