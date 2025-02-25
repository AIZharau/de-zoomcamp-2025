import dlt
import datetime
import os
import gzip
import pandas as pd
import pyarrow.parquet as pq
from dlt.sources.helpers import requests
from dlt.common.libs.pyarrow import pyarrow as pa
from dlt.sources.filesystem import FileItemDict, filesystem, readers, read_csv


BATCH_SIZE = 100_000
GROUP_NAME = DESTINATION_SCHEMA_NAME = "ny_taxi"

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
    @dlt.resource(
        name=f"taxi_data_{taxi_type}",
        table_format="iceberg",
        file_format="parquet",
        write_disposition="append",
        columns={"custom_date": {"partition": True}},
    )
    def taxi_data_chunker():
        url = get_taxi_data_url(taxi_type, year, month)

        # Download and process the .gz file in parts
        with requests.get(url, stream=True) as response:
            response.raise_for_status()

            # Unpack the .gz file and read the CSV in parts
            with gzip.GzipFile(fileobj=response.raw) as gz_file:

                # Read CSV in parts using pandas
                for chunk in pd.read_csv(gz_file, chunksize=BATCH_SIZE):

                    chunk = chunk.astype("string")
                    # Transform the chunk into PyArrow Table and add custom_date column
                    table = pa.Table.from_pandas(chunk)
                   
                    date_column = pa.array(
                        [datetime.date(year, month, 1)] * len(table), type=pa.date32()
                    )
                    
                    table = table.append_column(
                        "custom_date", pa.chunked_array([date_column])
                    )

                    yield table

    resource = taxi_data_chunker

    return resource

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    # Creating a Configuration for S3 bucket
    BUCKET_NAME = 'zoomcamp'
    s3_config = {
            "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
            "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
            "endpoint_url": os.getenv("ENDPOINT_URL"),
            "bucket_url": os.getenv("BUCKET_URL"),
            "region": os.getenv("REGION"),
            "use_ssl": False, 
        }

    # Create a pipeline and configure it to write to S3
    pipeline = dlt.pipeline(
        pipeline_name="ny_taxi_pipeline",
        destination="filesystem",
        dataset_name="ny_taxi"
    )

    # Setting filesystem s3
    filesystem_config = {
        "credentials": s3_config,
    }

    # Loading data by types years months
    taxi_types = ["green", "yellow"]
    years = [2019, 2020]

    for taxi_type in taxi_types:
        for year in years:
            for month in range(1, 13):

                s3_path = f"{taxi_type}/{year}/{month}/"
                resource = ny_taxi_source(taxi_type=taxi_type, year=year, month=month)
                resource.staging_path = s3_path
                
                pipeline.run(
                    resource,
                    loader_file_format='parquet', 
                    table_format='iceberg',
                    **filesystem_config
                )

                print(f"Finished processing {taxi_type} data for {year}-{month}.")
        print(f"{taxi_type} loading finished.")
    print("LOADING COMPLETED!")