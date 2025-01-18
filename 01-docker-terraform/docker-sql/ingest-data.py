import argparse
import os
import pandas as pd
from sqlalchemy import create_engine
from time import time


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    # for gzipped files
    # for pandas to be able to open the file
    if url.endwith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    count = 0
    full_size = 0
    while True:

        try:
            t_start = time()
            count += 1
            df = next(df_iter)
            full_size += len(df)

            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')
            t_end = time()
            print(f'{count}. insertanother chunk..., took %.3f second' % (t_end - t_start))

        except StopIteration:
            print(f'Insertion completed. {full_size} lines inserted into the postgres database!')
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres.')

    parser.add_argument(--'user', help='user name for postgres')
    parser.add_argument(--'password', help='password for postgres')
    parser.add_argument(--'host', help='host for postgres')
    parser.add_argument(--'port', help='port for postgres')
    parser.add_argument(--'db', help='database name for postgres')
    parser.add_argument(--'table_name', help='name of the table where we will write the results to')
    parser.add_argument(--'url', help='url of the csv file')

    args = parser.parse_args()

    main(args)