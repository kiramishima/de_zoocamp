import os
import dlt
import argparse
import pandas as pd
from dlt.destinations import postgres
from tqdm import tqdm
import pyarrow as pa
import pyarrow.parquet as pq

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'personal.json'

def load_yellow_data_from_source(st_year, ed_year):
    taxi_dtypes = {
        'VendorID': pd.Int64Dtype(),
        'passenger_count': pd.Int64Dtype(),
        'trip_distance': float,
        'RatecodeID':pd.Int64Dtype(),
        'store_and_fwd_flag':str,
        'PULocationID':pd.Int64Dtype(),
        'DOLocationID':pd.Int64Dtype(),
        'payment_type': pd.Int64Dtype(),
        'fare_amount': float,
        'extra':float,
        'mta_tax':float,
        'tip_amount':float,
        'tolls_amount':float,
        'improvement_surcharge':float,
        'total_amount':float,
        'congestion_surcharge':float
    }

    parse_dates = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
    data = []
    for year in tqdm(range(st_year, ed_year+1)):
        for month in range(1, 13):
            URL = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_{year}-{month:02d}.csv.gz"
    
            print(f"{year} - {month: 03d}: {URL}")
            df = pd.read_csv(URL, sep=',', compression='gzip', dtype=taxi_dtypes, parse_dates=parse_dates)
            data.append(df)

    return pd.concat(data, ignore_index=True)

def load_green_data_from_source(st_year, ed_year):
    taxi_dtypes = {
        'VendorID': pd.Int64Dtype(),
        'passenger_count': pd.Int64Dtype(),
        'trip_distance': float,
        'RatecodeID':pd.Int64Dtype(),
        'store_and_fwd_flag':str,
        'PULocationID':pd.Int64Dtype(),
        'DOLocationID':pd.Int64Dtype(),
        'payment_type': pd.Int64Dtype(),
        'fare_amount': float,
        'extra':float,
        'mta_tax':float,
        'tip_amount':float,
        'tolls_amount':float,
        'improvement_surcharge':float,
        'total_amount':float,
        'congestion_surcharge':float
    }

    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
    data = []
    for year in tqdm(range(st_year, ed_year+1)):
        for month in range(1, 13):
            URL = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_{year}-{month:02d}.csv.gz"
            print(f"{year} - {month:03d}: {URL}")
            df = pd.read_csv(URL, sep=',', compression='gzip', dtype=taxi_dtypes, parse_dates=parse_dates)
            data.append(df)

    return pd.concat(data, ignore_index=True)

def load_fhv_taxi_data_from_source(year):
    taxi_dtypes = {
        'PUlocationID':pd.Int64Dtype(),
        'DOlocationID':pd.Int64Dtype(),
        'SR_Flag': float
    }

    parse_dates = ['pickup_datetime', 'dropOff_datetime']
    data = []
    for month in tqdm(range(1, 13)):
        URL = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{month:02d}.csv.gz"
        print(f"2019 - {month:03d}")
        df = pd.read_csv(URL, sep=',', compression='gzip', dtype=taxi_dtypes, parse_dates=parse_dates)
        data.append(df)

    return pd.concat(data, ignore_index=True)

def export_data_to_gcs(df, **kwargs):
    project_id = kwargs['project_id']
    bucket_name = kwargs['bucket_name']
    table_name = kwargs['table_name']
    root_path = f'{bucket_name}/{table_name}'

    table = pa.Table.from_pandas(df)
    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        filesystem=gcs
    )

def main(params):
    uri = f"postgresql://{params.user}:{params.password}@{params.host}:{params.port}/{params.db}"
    # print(uri)
    # Create the pipeline
    pipeline = dlt.pipeline(
        pipeline_name="taxi_data",
        destination=postgres(credentials=uri),
    )
    # Loads Yellow
    yellow_data = load_yellow_data_from_source(2019, 2020)
    print(yellow_data.info())
    # Save in localfile in cas
    export_data_to_gcs(yellow_data, project_id=params.project_id, bucket_name='taxi-tripz-bucket', table_name='yellow_taxi')
    # load_info = pipeline.run(yellow_data, table_name="yellow_taxi_data", write_disposition="replace")
    # print(load_info)
    # Loads Green
    green_data = load_green_data_from_source(2019, 2020)
    print(green_data.info())
    export_data_to_gcs(green_data, project_id=params.project_id, bucket_name='taxi-tripz-bucket', table_name='green_taxi')
    # load_info2 = pipeline.run(green_data, table_name="green_taxi_data", write_disposition="replace")
    # print(load_info2)
    # Loads FHV
    fhv_data = load_fhv_taxi_data_from_source(2019)
    print(fhv_data.info())
    export_data_to_gcs(fhv_data, project_id=params.project_id, bucket_name='taxi-tripz-bucket', table_name='fhv_taxi_data')
    #load_info3 = pipeline.run(fhv_data, table_name="fhv_taxi_data", write_disposition="replace")
    #print(load_info3)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--project_id', required=True, help='GC Project ID')

    args = parser.parse_args()

    main(args)
