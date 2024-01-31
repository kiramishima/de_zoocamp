import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_data(*args, **kwargs):
    list_datasets = []
    for month in range(10, 13):
        url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-{month}.csv.gz"
        print(url)
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
        df = pd.read_csv(url, sep=',', compression='gzip', dtype=taxi_dtypes, parse_dates=parse_dates)
        list_datasets.append(df)

    # print(list_datasets)
    return pd.concat(list_datasets, ignore_index=True)
