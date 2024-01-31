from pandas import DataFrame
import pyarrow as pa
import pyarrow.parquet as pq
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

# Change personal-gcp.json with your own credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/src/personal-gcp.json'

@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
    project_id = kwargs['project_id']
    bucket_name = kwargs['bucket_name']
    table_name = kwargs['table_name']
    root_path = f'{bucket_name}/{table_name}'

    table = pa.Table.from_pandas(df)

    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['lpep_pickup_date'],
        filesystem=gcs
    )
