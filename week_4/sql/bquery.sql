-- Green Taxi
CREATE OR REPLACE EXTERNAL TABLE `taxi_trips_bq.external_green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://taxi-tripz-bucket/green_taxi/cf8b04f5eb2e486d9ad86f34e64fe408-0.parquet']
);

CREATE OR REPLACE TABLE `taxi_trips_bq.green_taxi_trips` AS
SELECT * FROM taxi_trips_bq.external_green_tripdata;

-- Yellow Taxi
CREATE OR REPLACE EXTERNAL TABLE `taxi_trips_bq.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://taxi-tripz-bucket/yellow_taxi/ee63864979514930848b4f39a341ff36-0.parquet']
);

CREATE OR REPLACE TABLE `taxi_trips_bq.yellow_taxi_trips` AS
SELECT * FROM taxi_trips_bq.external_yellow_tripdata;

-- FHV Data
CREATE OR REPLACE EXTERNAL TABLE `taxi_trips_bq.external_fhv_data`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://taxi-tripz-bucket/fhv_taxi_data/bab153af17ef4e32a74495f0382cb4c7-0.parquet']
);

CREATE OR REPLACE TABLE `taxi_trips_bq.fhv_taxi_data` AS
SELECT * FROM taxi_trips_bq.external_fhv_data;