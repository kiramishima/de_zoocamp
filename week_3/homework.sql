-- External Table
CREATE OR REPLACE EXTERNAL TABLE `bq_green_taxi.external_green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://green-taxi-bucket/green_tripdata_2022-*.parquet']
);

-- Table
CREATE OR REPLACE TABLE `bq_green_taxi.green_taxi_trips` AS
SELECT * FROM bq_green_taxi.external_green_tripdata;

-- Question 1
-- What is count of records for the 2022 Green Taxi Data??
SELECT COUNT(*) FROM `bq_green_taxi.external_green_tripdata`;
-- 840402

-- Question 2
-- Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
-- What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

-- COUNT in EXTERNAL TABLE
SELECT COUNT(DISTINCT PULocationID) 
FROM bq_green_taxi.external_green_tripdata;
-- COUNT in TABLE
SELECT COUNT(DISTINCT PULocationID) 
FROM bq_green_taxi.green_taxi_trips;

-- Answer: 0 MB for the External Table and 6.41MB for the Materialized Table


-- Question 3
-- How many records have a fare_amount of 0?

--Query
SELECT COUNT(*) 
FROM bq_green_taxi.green_taxi_trips
WHERE fare_amount = 0;

-- Answer: 1622

-- Question 4
-- What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)

-- Query: New Table partitioned and clustered
CREATE OR REPLACE TABLE bq_green_taxi.green_trips_partitoned
PARTITION BY
    DATE(lpep_pickup_datetime) 
CLUSTER BY PUlocationID AS
SELECT * FROM bq_green_taxi.external_green_tripdata;

-- Answer: Partition by lpep_pickup_datetime Cluster on PUlocationID

-- Question 5
-- Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)
-- Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?

-- Queries
-- No partitioned table
SELECT COUNT(DISTINCT PULocationID) 
FROM bq_green_taxi.green_taxi_trips
WHERE CAST(lpep_pickup_datetime AS DATE) BETWEEN '2022-06-01' AND '2022-06-30';

-- partitioned table
SELECT COUNT(DISTINCT PULocationID) 
FROM bq_green_taxi.green_trips_partitoned
WHERE CAST(lpep_pickup_datetime AS DATE) BETWEEN '2022-06-01' AND '2022-06-30';

-- Answer: 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table

-- Question 6
-- Where is the data stored in the External Table you created?

-- Answer: GCP Bucket

-- Question 7
-- It is best practice in Big Query to always cluster your data:

-- Answer: False.
-- It depends, we can also partitioned the data instead of clustering or we can use both.

-- Question 8
-- No Points: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

-- Query
SELECT COUNT(*) FROM bq_green_taxi.green_trips_partitoned;

-- Answer: 0B read, because the data was partitioned and clustered, also, its more fast to filter or aggregate thanks to data stored in .
