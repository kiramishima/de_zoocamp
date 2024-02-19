{{ config(materialized='table') }}

WITH dim_zones AS (
    SELECT * FROM {{ ref('taxi_zone_lookup') }}
    WHERE borough != 'Unknown'
),
fhv_data AS (
    SELECT *,
    "FHV" AS service_type
    FROM {{ ref('stg_fhv_tripdata') }}
    WHERE dispatching_base_num IS NOT NULL 
        AND pickup_locationid IS NOT NULL
        AND dropoff_locationid IS NOT NULL
        AND affiliated_base_number IS NOT NULL
)
SELECT 
    fhv_data.trip_id,
    fhv_data.service_type,
    fhv_data.dispatching_base_num,
    fhv_data.pickup_datetime,
    fhv_data.dropoff_datetime,
    fhv_data.pickup_locationid,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    fhv_data.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    fhv_data.sr_flag,
    fhv_data.affiliated_base_number
FROM fhv_data
INNER JOIN dim_zones AS pickup_zone
    ON fhv_data.pickup_locationid = pickup_zone.locationid
INNER JOIN dim_zones AS dropoff_zone
    ON fhv_data.dropoff_locationid = dropoff_zone.locationid

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  LIMIT 100

{% endif %}