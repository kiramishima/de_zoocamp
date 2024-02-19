{{ config(materialized='view') }}

SELECT
    CAST(dispatching_base_num AS STRING) AS dispatching_base_num,
    -- timestamps
    TIMESTAMP_MICROS(CAST(pickup_datetime / 1000 AS INT64)) AS pickup_datetime,
    TIMESTAMP_MICROS(CAST(dropOff_datetime / 1000 AS INT64)) AS dropoff_datetime,

    -- location
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} AS pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} AS dropoff_locationid,
    COALESCE(CAST(sr_flag as numeric), 0) as sr_flag,
    CAST(affiliated_base_number AS STRING) AS affiliated_base_number
FROM {{ source("staging", 'fhv_taxi_data') }}

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  LIMIT 100

{% endif %}