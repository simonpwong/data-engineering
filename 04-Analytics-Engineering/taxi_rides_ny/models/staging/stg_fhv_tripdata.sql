{{ config(materialized='view') }}

with tripdata as (
    select * from {{ source('staging', 'fhv_tripdata_2019') }}
    where extract(year from pickup_datetime) = 2019
)
select
    dispatching_base_number,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    sr_flag,
    affiliated_base_number
from tripdata

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
-- {% if var('is_test_run', default=true) %}

--   limit 100

-- {% endif %}