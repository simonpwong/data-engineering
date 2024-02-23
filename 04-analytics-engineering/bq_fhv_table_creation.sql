-- FHV 2019 Data
CREATE OR REPLACE EXTERNAL TABLE `peak-academy-343418.trips_data_all.fhv_2019_external`
 OPTIONS (
    format='parquet',
    uris=['gs://peak-academy-343418-terra-bucket/fhv/*']
);

CREATE OR REPLACE TABLE `peak-academy-343418.trips_data_all.fhv_tripdata_2019` AS
SELECT * FROM `peak-academy-343418.trips_data_all.fhv_2019_external`;

-- Combine FHV Trip Data with Yellow & Green Trip Data into 1 Table for Dashboard
CREATE OR REPLACE TABLE `peak-academy-343418.dbt_swong.fact_all_trips` AS
SELECT 
  pickup_locationid,
  pickup_datetime,
  pickup_borough,
  pickup_zone,
  dropoff_locationid,
  dropoff_datetime,
  dropoff_borough,
  dropoff_zone,
  service_type
FROM `peak-academy-343418.dbt_swong.fact_trips`
WHERE EXTRACT(year from pickup_datetime) = 2019

UNION ALL

SELECT 
  pickup_locationid,
  pickup_datetime,
  pickup_borough,
  pickup_zone,
  dropoff_locationid,
  dropoff_datetime,
  dropoff_borough,
  dropoff_zone,
  'FHV' as service_type
FROM `peak-academy-343418.dbt_swong.fact_fhv_trips`
WHERE EXTRACT(year from pickup_datetime) = 2019;