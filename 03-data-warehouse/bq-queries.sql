-- Create External Table
CREATE OR REPLACE EXTERNAL TABLE `peak-academy-343418.ny_taxi.external_green_taxi_trips_2022`
OPTIONS (
    format='parquet',
    uris=['gs://peak-academy-343418-terra-bucket/nyc_green_taxi_2022.parquet']
);

-- Create BQ Table
CREATE OR REPLACE TABLE `peak-academy-343418.ny_taxi.green_taxi_trips_2022` AS (
  SELECT * FROM `peak-academy-343418.ny_taxi.external_green_taxi_trips_2022`
);


-- Question 1
SELECT COUNT(*) FROM `peak-academy-343418.ny_taxi.external_green_taxi_trips_2022`;
-- 840402


-- Question 2
SELECT COUNT(DISTINCT PULocationID) FROM `peak-academy-343418.ny_taxi.green_taxi_trips_2022`; -- Materialized: 6.41MB
SELECT COUNT(DISTINCT PULocationID) FROM `peak-academy-343418.ny_taxi.external_green_taxi_trips_2022`; -- External: 0MB


-- Question 3
SELECT COUNT(*) 
FROM `peak-academy-343418.ny_taxi.external_green_taxi_trips_2022`
WHERE fare_amount = 0;
-- 1622


-- Question 4:
-- Convert Unix Time to Timestamp for lpep_pickup_datetime, lpep_dropoff_datetime
CREATE OR REPLACE TABLE `peak-academy-343418.ny_taxi.green_taxi_trips_2022` AS (
  SELECT VendorID, 
    TIMESTAMP_MICROS(CAST(lpep_pickup_datetime/1000 AS INT64)) as lpep_pickup_datetime,
    TIMESTAMP_MICROS(CAST(lpep_dropoff_datetime/1000 AS INT64)) as lpep_dropoff_datetime,
    * EXCEPT (VendorID, lpep_pickup_datetime, lpep_dropoff_datetime)
  FROM `peak-academy-343418.ny_taxi.green_taxi_trips_2022`
);


CREATE OR REPLACE TABLE `peak-academy-343418.ny_taxi.green_taxi_trips_2022_partitioned_clustered`
PARTITION BY DATE(lpep_pickup_datetime) 
CLUSTER BY PULocationID 
AS SELECT * FROM `peak-academy-343418.ny_taxi.green_taxi_trips_2022`;


-- Question 5:
SELECT DISTINCT PULocationID 
FROM `peak-academy-343418.ny_taxi.green_taxi_trips_2022_partitioned_clustered`
WHERE DATE(lpep_pickup_datetime) BETWEEN "2022-06-01" AND "2022-06-30";
-- Partitioned/Clustered Table: 1.12MB

SELECT DISTINCT PULocationID 
FROM `peak-academy-343418.ny_taxi.green_taxi_trips_2022`
WHERE DATE(lpep_pickup_datetime) BETWEEN "2022-06-01" AND "2022-06-30";
-- Materialized Table: 12.82MB


-- Question 8:
SELECT COUNT(*) FROM `peak-academy-343418.ny_taxi.green_taxi_trips_2022`
-- Processes 0B because it is using metadata to return row count. It is not processing the table itself.

