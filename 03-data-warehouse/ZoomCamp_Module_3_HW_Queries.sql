CREATE OR REPLACE EXTERNAL TABLE `airy-sled-482514-m7.nyc_taxi_data.yellow_taxi_external`
OPTIONS (
  format = 'PARQUET',
  uris = [
    'gs://nyt-taxi-data-airy-sled-482514-m7/nyc_taxi_data/taxi_color=yellow/year=2024/*'
  ]
);

CREATE OR REPLACE TABLE `airy-sled-482514-m7.nyc_taxi_data.yellow_taxi_regular`
AS
SELECT *
FROM `airy-sled-482514-m7.nyc_taxi_data.yellow_taxi_external`;


-- 20332093
select count(*)
from `airy-sled-482514-m7.nyc_taxi_data.yellow_taxi_external`;

-- This query will process 0 B when run.
SELECT COUNT(DISTINCT PULocationID)
from `airy-sled-482514-m7.nyc_taxi_data.yellow_taxi_external`;

-- This query will process 155.12 MB when run.
SELECT COUNT(DISTINCT PULocationID)
from `airy-sled-482514-m7.nyc_taxi_data.yellow_taxi_regular`;




-- This query will process 155.12 MB when run.
SELECT PULocationID
from `airy-sled-482514-m7.nyc_taxi_data.yellow_taxi_regular`;

-- This query will process 310.24 MB when run.
SELECT PULocationID,
        DOLocationID 
from `airy-sled-482514-m7.nyc_taxi_data.yellow_taxi_regular`;

-- 8333
select count(*)
from `airy-sled-482514-m7.nyc_taxi_data.yellow_taxi_regular`
where fare_amount = 0;

-- Optimization part
select count(distinct DATE(tpep_dropoff_datetime)) as distinct_date_count
from `airy-sled-482514-m7.nyc_taxi_data.yellow_taxi_regular`;

select count(distinct tpep_dropoff_datetime) as distinct_date
from `airy-sled-482514-m7.nyc_taxi_data.yellow_taxi_regular`;

select distinct VendorID 
from `airy-sled-482514-m7.nyc_taxi_data.yellow_taxi_regular`;


CREATE OR REPLACE TABLE `airy-sled-482514-m7.nyc_taxi_data.yellow_taxi_optimized`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT *
FROM `airy-sled-482514-m7.nyc_taxi_data.yellow_taxi_regular`;

-- This query will process 310.24 MB when run.
select distinct VendorID 
from `airy-sled-482514-m7.nyc_taxi_data.yellow_taxi_regular`
where DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' and '2024-03-15';

-- This query will process 26.84 MB when run.
select distinct VendorID 
from `airy-sled-482514-m7.nyc_taxi_data.yellow_taxi_optimized`
where DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' and '2024-03-15';