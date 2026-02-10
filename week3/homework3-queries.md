Query 1 : 

-- Create external table in existing dataset
CREATE OR REPLACE EXTERNAL TABLE `project-17f63bf9-2df3-4f7d-afd.zoomcamp.yellow_taxi_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dezoomcamp_hw3_jjbender/yellow_tripdata_2024-*.parquet']
);

-- Count records (Question 1)
SELECT COUNT(*) as record_count
FROM `project-17f63bf9-2df3-4f7d-afd.zoomcamp.yellow_taxi_external`;



-- Estimated bytes (Question 2)
-- Create native/materialized table
CREATE OR REPLACE TABLE `project-17f63bf9-2df3-4f7d-afd.zoomcamp.yellow_taxi_native` AS
SELECT * FROM `project-17f63bf9-2df3-4f7d-afd.zoomcamp.yellow_taxi_external`;



-- On External Table (check estimate before running)
SELECT COUNT(DISTINCT PULocationID)
FROM `project-17f63bf9-2df3-4f7d-afd.zoomcamp.yellow_taxi_external`;

-- On Native Table (check estimate before running)
SELECT COUNT(DISTINCT PULocationID)
FROM `project-17f63bf9-2df3-4f7d-afd.zoomcamp.yellow_taxi_native`;


-- Question 4

SELECT COUNT(*) as zero_fare_count
FROM `project-17f63bf9-2df3-4f7d-afd.zoomcamp.yellow_taxi_native`
WHERE fare_amount = 0;

--Question 5

partition is used for filterin, clustering used for ordering 

WHERE + ORDER BY

-- Question 6

x

-- Question 7

External tables refrence data stored internally

-- Question 8

 Clustering is not always best practice. Reasons: small tables do not benefit, clustering has storage overhead. Clustering only useful when frequently filter or group by specific columns 