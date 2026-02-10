-- Week 3 Homework: BigQuery Queries for Yellow Taxi 2024 Data
-- Make sure to replace 'your_project_id' and bucket name with your actual values

-- ============================================
-- 1. Create External Table from GCS
-- ============================================
CREATE OR REPLACE EXTERNAL TABLE `your_project_id.dezoomcamp.yellow_taxi_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dezoomcamp_hw3_jjbender/yellow_tripdata_2024-*.parquet']
);

-- ============================================
-- 2. Create Native/Materialized Table from External
-- ============================================
CREATE OR REPLACE TABLE `your_project_id.dezoomcamp.yellow_taxi_native` AS
SELECT * FROM `your_project_id.dezoomcamp.yellow_taxi_external`;

-- ============================================
-- 3. Count total records (Question about row count)
-- ============================================
SELECT COUNT(*) as total_rows
FROM `your_project_id.dezoomcamp.yellow_taxi_external`;

-- ============================================
-- 4. Count distinct PULocationID (common question)
-- ============================================
-- On External Table (will scan more data)
SELECT COUNT(DISTINCT PULocationID) as distinct_pickup_locations
FROM `your_project_id.dezoomcamp.yellow_taxi_external`;

-- On Native Table (more efficient)
SELECT COUNT(DISTINCT PULocationID) as distinct_pickup_locations
FROM `your_project_id.dezoomcamp.yellow_taxi_native`;

-- ============================================
-- 5. Fare amount = 0 count (common question)
-- ============================================
SELECT COUNT(*) as zero_fare_trips
FROM `your_project_id.dezoomcamp.yellow_taxi_external`
WHERE fare_amount = 0;

-- ============================================
-- 6. Create partitioned and clustered table
-- ============================================
CREATE OR REPLACE TABLE `your_project_id.dezoomcamp.yellow_taxi_partitioned_clustered`
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY PULocationID AS
SELECT * FROM `your_project_id.dezoomcamp.yellow_taxi_external`;

-- ============================================
-- 7. Query comparison: Non-partitioned vs Partitioned
-- ============================================
-- Query on non-partitioned table
SELECT COUNT(*) as trips
FROM `your_project_id.dezoomcamp.yellow_taxi_native`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

-- Query on partitioned table (will scan less data)
SELECT COUNT(*) as trips
FROM `your_project_id.dezoomcamp.yellow_taxi_partitioned_clustered`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

-- ============================================
-- 8. Query with PULocationID filter (benefits from clustering)
-- ============================================
SELECT COUNT(*) as trips
FROM `your_project_id.dezoomcamp.yellow_taxi_partitioned_clustered`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-03-01' AND '2024-03-15'
  AND PULocationID = 132;

-- ============================================
-- 9. Check table sizes and metadata
-- ============================================
SELECT
  table_name,
  ROUND(total_rows / 1000000, 2) as millions_of_rows,
  ROUND(total_logical_bytes / (1024*1024*1024), 2) as size_gb
FROM `your_project_id.dezoomcamp.INFORMATION_SCHEMA.TABLES`
WHERE table_name LIKE 'yellow_taxi%';
