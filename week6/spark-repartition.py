import os
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('spark-repartition') \
    .getOrCreate()

# Read the November 2025 Yellow taxi data
df = spark.read.parquet("yellow_tripdata_2025-11.parquet")
print(f"Row count: {df.count()}")
print(f"Partitions before: {df.rdd.getNumPartitions()}")

# Repartition to 4 and save
df.repartition(4).write.mode("overwrite").parquet("output/yellow_2025_11_repartitioned")

# Calculate average parquet file size
output_dir = "output/yellow_2025_11_repartitioned"
parquet_files = [f for f in os.listdir(output_dir) if f.endswith(".parquet")]
sizes = [os.path.getsize(os.path.join(output_dir, f)) for f in parquet_files]

print(f"\nParquet files created: {len(parquet_files)}")
for f, s in zip(parquet_files, sizes):
    print(f"  {f}: {s / (1024**2):.2f} MB")

avg_mb = sum(sizes) / len(sizes) / (1024**2)
print(f"\nAverage file size: {avg_mb:.2f} MB")

spark.stop()
