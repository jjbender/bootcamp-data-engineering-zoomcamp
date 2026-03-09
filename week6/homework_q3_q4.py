from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('homework_q3_q4') \
    .getOrCreate()

df = spark.read.parquet("yellow_tripdata_2025-11.parquet")

# Q3: Count trips that started on November 15th
nov15_count = df.filter(
    F.to_date(F.col("tpep_pickup_datetime")) == "2025-11-15"
).count()
print(f"\nQ3 - Trips on November 15th: {nov15_count:,}")

# Q4: Longest trip in hours
df_with_duration = df.withColumn(
    "duration_hours",
    (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 3600
)
longest = df_with_duration.agg(F.max("duration_hours").alias("max_hours")).collect()[0]["max_hours"]
print(f"Q4 - Longest trip in hours: {longest:.1f}")



spark.stop()
