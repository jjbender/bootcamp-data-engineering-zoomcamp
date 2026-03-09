from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('homework_q6') \
    .getOrCreate()

# Load yellow taxi data
df = spark.read.parquet("yellow_tripdata_2025-11.parquet")

# Load zone lookup into temp view
zones = spark.read.option("header", "true").csv("taxi_zone_lookup.csv")
zones.createOrReplaceTempView("zones")
df.createOrReplaceTempView("trips")

# Join and find least frequent pickup zone
result = spark.sql("""
    SELECT z.Zone, COUNT(*) as trip_count
    FROM trips t
    JOIN zones z ON t.PULocationID = z.LocationID
    GROUP BY z.Zone
    ORDER BY trip_count ASC
    LIMIT 5
""")

result.show(truncate=False)

spark.stop()
