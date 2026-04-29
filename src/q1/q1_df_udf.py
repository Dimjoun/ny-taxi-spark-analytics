from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, unix_timestamp, avg, count, expr, udf
from pyspark.sql.types import DoubleType
import math

spark = SparkSession.builder.appName("Q1_DF_Parquet_UDF").getOrCreate()

df = spark.read.parquet(
    "hdfs://hdfs-namenode:9000/user/dtzounidis/data/parquet_v2/yellow_2015_typed"
)

# UDF for haversine distance
def haversine_udf(lat1, lon1, lat2, lon2):
    if None in (lat1, lon1, lat2, lon2):
        return None

    R = 6371.0

    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)

    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )

    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c


haversine = udf(haversine_udf, DoubleType())

filtered = (
    df.withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
    .withColumn(
        "duration_minutes",
        (unix_timestamp("tpep_dropoff_datetime")
         - unix_timestamp("tpep_pickup_datetime")) / 60.0
    )
    .filter(col("pickup_hour").isin(1, 2, 3, 4))
    .filter(col("duration_minutes") > 0)
)

result = (
    filtered.withColumn(
        "haversine_km",
        haversine(
            col("pickup_latitude"),
            col("pickup_longitude"),
            col("dropoff_latitude"),
            col("dropoff_longitude"),
        ),
    )
    .groupBy("pickup_hour")
    .agg(
        count("*").alias("Trips"),
        avg("duration_minutes").alias("AvgDurationMin"),
        expr("percentile_approx(haversine_km, 0.9)").alias("P90HaversineKm"),
    )
    .orderBy("pickup_hour")
)

result.show(truncate=False)

spark.stop()
