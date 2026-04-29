from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, unix_timestamp, avg, count, expr, radians, sin, cos, atan2, sqrt, pow

spark = SparkSession.builder.appName("Q1_DF_Parquet").getOrCreate()

df = spark.read.parquet("hdfs://hdfs-namenode:9000/user/dtzounidis/data/parquet_v2/yellow_2015_typed")

filtered = (
    df.withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
      .withColumn("duration_minutes", (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60.0)
      .filter(col("pickup_hour").isin(1, 2, 3, 4))
      .filter(col("duration_minutes") > 0)
      .filter(col("pickup_latitude").between(-90, 90))
      .filter(col("dropoff_latitude").between(-90, 90))
      .filter(col("pickup_longitude").between(-180, 180))
      .filter(col("dropoff_longitude").between(-180, 180))
      .filter((col("pickup_latitude") != 0) & (col("pickup_longitude") != 0))
      .filter((col("dropoff_latitude") != 0) & (col("dropoff_longitude") != 0))
)

lat1 = radians(col("pickup_latitude"))
lat2 = radians(col("dropoff_latitude"))
dlat = radians(col("dropoff_latitude") - col("pickup_latitude"))
dlon = radians(col("dropoff_longitude") - col("pickup_longitude"))

haversine = 6371.0 * 2.0 * atan2(
    sqrt(pow(sin(dlat / 2.0), 2) + cos(lat1) * cos(lat2) * pow(sin(dlon / 2.0), 2)),
    sqrt(1.0 - (pow(sin(dlat / 2.0), 2) + cos(lat1) * cos(lat2) * pow(sin(dlon / 2.0), 2)))
)

result = (
    filtered.withColumn("haversine_km", haversine)
            .groupBy("pickup_hour")
            .agg(
                count("*").alias("Trips"),
                avg("duration_minutes").alias("AvgDurationMin"),
                expr("percentile_approx(haversine_km, 0.9)").alias("P90HaversineKm")
            )
            .orderBy("pickup_hour")
)

result.show(truncate=False)
spark.stop()
