from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, count, sum as _sum

spark = SparkSession.builder.appName("Q6_DF").getOrCreate()

trips = spark.read.parquet(
    "hdfs://hdfs-namenode:9000/user/dtzounidis/data/parquet_v2/yellow_2024_partitioned"
)

zones = spark.read.parquet(
    "hdfs://hdfs-namenode:9000/user/dtzounidis/data/parquet_v2/zones"
)

filtered = (
    trips
    .filter(col("pickup_date").isin("2024-01-17", "2024-01-18", "2024-01-19"))
    .filter(hour(col("tpep_pickup_datetime")).isin(1, 2, 3, 4))
)

joined = filtered.join(
    zones,
    filtered.PULocationID == zones.LocationID,
    "inner"
)

result = (
    joined
    .groupBy("VendorID", "service_zone")
    .agg(
        count("*").alias("Trips"),
        _sum("total_amount").alias("TotalRevenue"),
        _sum(col("congestion_surcharge") + col("Airport_fee")).alias("CongestionAirport")
    )
    .filter(col("TotalRevenue") > 0)
    .withColumn("Share", col("CongestionAirport") / col("TotalRevenue"))
    .withColumn("AvgRevenuePerTrip", col("TotalRevenue") / col("Trips"))
    .orderBy("VendorID", "service_zone")
)

result.explain(True)
result.show(truncate=False)

spark.stop()
