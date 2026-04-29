from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, count

spark = SparkSession.builder.appName("Q3_DF_Parquet_Flow").getOrCreate()

K = 11

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

pu_zones = zones.select(
    col("LocationID").alias("PU_LocationID"),
    col("Borough").alias("PickupBorough")
)

do_zones = zones.select(
    col("LocationID").alias("DO_LocationID"),
    col("Borough").alias("DropoffBorough")
)

joined = (
    filtered
    .join(pu_zones, filtered.PULocationID == pu_zones.PU_LocationID, "inner")
    .join(do_zones, filtered.DOLocationID == do_zones.DO_LocationID, "inner")
)

result = (
    joined
    .filter(col("PickupBorough") != col("DropoffBorough"))
    .groupBy("PickupBorough", "DropoffBorough")
    .agg(count("*").alias("Trips"))
    .orderBy(col("Trips").desc())
    .limit(K)
)

result.explain(True)
result.show(truncate=False)

spark.stop()
