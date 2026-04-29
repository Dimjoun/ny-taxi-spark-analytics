from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour

spark = SparkSession.builder.appName("Optimizer_Test_NoBroadcast").getOrCreate()

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.adaptive.enabled", "false")

trips = spark.read.parquet(
    "hdfs://hdfs-namenode:9000/user/dtzounidis/data/parquet_v2/yellow_2024_partitioned"
)

zones = spark.read.parquet(
    "hdfs://hdfs-namenode:9000/user/dtzounidis/data/parquet_v2/zones"
)

small_zones = zones.limit(50)

filtered = (
    trips
    .filter(col("pickup_date").isin("2024-01-17", "2024-01-18", "2024-01-19"))
    .filter(hour(col("tpep_pickup_datetime")).isin(1, 2, 3, 4))
)

joined = (
    filtered
    .join(
        small_zones,
        col("PULocationID") == col("LocationID"),
        "inner"
    )
)

joined.explain(True)
joined.count()

spark.stop()
