from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofmonth, hour

spark = SparkSession.builder.appName("Q3_DF_CSV").getOrCreate()

K = 11

trips = spark.read.option("header", "true").csv(
    "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2024.csv"
)

zones = spark.read.option("header", "true").csv(
    "hdfs://hdfs-namenode:9000/data/taxi_zone_lookup.csv"
)

filtered = (
    trips
    .filter(dayofmonth(col("tpep_pickup_datetime")).isin(17, 18, 19))
    .filter(hour(col("tpep_pickup_datetime")).isin(1, 2, 3, 4))
)

joined = (
    filtered
    .join(
        zones.alias("pu"),
        col("PULocationID") == col("pu.LocationID"),
        "inner"
    )
    .join(
        zones.alias("do"),
        col("DOLocationID") == col("do.LocationID"),
        "inner"
    )
    .filter(col("pu.Borough") != col("do.Borough"))
)

result = (
    joined
    .groupBy(
        col("pu.Borough").alias("PickupBorough"),
        col("do.Borough").alias("DropoffBorough")
    )
    .count()
    .orderBy(col("count").desc())
    .limit(K)
)

result.explain(True)

result.show(50, truncate=False)

spark.stop()
