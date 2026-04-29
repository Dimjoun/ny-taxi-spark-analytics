from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, coalesce, greatest, least, when, lit, hour

spark = SparkSession.builder.appName("Q5_DF_Default").getOrCreate()

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

pickups = (
    filtered
    .groupBy(col("PULocationID").alias("LocationID"))
    .agg(count("*").alias("pickups"))
)

dropoffs = (
    filtered
    .groupBy(col("DOLocationID").alias("LocationID"))
    .agg(count("*").alias("dropoffs"))
)

flows = (
    pickups.alias("p")
    .join(dropoffs.alias("d"), col("p.LocationID") == col("d.LocationID"), "full_outer")
    .select(
        coalesce(col("p.LocationID"), col("d.LocationID")).alias("LocationID"),
        coalesce(col("pickups"), lit(0)).alias("pickups"),
        coalesce(col("dropoffs"), lit(0)).alias("dropoffs")
    )
)

joined = (
    flows.alias("f")
    .join(zones.alias("z"), col("f.LocationID") == col("z.LocationID"), "left")
)

result = (
    joined
    .withColumn(
        "imbalance",
        greatest(col("pickups"), col("dropoffs")) /
        when(least(col("pickups"), col("dropoffs")) == 0, lit(1))
        .otherwise(least(col("pickups"), col("dropoffs")))
    )
    .select(
        col("z.Borough").alias("Borough"),
        col("z.Zone").alias("Zone"),
        col("pickups"),
        col("dropoffs"),
        col("imbalance")
    )
    .orderBy(col("imbalance").desc())
    .limit(11)
)

result.explain(True)
result.show(20, truncate=False)

spark.stop()
