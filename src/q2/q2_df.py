from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, to_date, avg, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Q2_DF_2015").getOrCreate()

K = 11

df = spark.read.parquet(
    "hdfs://hdfs-namenode:9000/user/dtzounidis/data/parquet_v2/yellow_2015_typed"
)

filtered = (
    df
    .filter(hour(col("tpep_pickup_datetime")).isin(1, 2, 3, 4))
    .filter(col("trip_distance") > 0)
    .filter(col("fare_amount") > 0)
    .withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))
    .withColumn("tip_per_mile", col("tip_amount") / col("trip_distance"))
)

daily = (
    filtered
    .groupBy("VendorID", "pickup_date")
    .agg(avg("tip_per_mile").alias("avg_tip_per_mile"))
)

w = Window.partitionBy("VendorID").orderBy(col("avg_tip_per_mile").desc())

result = (
    daily
    .withColumn("rn", row_number().over(w))
    .filter(col("rn") <= K)
    .select("VendorID", "pickup_date", "avg_tip_per_mile")
    .orderBy("VendorID", col("avg_tip_per_mile").desc())
)

result.explain(True)
result.show(50, truncate=False)

spark.stop()
