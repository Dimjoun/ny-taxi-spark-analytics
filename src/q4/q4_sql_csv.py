from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, col, to_date

spark = SparkSession.builder.appName("Q4_SQL_CSV").getOrCreate()

trips = spark.read.csv(
    "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2024.csv",
    header=True,
    inferSchema=True
)

zones = spark.read.parquet(
    "hdfs://hdfs-namenode:9000/user/dtzounidis/data/parquet_v2/zones"
)

trips = trips.withColumn(
    "pickup_date",
    to_date(col("tpep_pickup_datetime"))
)

filtered = (
    trips
    .filter(col("pickup_date").isin("2024-01-17", "2024-01-18", "2024-01-19"))
    .filter(hour(col("tpep_pickup_datetime")).isin(1, 2, 3, 4))
)

filtered.createOrReplaceTempView("trips")
zones.createOrReplaceTempView("zones")

query = """
SELECT
    z.Borough AS PickupBorough,
    COUNT(*) AS Trips,
    SUM(CASE WHEN t.payment_type = 1 THEN 1 ELSE 0 END) / COUNT(*) AS card_share,
    AVG(CASE
        WHEN t.payment_type = 1 AND t.fare_amount > 0
        THEN t.tip_amount / t.fare_amount
        ELSE NULL
    END) AS avg_tip_rate_card
FROM trips t
JOIN zones z
    ON t.PULocationID = z.LocationID
GROUP BY z.Borough
ORDER BY avg_tip_rate_card DESC
"""

result = spark.sql(query)

result.explain(True)

result.show(truncate=False)

spark.stop()
