from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, col

spark = SparkSession.builder.appName("Q4_SQL_Parquet").getOrCreate()

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
