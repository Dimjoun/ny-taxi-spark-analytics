from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Q3_SQL_CSV").getOrCreate()

K = 11

trips = spark.read.option("header", "true").csv(
    "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2024.csv"
)

zones = spark.read.option("header", "true").csv(
    "hdfs://hdfs-namenode:9000/data/taxi_zone_lookup.csv"
)

trips.createOrReplaceTempView("trips")
zones.createOrReplaceTempView("zones")

query = f"""
SELECT
    pu.Borough AS PickupBorough,
    do.Borough AS DropoffBorough,
    COUNT(*) AS Trips
FROM trips t
JOIN zones pu
    ON t.PULocationID = pu.LocationID
JOIN zones do
    ON t.DOLocationID = do.LocationID
WHERE
    dayofmonth(t.tpep_pickup_datetime) IN (17,18,19)
    AND hour(t.tpep_pickup_datetime) IN (1,2,3,4)
    AND pu.Borough <> do.Borough
GROUP BY
    pu.Borough,
    do.Borough
ORDER BY Trips DESC
LIMIT {K}
"""

result = spark.sql(query)

result.explain(True)

result.show(50, truncate=False)

spark.stop()
