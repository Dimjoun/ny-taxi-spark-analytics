from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, to_date

spark = SparkSession.builder.appName("Q2_SQL_2015").getOrCreate()

df = spark.read.parquet(
    "hdfs://hdfs-namenode:9000/user/dtzounidis/data/parquet_v2/yellow_2015_typed"
)

filtered = (
    df
    .filter(hour(col("tpep_pickup_datetime")).isin(1, 2, 3, 4))
    .filter(col("trip_distance") > 0)
    .filter(col("fare_amount") > 0)
)

filtered = filtered.withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))

filtered.createOrReplaceTempView("trips2015")

query = """
SELECT
    VendorID,
    pickup_date,
    avg_tip_per_mile
FROM (
    SELECT
        VendorID,
        pickup_date,
        AVG(tip_amount / trip_distance) AS avg_tip_per_mile,
        ROW_NUMBER() OVER (
            PARTITION BY VendorID
            ORDER BY AVG(tip_amount / trip_distance) DESC
        ) AS rn
    FROM trips2015
    GROUP BY VendorID, pickup_date
) t
WHERE rn <= 11
ORDER BY VendorID, rn
"""

result = spark.sql(query)

result.explain(True)
result.show(50, truncate=False)

spark.stop()
