from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date

spark = SparkSession.builder.appName("csv_to_parquet_v2").getOrCreate()

base = "hdfs://hdfs-namenode:9000"
out = base + "/user/dtzounidis/data/parquet_v2"

df2024 = spark.read.option("header", True).csv(base + "/data/yellow_tripdata_2024.csv")

df2024 = (
    df2024
    .withColumn("VendorID", col("VendorID").cast("int"))
    .withColumn("tpep_pickup_datetime", to_timestamp("tpep_pickup_datetime"))
    .withColumn("tpep_dropoff_datetime", to_timestamp("tpep_dropoff_datetime"))
    .withColumn("passenger_count", col("passenger_count").cast("int"))
    .withColumn("trip_distance", col("trip_distance").cast("double"))
    .withColumn("RatecodeID", col("RatecodeID").cast("int"))
    .withColumn("PULocationID", col("PULocationID").cast("int"))
    .withColumn("DOLocationID", col("DOLocationID").cast("int"))
    .withColumn("payment_type", col("payment_type").cast("int"))
    .withColumn("fare_amount", col("fare_amount").cast("double"))
    .withColumn("extra", col("extra").cast("double"))
    .withColumn("mta_tax", col("mta_tax").cast("double"))
    .withColumn("tip_amount", col("tip_amount").cast("double"))
    .withColumn("tolls_amount", col("tolls_amount").cast("double"))
    .withColumn("improvement_surcharge", col("improvement_surcharge").cast("double"))
    .withColumn("total_amount", col("total_amount").cast("double"))
    .withColumn("congestion_surcharge", col("congestion_surcharge").cast("double"))
    .withColumn("Airport_fee", col("Airport_fee").cast("double"))
    .withColumn("pickup_date", to_date("tpep_pickup_datetime"))
)

df2024.write.mode("overwrite").partitionBy("pickup_date").parquet(out + "/yellow_2024_partitioned")

df2015 = spark.read.option("header", True).csv(base + "/data/yellow_tripdata_2015.csv")

df2015 = (
    df2015
    .withColumn("VendorID", col("VendorID").cast("int"))
    .withColumn("tpep_pickup_datetime", to_timestamp("tpep_pickup_datetime"))
    .withColumn("tpep_dropoff_datetime", to_timestamp("tpep_dropoff_datetime"))
    .withColumn("passenger_count", col("passenger_count").cast("int"))
    .withColumn("trip_distance", col("trip_distance").cast("double"))
    .withColumn("pickup_longitude", col("pickup_longitude").cast("double"))
    .withColumn("pickup_latitude", col("pickup_latitude").cast("double"))
    .withColumn("RateCodeID", col("RateCodeID").cast("int"))
    .withColumn("dropoff_longitude", col("dropoff_longitude").cast("double"))
    .withColumn("dropoff_latitude", col("dropoff_latitude").cast("double"))
    .withColumn("payment_type", col("payment_type").cast("int"))
    .withColumn("fare_amount", col("fare_amount").cast("double"))
    .withColumn("extra", col("extra").cast("double"))
    .withColumn("mta_tax", col("mta_tax").cast("double"))
    .withColumn("tip_amount", col("tip_amount").cast("double"))
    .withColumn("tolls_amount", col("tolls_amount").cast("double"))
    .withColumn("improvement_surcharge", col("improvement_surcharge").cast("double"))
    .withColumn("total_amount", col("total_amount").cast("double"))
)

df2015.write.mode("overwrite").parquet(out + "/yellow_2015_typed")

zones = spark.read.option("header", True).csv(base + "/data/taxi_zone_lookup.csv")

zones = zones.withColumn("LocationID", col("LocationID").cast("int"))

zones.write.mode("overwrite").parquet(out + "/zones_typed")

spark.stop()
