from pyspark.sql import SparkSession
import math
from datetime import datetime

spark = SparkSession.builder.appName("Q1_RDD_Parquet").getOrCreate()

sc = spark.sparkContext

df = spark.read.parquet(
    "hdfs://hdfs-namenode:9000/user/dtzounidis/data/parquet_v2/yellow_2015_typed"
)

rdd = df.rdd

def haversine(lat1, lon1, lat2, lon2):

    R = 6371.0

    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)

    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )

    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c


def process(row):

    try:

        pickup = row.tpep_pickup_datetime
        dropoff = row.tpep_dropoff_datetime

        hour = pickup.hour

        if hour not in [1, 2, 3, 4]:
            return None

        duration = (dropoff - pickup).total_seconds() / 60.0

        if duration <= 0:
            return None

        lat1 = row.pickup_latitude
        lon1 = row.pickup_longitude
        lat2 = row.dropoff_latitude
        lon2 = row.dropoff_longitude

        if (
            lat1 == 0
            or lon1 == 0
            or lat2 == 0
            or lon2 == 0
        ):
            return None

        dist = haversine(lat1, lon1, lat2, lon2)

        return (hour, (1, duration, dist))

    except:
        return None


filtered = rdd.map(process).filter(lambda x: x is not None)

aggregated = (
    filtered
    .reduceByKey(
        lambda a, b: (
            a[0] + b[0],
            a[1] + b[1],
            a[2] + b[2]
        )
    )
)

result = (
    aggregated
    .mapValues(
        lambda x: (
            x[0],
            x[1] / x[0],
            x[2] / x[0]
        )
    )
    .sortByKey()
)

for row in result.collect():

    print(
        row[0],
        row[1][0],
        round(row[1][1], 3),
        round(row[1][2], 3)
    )

spark.stop()
