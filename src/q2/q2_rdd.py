from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.appName("Q2_RDD_2015").getOrCreate()
sc = spark.sparkContext

K = 11

path = "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv"

lines = sc.textFile(path)

header = lines.first()

data = lines.filter(lambda x: x != header)


def parse(line):
    try:
        parts = line.split(",")

        VendorID = parts[0]

        pickup = parts[1]
        dt = datetime.strptime(pickup, "%Y-%m-%d %H:%M:%S")

        hour = dt.hour

        if hour not in [1, 2, 3, 4]:
            return None

        trip_distance = float(parts[4])
        fare_amount = float(parts[12])
        tip_amount = float(parts[15])

        if trip_distance <= 0 or fare_amount <= 0:
            return None

        date = dt.strftime("%Y-%m-%d")

        tip_per_mile = tip_amount / trip_distance

        return ((VendorID, date), (tip_per_mile, 1))

    except:
        return None


parsed = data.map(parse).filter(lambda x: x is not None)

aggregated = (
    parsed
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
)

averages = (
    aggregated
    .map(lambda x: (
        x[0][0],
        x[0][1],
        x[1][0] / x[1][1]
    ))
)

grouped = averages.groupBy(lambda x: x[0])

topk = grouped.mapValues(
    lambda vals: sorted(
        vals,
        key=lambda x: x[2],
        reverse=True
    )[:K]
)

result = topk.collect()

for vendor, records in result:
    for r in records:
        print(r)

spark.stop()
