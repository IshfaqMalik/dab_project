import sys
import pyspark.sql.functions as F

catalog = sys.argv[1]

df = spark.table("citibikes_dev.02_silver.py_citibikes")

df = df.groupby("trip_start_date", "start_station_name").agg(
    F.round(F.count("rider_d"), 2).alias("total_trips"),
    F.round(F.min("trip_duration_mins"), 2).alias("min_trip_duration_mins"),
    F.round(F.max("trip_duration_mins"), 2).alias("max_trip_duration_mins"),
    F.round(F.avg("trip_duration_mins"), 2).alias("avg_trip_duration_mins"),
)
df.writeTo(f"{catalog}.03_gold.daily_py_citibikes_station_summary").createOrReplace()
