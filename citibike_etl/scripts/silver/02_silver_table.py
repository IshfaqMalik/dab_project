import os
import sys


from citibikes.cityibikes_utils import get_trip_duration
from utility.datetime_utility import timestamp_to_date
import pyspark.sql.functions as F

pipeline_id = sys.argv[1]
run_id = sys.argv[2]
task_id = sys.argv[3]
processed_timestamp = sys.argv[4]
catalog = sys.argv[5]

df = spark.table(f"{catalog}.01_bronze.py_citibikes")

df = get_trip_duration(spark, df, "started_at", "ended_at", "trip_duration_mins")
df = timestamp_to_date(spark, df, "started_at", "trip_start_date")
df = df.withColumn(
    "metadata",
    F.create_map(
        F.lit("pipeline_id"),
        F.lit(pipeline_id),
        F.lit("rund_id"),
        F.lit(run_id),
        F.lit("task_id"),
        F.lit(task_id),
        F.lit("processed_timestamp"),
        F.lit(processed_timestamp),
    ),
)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.02_silver.py_citibikes")
