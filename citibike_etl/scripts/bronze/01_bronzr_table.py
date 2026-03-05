import pyspark.sql.functions as F
from pyspark.sql.types import *
import sys

landing_path = "/Volumes/citibikes_dev/00_landing/operational_data/rides/*"

pipeline_id = sys.argv[1]
run_id = sys.argv[2]
task_id = sys.argv[3]
processed_timestamp = sys.argv[4]
catalog = sys.argv[5]
schema = StructType(
    [
        StructField("rider_d", StringType(), True),
        StructField("rideable_type", StringType(), True),
        StructField("started_at", TimestampType(), True),
        StructField("ended_at", TimestampType(), True),
        StructField("start_station_name", StringType(), True),
        StructField("start_station_id", StringType(), True),
        StructField("end_station_name", StringType(), True),
        StructField("end_station_id", StringType(), True),
        StructField("start_lat", DecimalType(), True),
        StructField("start_lng", DecimalType(), True),
        StructField("end_lat", DecimalType(), True),
        StructField("end_lng", DecimalType(), True),
        StructField("member_casual", StringType(), True),
    ]
)
df = spark.read.format("csv").load(landing_path, schema=schema, header=True)
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

df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.01_bronze.py_citibikes")
