from src.utility.datetime_utility import timestamp_to_date
import datetime


def test_timestamp_to_date(spark):
    data = [(datetime.datetime(2026, 2, 3, 10, 30, 30),)]
    schema = "ride_timestamp timestamp"
    df = spark.createDataFrame(data, schema=schema)
    result_df = timestamp_to_date(spark, df, "ride_timestamp", "output_time")

    row = result_df.select("output_time").first()

    expected_date = datetime.date(2026, 2, 3)
    assert row["output_time"] == expected_date
