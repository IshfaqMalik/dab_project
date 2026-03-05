import datetime
from src.citibikes.cityibikes_utils import get_trip_duration


def test_get_trip_duration(spark):
    data = [
        (datetime.datetime(2026, 1, 1, 10, 00, 00), datetime.datetime(2026, 1, 1, 10, 10, 00)),
        (datetime.datetime(2026, 1, 1, 10, 00, 00), datetime.datetime(2026, 1, 1, 10, 30, 00)),
    ]
    schema = "start_time timestamp, end_time timestamp"
    df = spark.createDataFrame(data, schema=schema)

    result_df = get_trip_duration(spark, df, "start_time", "end_time", "trip_duration")
    results = result_df.select("trip_duration").collect()

    assert results[0]["trip_duration"] == 10
    assert results[1]["trip_duration"] == 30
