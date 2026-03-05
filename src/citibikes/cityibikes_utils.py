import pyspark.sql.functions as F


def get_trip_duration(spark, df, start_col, end_col, output_col):
    """
    Adds a column to adatframe calculating the difference in minutes between two timestamps
    Parameters:
               spark:spark session
               df: Dataframe
               start_col: Name of the column with start timestamp
               end_col: Name of the column with end time stamp
               output_col: Name of the column to be added to the DF
    Returns:
            Dataframe with addditional column showing the difference in minutes
    """
    return df.withColumn(output_col, (F.unix_timestamp(F.col(end_col)) - F.unix_timestamp(F.col(start_col))) / 60)
