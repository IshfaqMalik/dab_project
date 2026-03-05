import pyspark.sql.functions as F


def timestamp_to_date(spark, df, timestamp_col, output_col):
    """
    Extracts timestamp column from the table and adds a new date column

    Parameters:
    spark:spark session
    df: input pyspark dataframe with timestamp column
    timestamp_col: timestamp column from dataframe
    output_col: Name of the new column

    Returns:
            DataFrame: A new DataFrame with additional ride date column
    """
    return df.withColumn(output_col, F.to_date(F.col(timestamp_col)))
