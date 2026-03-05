try:
    from databricks.connect import DatabricksSession

    spark = DatabricksSession.builder.getOrCreate()
    print("Using Databricks Session")
except ImportError:
    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        print("Using local Spark Session")
    except ImportError:
        raise ImportError("Neither Databricks nor Spark session available")
