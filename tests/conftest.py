import os
import sys
import pytest

sys.path.append(os.getcwd())


# returning spark session
@pytest.fixture
def spark():
    try:
        from databricks.connect import DatabricksSession

        spark = DatabricksSession.builder.getOrCreate()
        print("Using Databricks Session")
    except ImportError:
        try:
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.getOrCreate()
            print("Using Local SparkSession")
        except:
            raise ImportError("Neither DatabricksSession not Local SparkSession available")

    return spark
