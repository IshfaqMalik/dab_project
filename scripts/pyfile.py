from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.remote("cluster_id =0227-133115-swk9sy3r").getOrCreate()
spark.sql(""" select 1 """).show()
