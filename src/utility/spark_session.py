from databricks.connect import DatabricksSession


def get_spark(profile="DEFAULT"):
    return DatabricksSession.builder.profile(profile).getOrCreate()
