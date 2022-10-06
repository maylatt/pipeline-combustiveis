"""
Spark helpers
"""
from pyspark.sql import SparkSession


def get_spark_session() -> SparkSession:
    """
    Create a spark session
    :return: A spark session
    """
    print("Getting the Spark Session.")
    return SparkSession.builder.getOrCreate()