"""Verify Spark can start and run a simple operation.

Run after sourcing setup-pyspark.ps1:
    & $env:PYSPARK_PYTHON verify_spark.py
"""
from pyspark.sql import SparkSession

try:
    spark = (
        SparkSession.builder
        .appName("VersionCheck")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    print(f"Spark Version: {spark.version}")

    df = spark.createDataFrame([{"test": "Success"}])
    df.show()
    spark.stop()
except Exception as e:
    print(f"Spark failed to start: {e}")
