# PYSPARK-NOTES

CHP 1 (CREATE SPARK SESSION)

from pyspark.sql import SparkSession

# Create or get a Spark session
spark = SparkSession.builder \
    .appName("spark introduction") \
    .master("local[*]") \
    .getOrCreate()
