from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CheckPySpark") \
    .master("local[*]") \
    .getOrCreate()

print(spark.version)
