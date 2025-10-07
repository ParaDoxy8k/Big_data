from pyspark.sql import SparkSession
from pyspark.sql.functions import split, current_timestamp, window
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("EventTimeWatermarkingExample") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

lines = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

words = lines.withColumn("word", split(lines["value"], " ").getItem(0)) \
    .withColumn("date", F.current_date()) \
    .withColumn("timestamp", current_timestamp()) \
    .withWatermark("timestamp", "10 seconds")


windowCounts = words.groupBy(
    window(words.timestamp, "10 seconds", "5 seconds"),
    words.word
).count()

query = windowCounts.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
