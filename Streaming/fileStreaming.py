from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder \
    .appName("FileStreamingExample") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("status_id", StringType(), True),
    StructField("status_type", StringType(), True),
    StructField("status_published", StringType(), True),
    StructField("num_reactions", IntegerType(), True),
    StructField("num_comments", IntegerType(), True),
    StructField("num_shares", IntegerType(), True),
    StructField("num_likes", IntegerType(), True),
    StructField("num_loves", IntegerType(), True),
    StructField("num_wows", IntegerType(), True),
    StructField("num_hahas", IntegerType(), True),
    StructField("num_sads", IntegerType(), True),
    StructField("num_angrys", IntegerType(), True)
])

lines = spark.readStream.format("csv") \
    .option("header", True) \
    .option("maxFilesPerTrigger", 1) \
    .schema(schema) \
    .load("./")

words = lines.withColumn("date", split(lines["status_published"], " ").getItem(0))

wordCounts = words.groupBy("date", "status_type").count()


query = wordCounts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()