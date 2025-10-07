import os
os.environ["HADOOP_HOME"] = "C\\hadoop\bin"

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

spark = SparkSession.builder.getOrCreate()

read_file = spark.read.format("csv")\
  .option("header",True)\
  .load("fb1.csv")
read_file.printSchema()

lines = spark.readStream.format("socket")\
    .option("host","localhost")\
    .option("port",9999)\
    .load()

words = lines.select(explode(split(lines.value," ")).alias("word"))

wordCounts = words.groupBy("word").count()

query = wordCounts.writeStream\
    .outputMode("complete")\
    .format("console")\
    .start()
query.awaitTermination()