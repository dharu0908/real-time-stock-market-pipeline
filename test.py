from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("stream-test")
    .getOrCreate()
)

df = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 5)
    .load()
)

query = (
    df.writeStream
    .format("console")
    .outputMode("append")
    .start()
)

query.awaitTermination()