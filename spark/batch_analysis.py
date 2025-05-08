from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min

spark = SparkSession.builder     .appName("Batch Analysis")     .config("spark.mongodb.input.uri", "mongodb://mongodb/stock_data.realtime_data")     .config("spark.mongodb.output.uri", "mongodb://mongodb/stock_data.analytics")     .getOrCreate()

df = spark.read.format("mongo").load()

agg = df.groupBy("symbol").agg(
    avg("Close").alias("avg_close"),
    max("High").alias("max_price"),
    min("Low").alias("min_price")
)

agg.write.format("mongo").mode("overwrite").save()
