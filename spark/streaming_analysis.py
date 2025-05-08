from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, FloatType

spark = SparkSession.builder     .appName("Streaming Analysis")     .getOrCreate()

schema = StructType().add("symbol", StringType())     .add("Open", FloatType()).add("Close", FloatType())     .add("High", FloatType()).add("Low", FloatType())     .add("Volume", FloatType()).add("timestamp", StringType())

df = spark.readStream     .format("kafka")     .option("kafka.bootstrap.servers", "kafka:9092")     .option("subscribe", "stocks")     .load()

json_df = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select("data.*")

query = json_df.writeStream     .foreachBatch(lambda batch_df, _: batch_df.write.format("mongo")
                  .mode("append")
                  .option("uri", "mongodb://mongodb/stock_data.realtime_data")
                  .save())     .start()

query.awaitTermination()
