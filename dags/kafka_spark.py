from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.functions as F
import json

checkpoint_path = "/home/jovyan/work/data/checkpoints/raw_to_silver"
output_path = "/home/jovyan/work/data/silver/transactions"

spark = SparkSession.builder \
    .appName('SeniorDV') \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

schema = 'id INT, user STRING, amount INT, timestamp INT'

spark.sparkContext.setLogLevel('WARN')

df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'kafka_broker:29092') \
    .option('subscribe', 'raw_transactions') \
    .option('startingOffsets', 'earliest') \
    .load()

parsed_df = df.select(
    F.from_json(F.col("value").cast("string"), schema).alias("data")
).select('data.*')

clean_df = parsed_df \
    .withColumn('amount', F.col('amount').cast('double')) \
    .filter(F.col('amount') > 0)


query = clean_df.writeStream \
    .format('parquet') \
    .option('path', output_path) \
    .option('checkpointLocation', checkpoint_path) \
    .outputMode('append') \
    .trigger(availableNow=True) \
    .start()


print('spark is starting and waiting data')
query.awaitTermination()