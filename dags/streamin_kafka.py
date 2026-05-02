from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName('streamwinda') \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.") \
    .getOrCreate()

df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'kafka_broker:29092') \
    .option('subscribe', 'raw_transactions') \
    .option('startingOffsets', 'earliest') \
    .load()

my_schema = 'id INT, user STRING, amount INT, timestamp LONG, category STRING'

df_pars = df.select(F.from_json(F.col('value').cast('string'), my_schema).alias('data')).select('data.*') \
    .withColumn('event_time', F.from_unixtime(F.col('timestamp')).cast('timestamp'))

df_winda = df_pars \
    .withWatermark('event_time', '15 minutes') \
    .groupBy(
        F.window(F.col('event_time'), '10 minutes', '1 minutes'),
        F.col('category')
    ) \
    .agg(F.sum('amount').alias('total_sum'), F.count('id').alias('tx_count'))

query = df_winda.writeStream \
    .outputMode('complete') \
    .format('console') \
    .option('truncate', 'false') \
    .start()

query.awaitTermination()