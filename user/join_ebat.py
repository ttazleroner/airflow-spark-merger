from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import broadcast
import os

spark = (SparkSession.builder
    .appName('join_gold')
    .getOrCreate())

spark.sparkContext.setLogLevel('WARN')

silver_path = os.getenv("SILVER_PATH", "/home/jovyan/work/data/silver/transactions_cleaned.parquet")
df_silver = spark.read.parquet(silver_path)

my_schema = 'user STRING, segment STRING'
user_data = [("1", "VIP"), ("2", "Standart"), ("3", "Premium")] 
df_user = spark.createDataFrame(user_data, schema=my_schema)

df_silver = df_silver.withColumn("user", F.col("user_id").cast("string"))
df_user = df_user.withColumn("user", F.col("user").cast("string"))

df_join = df_silver.join(broadcast(df_user), on="user", how="inner")

df_group = (df_join
    .withColumn('hour_time', F.hour(F.col('timestamp')))
    .groupBy('hour_time')
    .agg(
        F.count('user').alias('tx_count'),      
        F.sum('amount').alias('total_sum'),
        F.round(F.avg('amount'), 2).alias('avg_value')
    )
    .orderBy(F.col('hour_time').asc())
)

join_analytics = '/home/jovyan/work/data/gold/hourly_activity'
df_group.write.mode('overwrite').parquet(join_analytics)

print("✅ Код отработал! Проверь папку /data/gold/hourly_activity")

spark.stop()



























# from pyspark.sql import SparkSession
# import pyspark.sql.functions as F
# from pyspark.sql.functions import broadcast
# from pyspark.sql.functions import sum, avg

# spark = SparkSession \
#     .builder \
#     .appName('join_gold')\
#     .getOrCreate()

# gold_path = '/home/jovyan/work/data/otchet/otchet_clean.parquet'

# df_gold = spark.read.parquet(gold_path)

# my_shema = 'user INT, amount INT, timestamp LONG'

# df_user = spark.CreateDataFrame(schema=my_shema)

# df_join = df_gold.join(broadcast(df_user), "user_id", "inner")

# df_group = df_join \
#     .withColumn('hour_time', F.hour(F.from_unixtime(F.col('timestamp')))) \
#     .groupBy('hour_time') \
#     .agg(
#         F.count('id').alias('tx_count'),      
#         F.sum('amount').alias('total_sum'),
#         F.round(F.avg('amount'), 2).alias('avg_value')
#     ) \
#     .orderBy(F.col('hour_time').asc())


# join_analytics = '/home/jovyan/work/data/gold/hourly_activity'

# df_group.write.mode('overwrite').parquet(join_analytics)

# spark.stop()


