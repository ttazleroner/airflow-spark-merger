from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import sum, avg

users = [
    (1, 'Andrey', 'M', 25),
    (2, "Maria", "F", 30),
    (3, "Oleg", "M", 45),
    (4, "Anna", "F", 22),
]

spark = SparkSession \
    .builder\
    .appName("join_users")\
    .getOrCreate()

silver_path = "/home/jovyan/work/data/silver/transactions_cleaned.parquet"

df_silver = spark.read.parquet(silver_path)

users_schema = 'user_id INT, name STRING, gender STRING, age INT'

df_users = spark.createDataFrame(users, schema=users_schema)

df_join = df_silver.join(broadcast(df_users), "user_id", "inner")

df_group = (df_join.groupBy('gender','category')\
    .agg(
        F.sum('amount').alias('total_amount'),
        F.count('id').alias('tx_count'),
        F.round(F.avg('amount'), 2).alias('avg_check')
    )
    .orderBy(F.col('total_amount').desc())
)

join_path = "/home/jovyan/work/data/gold/join_report"

df_group.spark.write.mode('overwrite').parquet()

df_group.show(20)

spark.stop()