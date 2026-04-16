from pyspark.sql import SparkSession
import os

spark = SparkSession.builder \
    .appName("Airflow_spark_vmeste") \
    .getOrCreate()
print('запуск')

raw_path = "/home/jovyan/work/data/raw/dirty_transactions_1gb.csv"
out_path = "/home/jovyan/work/data/silver/transactions_cleaned.parquet"

if os.path.exists(raw_path):
    df = spark.read.csv(raw_path, header=True, inferSchema=True)
    df.write.mode('overwrite').parquet(out_path)
else:
    print(f'не получилось получить данные из: {out_path}')

spark.stop()