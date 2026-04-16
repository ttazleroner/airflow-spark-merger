from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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
rn_city = {
        'La': 'Los Angeles',
        'Sf': 'San Francisco',
        'Nyc': 'New York'
    }
grb = ['N/a', 'Unknown\t', 'Unknown', 'NULL', '\t']

rename_cat = ['N/a\t', 'Tech\t']

valid_cat = ["Retail", "Tech", "Food", "Finance", 'Unknown', 'NoName',]

df = df.replace(rn_city, subset=['city'])

df = df.fillna(0, subset=['amount'])

df = df.withColumn('city', F.trim(F.col('city')))\
            .withColumn('city', F.trim(F.col('category')))

df = df.replace(grb, None)

df = df.fillna('Unknown', subset=['city'])

df = df.fillna({'category': 'Unknown'})

df = df.replace(rename_cat, None)

df = df.filter(~F.col('category').isin(valid_cat))

df = df.replace(['',' ', '\t'], None, subset=['category'])

df = df.dropDuplicates(['id'])

df.show(20)


spark.stop()