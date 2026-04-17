from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
from pyspark.sql.types import IntegerType, FloatType

spark = SparkSession.builder \
    .appName("Airflow_spark_vmeste") \
    .getOrCreate()
print('запуск')

raw_path = "/home/jovyan/work/data/raw/dirty_transactions_1gb.csv"
out_path = "/home/jovyan/work/data/silver/transactions_cleaned.parquet"

if not os.path.exists(raw_path):
    print(f'файла нету {raw_path}')
    exit(1)

rn_city = {
        'La': 'Los Angeles',
        'Sf': 'San Francisco',
        'Nyc': 'New York'
    }
grb = ['N/a', 'Unknown\t', 'Unknown', 'NULL', '\t']
rename_cat = ['N/a\t', 'Tech\t']
valid_cat = ["Retail", "Tech", "Food", "Finance", 'Unknown', 'NoName',]

df = spark.read.csv(raw_path, header=True, inferSchema=True)

df = (df
    .withColumn('id', F.col('id').cast(IntegerType()))
    .withColumn('user_id', F.col('user_id').cast(IntegerType()))
    .withColumn('amount', F.regexp_replace(F.col('amount'), ',', '.').cast(FloatType()))
    .withColumn('timestamp', F.to_timestamp(F.col('timestamp')))
    .withColumn('city', F.initcap(F.col('city')))
    .withColumn('category', F.initcap(F.col('category')))
    .withColumn("category", F.trim(F.regexp_replace(F.col("category"), r"\t", ""))))

df = df.withColumn('city', F.trim(F.col('city')))\
            .withColumn('category', F.trim(F.col('category')))

df = df.replace(rename_cat, None)
df = df.replace(grb, None)
df = df.replace(['',' ', '\t'], None, subset=['category'])
df = df.replace(rn_city, subset=['city'])

df = df.fillna('Unknown', subset=['city'])
df = df.fillna({'category': 'Unknown'})
df = df.fillna('1970-01-01 00:00:00', subset=['timestamp'])
df = df.fillna(0, subset=['amount'])

df = df.filter(F.col('category').isin(valid_cat))

df = df.dropDuplicates(['id'])

df.write.mode('overwrite').parquet(out_path)

df.show(20)

spark.stop()