from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

minio_access_key = os.getenv("AWS_ACCESS_KEY_ID", "admin")
minio_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "adminadmin")
db_pass = os.getenv("ICEBERG_DB_PASS")

ICEBERG_PACKAGES = [
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "org.postgresql:postgresql:42.6.0"
]

spark = SparkSession.builder \
    .appName("IcebergTesting") \
    .config("spark.jars.packages", ",".join(ICEBERG_PACKAGES)) \
    .config("spark.sql.catalog.demo.jdbc.password", db_pass) \
    .config("spark.sql.catalog.demo.jdbc.schema-version", "V1") \
    \
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.demo.type", "jdbc") \
    .config("spark.sql.catalog.demo.uri", "jdbc:postgresql://postgres:5432/airflow") \
    .config("spark.sql.catalog.demo.jdbc.user", "airflow") \
    .config("spark.sql.catalog.demo.warehouse", "s3a://gold-bucket/warehouse") \
    .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO") \
    \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key ) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()

print("айсберг готов")

spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.db.transactions (
        id INT, 
        user STRING, 
        amount DOUBLE, 
        ts TIMESTAMP
    ) 
    USING iceberg 
    PARTITIONED BY (days(ts), bucket(16, user))
""")

#сайз файла /
#группировка по парт ключам//
#если данных овер///
spark.sql("""
    ALTER TABLE demo.db.transactions SET TBLPROPERTIES (
        'write.target-file-size-bytes' = '134217728',
        'write.distribution-mode' = 'hash',
        'write.spark.fanout.enabled' = 'true'
    )
""")

dirty_csv = '/home/jovyan/work/data/raw/dirty_transactions_1gb.csv'
df = spark.read.option('header', 'true').option('inferSchema', 'true').csv(dirty_csv)

df_clean = df.select(
    F.col("id").cast("int").alias("id"),
    F.col("user_id").cast("string").alias("user"),
    F.col("amount").cast("double").alias("amount"),
    F.current_timestamp().alias("ts"),
)

df_clean.writeTo('demo.db.transactions').append()
print("даннык внутри")

#тест данные
new_data = [(1, 'Prostitutka', 777.0, "2026-05-01 10:00:00"), 
            (3, 'Pidor', 300.0, "2026-05-01 11:00:00")]
columns = ['id', 'user', 'amount', 'ts']

df_updates = spark.createDataFrame(new_data, columns) \
    .withColumn('ts', F.to_timestamp('ts'))
df_updates.createOrReplaceTempView('updates')

spark.sql("""
    MERGE INTO demo.db.transactions t
    USING updates u
    ON t.id = u.id
    WHEN MATCHED THEN UPDATE SET t.amount = u.amount, t.ts = u.ts
    WHEN NOT MATCHED THEN INSERT *
""")

spark.sql("""
    CREATE VIEW demo.db.transactions_old AS
    SELECT user, amount, ts FROM demo.db.transactions
""")

print("обслужка")
spark.sql("CALL demo.system.rewrite_data_files(table => 'db.transactions')")
spark.sql("CALL demo.system.expire_snapshots(table => 'db.transactions', retain_last => 5)")
spark.sql("CALL demo.system.remove_orphan_files(table => 'db.transactions')")

spark.sql("""
    SELECT file_path, record_count, file_size_in_bytes, partition 
    FROM demo.db.transactions.files
""").show(truncate=False)