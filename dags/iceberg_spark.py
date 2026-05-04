from pyspark.sql import SparkSession
import os

minio_access_key = os.getenv("AWS_ACCESS_KEY_ID", "admin")
minio_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "adminadmin")

ICEBERG_PACKAGES = [
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "org.postgresql:postgresql:42.6.0"
]

spark = SparkSession.builder \
    .appName("IcebergTesting") \
    .config("spark.jars.packages", ",".join(ICEBERG_PACKAGES)) \
    \
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.demo.type", "jdbc") \
    .config("spark.sql.catalog.demo.uri", "jdbc:postgresql://postgres:5432/iceberg_db") \
    .config("spark.sql.catalog.demo.jdbc.user", "airflow") \
    .config("spark.sql.catalog.demo.jdbc.password", "airflow") \
    .config("spark.sql.catalog.demo.warehouse", "s3a://gold-bucket/warehouse") \
    .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO") \
    \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", тут пришлось хардкоднуть пароли, буду фиксить это, а то не запускалось нихуя) \
    .config("spark.hadoop.fs.s3a.secret.key", тут пришлось хардкоднуть пароли, буду фиксить это, а то не запускалось нихуя ) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()

print("айсберг готов")

spark.sql("CREATE TABLE IF NOT EXISTS demo.db.transactions (id INT, user STRING, amount DOUBLE) USING iceberg")

spark.sql("INSERT INTO demo.db.transactions VALUES (1, 'Prostitutka', 500.0), (2, 'Chapa', 1000.0)")

df = spark.table("demo.db.transactions")
df.show()

spark.sql("SELECT * FROM demo.db.transactions.snapshots").show()

new_data = [(1, 'Prostitutka', 777.0), (3,'Pidor', 300.0)]

df_updatedb = spark.createDataFrame(new_data, ['id', 'user', 'amount'])
df_updatedb.createOrReplaceTempView('updates')

spark.sql("""
    MERGE INTO demo.db.transactions t
    USING updates u
    ON t.id = u.id
    WHEN MATCHED THEN UPDATE SET t.amount = u.amount
    WHEN NOT MATCHED THEN INSERT *
""")

spark.sql("""
    CALL demo.system.expire_snapshots(
        table => 'db.transactions', 
        retain_last => 1
    )
""").show

spark.sql("""
    CALL demo.system.remove_orphan_files(
        table => 'db.transactions'
    )
""").show()

spark.sql("""
    CALL demo.system.rewrite_data_files(
        table => 'db.transactions'
    )
""").show()

print('данные готовы')
spark.table('demo.db.transactions').orderBy('id').show()

history_snap = spark.sql('SELECT snapshot_id FROM demo.db.transactions.snapshots').collect()
perviy_snap = history_snap[0]['snapshot_id']
print('олд данные')
spark.read.option('snapshot-id', perviy_snap).table('demo.db.transactions').show()
spark.sql('SELECT file_path, record_count FROM demo.db.transactions.files').show(truncate=False)

print("список файлов таблицы")
spark.sql("SELECT file_path, file_format, record_count, file_size_in_bytes FROM demo.db.transactions.files").show(truncate=False)

# манифесты и физ файлы (это для просмотра что в наличии, оставлю навсякий)
print("список манифестов")
# spark.sql("SELECT path, length, added_files_count, existing_files_count FROM demo.db.transactions.manifests").show(truncate=False)
spark.sql("""
    SELECT 
        path, 
        length, 
        added_data_files_count, 
        existing_data_files_count 
    FROM demo.db.transactions.manifests
""").show(truncate=False)

spark.sql('ALTER TABLE demo.db.transactions ADD PARTITION FIELD user')

spark.sql("INSERT INTO demo.db.transactions VALUES (4, 'New_Guey', 5000.0)")

spark.sql('SELECT file_path, partition FROM demo.db.transactions.files').show(truncate=False)