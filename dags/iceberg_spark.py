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
    .config("spark.hadoop.fs.s3a.secret.key", тут пришлось хардкоднуть пароли, буду фиксить это, а то не запускалось нихуя) \
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