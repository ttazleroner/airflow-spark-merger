from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

minio_access_key = os.getenv("AWS_ACCESS_KEY_ID", "admin")
minio_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "adminadmin")
db_pass = os.getenv("ICEBERG_DB_PASS", "airflow")

ICEBERG_PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "org.postgresql:postgresql:42.6.0",
])

spark = SparkSession.builder \
    .appName("steam_to_iceberg") \
    .config("spark.jars.packages", ICEBERG_PACKAGES) \
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.demo.type", "jdbc") \
    .config("spark.sql.catalog.demo.uri", "jdbc:postgresql://postgres:5432/airflow") \
    .config("spark.sql.catalog.demo.jdbc.user", "airflow") \
    .config("spark.sql.catalog.demo.jdbc.password", db_pass) \
    .config("spark.sql.catalog.demo.jdbc.schema-version", "V1") \
    .config("spark.sql.catalog.demo.warehouse", "s3a://gold-bucket/warehouse") \
    .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()

spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.db.windowed_stats (
        window STRUCT<start: TIMESTAMP, end: TIMESTAMP>,
        category STRING,
        total_sum BIGINT,
        tx_count BIGINT
    ) USING iceberg
""")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka_broker:29092") \
    .option("subscribe", "raw_transactions") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

my_schema = "id INT, user STRING, amount INT, timestamp LONG, category STRING"

df_pars = df.select(
    F.from_json(F.col("value").cast("string"), my_schema).alias("data")
).select("data.*").withColumn(
    "event_time", F.from_unixtime(F.col("timestamp")).cast("timestamp")
)

df_winda = df_pars \
    .withWatermark("event_time", "15 minutes") \
    .groupBy(
        F.window(F.col("event_time"), "10 minutes", "1 minutes"),
        F.col("category"),
    ) \
    .agg(F.sum("amount").alias("total_sum"), F.count("id").alias("tx_count"))

query = df_winda.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", "/home/jovyan/work/data/checkpoints/windowed_stats") \
    .trigger(availableNow=True) \
    .toTable("demo.db.windowed_stats")

query.awaitTermination()
