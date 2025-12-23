from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, current_timestamp
import uuid

spark = SparkSession.builder \
    .appName("MinIOToCassandra") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio.default.svc.cluster.local:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.cassandra.connection.host", "cassandra.default.svc.cluster.local") \
    .getOrCreate()

try:
    df = spark.read.json("s3a://sensor-data/*.json")
    if not df.rdd.isEmpty():
        total = df.agg(sum("counter").alias("total")).collect()[0]["total"]
        print(f"Total calculated: {total}")

        output_df = spark.createDataFrame([(str(uuid.uuid4()), "latest", total)], ["id", "bucket_time", "total_count"]) \
            .withColumn("processed_at", current_timestamp())

        output_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="counters", keyspace="metrics") \
            .mode("append").save()
except Exception as e:
    print(f"Spark Job Error: {e}")

spark.stop()



Muốn chạy spark job -> Minio, Spark, Cassandra

Minio (tạo 1 số file data (.json))

