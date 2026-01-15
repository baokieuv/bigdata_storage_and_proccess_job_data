from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, current_timestamp, lit
from pyspark.sql.types import StructType, StringType, LongType

# 1. Spark Session
spark = SparkSession.builder \
    .appName("JobStreamingAnalytics") \
    .config("spark.cores.max", "8") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.memory", "512m") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/*") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/*") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

kafka_bootstrap = "my-cluster-kafka-bootstrap.default.svc.cluster.local:9092"
topic = "jobs-topic"

# 3. Schema (chỉ field realtime cần thiết)
schema = StructType() \
    .add("job_id", StringType()) \
    .add("company_name", StringType()) \
    .add("location", StringType()) \
    .add("work_type", StringType()) \
    .add("formatted_work_type", StringType()) \
    .add("ingest_timestamp", StringType()) # Kafka producer gửi dạng float/string


# 4. Read stream from Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("subscribe", topic) \
    .option("startingOffsets", "latest") \
    .load()

# 5. Parse JSON + giữ raw
df_parsed = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Chuyển ingest_timestamp thành TimestampType để dùng cho Window
df_with_time = df_parsed.withColumn("timestamp", 
                                    col("ingest_timestamp").cast("double").cast("timestamp"))

# 6. Output dataframe (analytics-ready)
df_clean_stream = df_with_time \
    .withColumn("ingest_type", lit("streaming")) \
    .withColumn("@timestamp", col("timestamp")) # Mapping cho Kibana time filter

query_raw = df_clean_stream.writeStream \
    .format("org.elasticsearch.spark.sql") \
    .option("checkpointLocation", "/tmp/checkpoint_es_raw") \
    .option("es.nodes", "elasticsearch.default.svc.cluster.local") \
    .option("es.port", "9200") \
    .option("es.resource", "jobs-realtime/_doc") \
    .option("es.index.auto.create", "true") \
    .outputMode("append") \
    .start()

windowed_counts = df_with_time \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "10 minutes", "5 minutes"),
        col("company_name")
    ) \
    .count() \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("company_name"),
        col("count").alias("job_count")
    )
    
query_agg = windowed_counts.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .option("es.nodes", "elasticsearch.default.svc.cluster.local") \
    .option("es.port", "9200") \
    .option("es.resource", "jobs-realtime/_doc") \
    .option("es.index.auto.create", "true") \
    .start()

spark.streams.awaitAnyTermination()

# ==============================
# 7. Write to Elasticsearch
# ==============================
# query = df_out.writeStream \
#     .format("org.elasticsearch.spark.sql") \
#     .option("es.nodes", "elasticsearch.default.svc.cluster.local") \
#     .option("es.port", "9200") \
#     .option("es.resource", "jobs-streaming/_doc") \
#     .option("es.index.auto.create", "true") \
#     .option("checkpointLocation", "/tmp/checkpoint-jobs-streaming") \
#     .outputMode("append") \
#     .start()

# spark.streams.awaitAnyTermination()