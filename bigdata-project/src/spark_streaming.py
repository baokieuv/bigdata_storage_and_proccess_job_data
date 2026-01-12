from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    current_timestamp
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)

# 1. Spark Session
spark = SparkSession.builder \
    .appName("KafkaToElasticsearchStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

kafka_bootstrap = "my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092"
topic = "jobs-topic"

# 3. Schema (chỉ field realtime cần thiết)
schema = StructType([
    StructField("job_id", StringType()),
    StructField("views", StringType()),
    StructField("applies", StringType()),
    StructField("listed_time", StringType()),
    StructField("expiry", StringType()),
    StructField("closed_time", StringType()),
    StructField("sponsored", StringType()),
    StructField("remote_allowed", StringType())
])


# 4. Read stream from Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .load()


# 5. Parse JSON + giữ raw
df_parsed = df_kafka.selectExpr(
    "CAST(value AS STRING) as raw_json"
).select(
    col("raw_json"),
    from_json(col("raw_json"), schema).alias("data")
)


# 6. Output dataframe (analytics-ready)
df_out = df_parsed.select(
    col("data.job_id").alias("job_id"),
    col("data.views").cast("int").alias("views"),
    col("data.applies").cast("int").alias("applies"),
    col("data.listed_time").cast("long").alias("listed_time"),
    col("data.expiry").cast("long").alias("expiry"),
    col("data.closed_time").cast("long").alias("closed_time"),
    col("data.sponsored").cast("int").alias("sponsored"),
    col("data.remote_allowed").cast("int").alias("remote_allowed"),
    current_timestamp().alias("ingest_time")
)


# ==============================
# 7. Write to Elasticsearch
# ==============================
query = df_out.writeStream \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", "elasticsearch.default.svc.cluster.local") \
    .option("es.port", "9200") \
    .option("es.resource", "jobs-streaming/_doc") \
    .option("es.index.auto.create", "true") \
    .option("checkpointLocation", "/tmp/checkpoint-jobs-streaming") \
    .outputMode("append") \
    .start()

spark.streams.awaitAnyTermination()
