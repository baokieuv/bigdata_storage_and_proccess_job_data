from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark = SparkSession.builder \
    .appName("KafkaToElasticsearchStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

kafka_bootstrap = "my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092"
topic = "jobs-topic"

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .load()

# 👉 Giữ raw JSON (best practice cho Big Data)
df_out = df_kafka.selectExpr(
    "CAST(value AS STRING) as raw_json"
).withColumn(
    "ingest_time", current_timestamp()
)

query = df_out.writeStream \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", "elasticsearch.default.svc.cluster.local") \
    .option("es.port", "9200") \
    .option("es.resource", "jobs-streaming/_doc") \
    .option("checkpointLocation", "/tmp/checkpoint-jobs-streaming") \
    .outputMode("append") \
    .start()

spark.streams.awaitAnyTermination()
