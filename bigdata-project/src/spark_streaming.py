"""
Spark Streaming Job: Kafka -> Spark Streaming -> Elasticsearch -> Kibana
Luồng xử lý: 
    - Đọc dữ liệu streaming từ Kafka topic
    - Transform và enrich dữ liệu
    - Ghi vào Elasticsearch để visualize trên Kibana
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import json
import time

# ==================== CẤU HÌNH ====================
KAFKA_BOOTSTRAP_SERVERS = "my-cluster-kafka-bootstrap.default.svc.cluster.local:9092"
KAFKA_TOPIC = "sensor-topic"
ELASTICSEARCH_NODES = "elasticsearch"
ELASTICSEARCH_PORT = "9200"
ELASTICSEARCH_INDEX = "sensor-data"
CHECKPOINT_LOCATION = "/tmp/spark-checkpoint-kafka-es"

# Schema cho dữ liệu từ Kafka (format từ producer.py)
schema = StructType([
    StructField("timestamp", StringType(), nullable=False),
    StructField("counter", IntegerType(), nullable=False)
])


def create_spark_session():
    """
    Tạo SparkSession với các config cần thiết cho Kafka và Elasticsearch
    Dựa trên pattern từ cassandra_es_sync.py và các file config
    """
    spark = SparkSession.builder \
        .appName("KafkaToElasticsearchStreaming") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                "org.elasticsearch:elasticsearch-spark-30_2.12:8.12.0") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("es.nodes", ELASTICSEARCH_NODES) \
        .config("es.port", ELASTICSEARCH_PORT) \
        .config("es.index.auto.create", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def process_streaming_data(df, epoch_id):
    """
    Xử lý từng batch dữ liệu từ Kafka
    Transform và enrich dữ liệu, sau đó ghi vào Elasticsearch
    Pattern tương tự transform_load.py nhưng đơn giản hóa cho streaming
    """
    if df.isEmpty():
        print(f"Batch {epoch_id}: No data to process")
        return
    
    record_count = df.count()
    print(f"Batch {epoch_id}: Processing {record_count} records")
    
    try:
        # Parse JSON từ value column (Kafka trả về binary)
        df_parsed = df.select(
            col("key").cast("string"),
            col("value").cast("string"),
            col("timestamp").alias("kafka_timestamp"),
            col("partition"),
            col("offset")
        ).select(
            from_json(col("value"), schema).alias("data"),
            col("kafka_timestamp"),
            col("partition"),
            col("offset")
        ).select(
            "data.*",
            col("kafka_timestamp"),
            col("partition"),
            col("offset")
        )
        
        # Enrich dữ liệu: thêm các trường metadata và tính toán
        df_enriched = df_parsed.withColumn(
            "processed_at", 
            current_timestamp()
        ).withColumn(
            "date",
            to_date(col("processed_at"))
        ).withColumn(
            "hour",
            hour(col("processed_at"))
        ).withColumn(
            "day_of_week",
            date_format(col("processed_at"), "EEEE")
        ).withColumn(
            "id",
            concat(
                lit(f"batch_{epoch_id}_"),
                col("partition"),
                lit("_"),
                col("offset")
            )
        ).withColumn(
            "timestamp_parsed",
            # Thử parse với nhiều format khác nhau (ISO format từ producer)
            coalesce(
                to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
                to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"),
                to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"),
                to_timestamp(col("timestamp"))
            )
        ).drop("timestamp").withColumnRenamed("timestamp_parsed", "event_timestamp")
        
        # Hiển thị sample data để debug
        print(f"\n--- Batch {epoch_id} Sample Data ---")
        df_enriched.select("id", "event_timestamp", "counter", "processed_at", "hour").show(5, truncate=False)
        
        # Tính toán thống kê
        stats = df_enriched.agg(
            count("*").alias("total_records"),
            sum("counter").alias("total_counter"),
            avg("counter").alias("avg_counter"),
            max("counter").alias("max_counter"),
            min("counter").alias("min_counter")
        ).collect()[0]
        
        print(f"Stats - Total: {stats['total_records']}, "
              f"Sum: {stats['total_counter']}, "
              f"Avg: {stats['avg_counter']:.2f}")
        
        # Ghi vào Elasticsearch (pattern từ cassandra_es_sync.py)
        df_enriched.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.resource", f"{ELASTICSEARCH_INDEX}/_doc") \
            .option("es.nodes", ELASTICSEARCH_NODES) \
            .option("es.port", ELASTICSEARCH_PORT) \
            .option("es.index.auto.create", "true") \
            .option("es.mapping.id", "id") \
            .option("es.write.operation", "index") \
            .mode("append") \
            .save()
        
        print(f"✓ Batch {epoch_id}: Successfully wrote {record_count} records to Elasticsearch index '{ELASTICSEARCH_INDEX}'")
        print("-" * 60)
        
    except Exception as e:
        print(f"✗ Batch {epoch_id}: Error processing data - {str(e)}")
        import traceback
        traceback.print_exc()
        # Không throw exception để streaming tiếp tục chạy


def main():
    """
    Hàm chính để chạy Spark Streaming job
    Luồng: Kafka -> Spark Streaming -> Elasticsearch -> Kibana
    """
    print("=" * 70)
    print("🚀 Starting Kafka to Elasticsearch Streaming Job")
    print("=" * 70)
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC}")
    print(f"Elasticsearch: {ELASTICSEARCH_NODES}:{ELASTICSEARCH_PORT}")
    print(f"Index: {ELASTICSEARCH_INDEX}")
    print("=" * 70)
    
    spark = None
    query = None
    
    try:
        # Tạo SparkSession
        print("\n📦 Creating SparkSession...")
        spark = create_spark_session()
        print("✓ SparkSession created successfully")
        
        # Đọc streaming từ Kafka (retry logic tương tự producer.py)
        print(f"\n📡 Connecting to Kafka...")
        max_retries = 5
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                df_kafka = spark \
                    .readStream \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                    .option("subscribe", KAFKA_TOPIC) \
                    .option("startingOffsets", "latest") \
                    .option("failOnDataLoss", "false") \
                    .option("kafka.consumer.group.id", "spark-streaming-consumer") \
                    .load()
                
                print("✓ Kafka connection established!")
                print(f"Schema: {df_kafka.schema}")
                break
                
            except Exception as e:
                retry_count += 1
                if retry_count < max_retries:
                    print(f"⚠ Kafka not ready yet (attempt {retry_count}/{max_retries}), retrying in 5s...")
                    time.sleep(5)
                else:
                    raise Exception(f"Failed to connect to Kafka after {max_retries} attempts: {str(e)}")
        
        # Xử lý streaming với foreachBatch
        print("\n🔄 Starting streaming query...")
        query = df_kafka \
            .writeStream \
            .foreachBatch(process_streaming_data) \
            .outputMode("update") \
            .trigger(processingTime="10 seconds") \
            .option("checkpointLocation", CHECKPOINT_LOCATION) \
            .start()
        
        print("=" * 70)
        print("✅ Streaming query started successfully!")
        print("📊 Waiting for data from Kafka...")
        print("💡 Data will be automatically written to Elasticsearch")
        print("🌐 Access Kibana to visualize the data")
        print("=" * 70)
        print("\nPress Ctrl+C to stop the streaming job\n")
        
        # Chờ query chạy
        query.awaitTermination()
        
    except KeyboardInterrupt:
        print("\n\n⚠ Received interrupt signal. Stopping streaming...")
    except Exception as e:
        print(f"\n❌ Error in streaming job: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        # Cleanup
        if query:
            try:
                print("\n🛑 Stopping streaming query...")
                query.stop()
            except:
                pass
        
        if spark:
            print("🛑 Stopping Spark session...")
            spark.stop()
        
        print("✅ Job completed.")
        print("=" * 70)


if __name__ == "__main__":
    main()

