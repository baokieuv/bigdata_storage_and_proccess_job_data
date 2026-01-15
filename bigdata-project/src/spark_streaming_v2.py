from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, current_timestamp, lit, avg, count, sum, max,
    when, upper, trim, from_unixtime, to_timestamp, expr, first, concat_ws
)
from pyspark.sql.types import (
    StructType, StringType, LongType, DoubleType, BooleanType, IntegerType
)

# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("JobStreamingAnalyticsEnhanced") \
    .config("spark.cores.max", "8") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.memory", "512m") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/*") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/*") \
    .config("spark.es.nodes", "elasticsearch.default.svc.cluster.local") \
    .config("spark.es.port", "9200") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

kafka_bootstrap = "my-cluster-kafka-bootstrap.default.svc.cluster.local:9092"
topic = "jobs-topic"

# Schema cho normalized data
schema = StructType() \
    .add("job_id", StringType()) \
    .add("source", StringType()) \
    .add("company_name", StringType()) \
    .add("title", StringType()) \
    .add("description", StringType()) \
    .add("location", StringType()) \
    .add("location_country", StringType()) \
    .add("location_city", StringType()) \
    .add("location_state", StringType()) \
    .add("salary_min", DoubleType()) \
    .add("salary_max", DoubleType()) \
    .add("salary_currency", StringType()) \
    .add("work_type", StringType()) \
    .add("formatted_work_type", StringType()) \
    .add("contract_type", StringType()) \
    .add("experience_level", StringType()) \
    .add("remote_allowed", BooleanType()) \
    .add("listed_time", LongType()) \
    .add("views", IntegerType()) \
    .add("applies", IntegerType()) \
    .add("category", StringType()) \
    .add("latitude", DoubleType()) \
    .add("longitude", DoubleType()) \
    .add("ingest_timestamp", DoubleType())

print("Starting Streaming Job...")

# Read stream from Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("subscribe", topic) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Parse JSON
df_parsed = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Convert timestamp
df_with_time = df_parsed.withColumn(
    "timestamp", 
    to_timestamp(col("ingest_timestamp"))
)

# ===== STREAM TRANSFORMATION & ENRICHMENT =====
print("Applying transformations...")

df_transformed = df_with_time \
    .withColumn("company_name_clean", upper(trim(col("company_name")))) \
    .withColumn("location_clean", upper(trim(col("location")))) \
    .withColumn("salary_avg", 
               when((col("salary_min").isNotNull()) & (col("salary_max").isNotNull()),
                    (col("salary_min") + col("salary_max")) / 2)
               .when(col("salary_max").isNotNull(), col("salary_max"))
               .when(col("salary_min").isNotNull(), col("salary_min"))
               .otherwise(None)) \
    .withColumn("work_type_clean",
               when(col("work_type").rlike("(?i)full"), "FULL_TIME")
               .when(col("work_type").rlike("(?i)part"), "PART_TIME")
               .when(col("work_type").rlike("(?i)contract"), "CONTRACT")
               .otherwise(upper(col("work_type")))) \
    .withColumn("job_category",
               when(col("title").rlike("(?i)software|developer|engineer"), "Software Engineering")
               .when(col("title").rlike("(?i)data|analyst|scientist"), "Data & Analytics")
               .when(col("title").rlike("(?i)manager|management"), "Management")
               .when(col("title").rlike("(?i)marketing"), "Marketing")
               .otherwise("Other")) \
    .withColumn("@timestamp", col("timestamp")) \
    .withColumn("ingest_type", lit("streaming"))

# ===== OUTPUT 1: RAW DETAIL DATA TO ELASTICSEARCH =====
print("Setting up raw data stream to Elasticsearch...")

query_raw = df_transformed \
    .select(
        col("job_id"),
        col("source"),
        col("company_name_clean").alias("company_name"),
        col("title"),
        col("location_clean").alias("location"),
        col("location_country"),
        col("salary_min"),
        col("salary_max"),
        col("salary_avg"),
        col("salary_currency"),
        col("work_type_clean").alias("work_type"),
        col("experience_level"),
        col("job_category"),
        col("remote_allowed"),
        col("views"),
        col("applies"),
        col("@timestamp"),
        col("ingest_type")
    ) \
    .writeStream \
    .format("org.elasticsearch.spark.sql") \
    .option("checkpointLocation", "/tmp/checkpoint_es_raw_detail") \
    .option("es.resource", "jobs_realtime_detail/_doc") \
    .option("es.index.auto.create", "true") \
    .option("es.mapping.id", "job_id") \
    .outputMode("append") \
    .start()

# ===== OUTPUT 2: AGGREGATION - JOBS BY COMPANY (5-MIN WINDOW) =====
print("Setting up company aggregation stream...")

windowed_company = df_transformed \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("company_name_clean"),
        col("source")
    ) \
    .agg(
        count("job_id").alias("job_count"),
        avg("salary_avg").alias("avg_salary"),
        sum(when(col("remote_allowed") == True, 1).otherwise(0)).alias("remote_count")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("company_name_clean").alias("company_name"),
        col("source"),
        col("job_count"),
        col("avg_salary"),
        col("remote_count")
    ) \
    .withColumn("@timestamp", col("window_start")) \
    .withColumn("doc_id", concat_ws("_", col("company_name"), col("source"), col("window_start").cast("string")))

query_company = windowed_company.writeStream \
    .outputMode("update") \
    .format("org.elasticsearch.spark.sql") \
    .option("checkpointLocation", "/tmp/checkpoint_es_company") \
    .option("es.resource", "jobs_realtime_company_agg/_doc") \
    .option("es.index.auto.create", "true") \
    .option("es.mapping.id", "doc_id") \
    .start()

# ===== OUTPUT 3: AGGREGATION - JOBS BY LOCATION (5-MIN WINDOW) =====
print("Setting up location aggregation stream...")

windowed_location = df_transformed \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("location_country"),
        col("location_city")
    ) \
    .agg(
        count("job_id").alias("job_count"),
        avg("salary_avg").alias("avg_salary"),
        count(when(col("source") == "linkedin", 1)).alias("linkedin_jobs"),
        count(when(col("source") == "adzuna", 1)).alias("adzuna_jobs")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("location_country"),
        col("location_city"),
        col("job_count"),
        col("avg_salary"),
        col("linkedin_jobs"),
        col("adzuna_jobs")
    ) \
    .withColumn("@timestamp", col("window_start")) \
    .withColumn("doc_id", concat_ws("_", col("location_country"), col("location_city"), col("window_start").cast("string")))

query_location = windowed_location.writeStream \
    .outputMode("update") \
    .format("org.elasticsearch.spark.sql") \
    .option("checkpointLocation", "/tmp/checkpoint_es_location") \
    .option("es.resource", "jobs_realtime_location_agg/_doc") \
    .option("es.index.auto.create", "true") \
    .option("es.mapping.id", "doc_id") \
    .start()

# ===== OUTPUT 4: AGGREGATION - JOBS BY CATEGORY & EXPERIENCE (10-MIN WINDOW) =====
print("Setting up category aggregation stream...")

windowed_category = df_transformed \
    .withWatermark("timestamp", "15 minutes") \
    .groupBy(
        window(col("timestamp"), "10 minutes"),
        col("job_category"),
        col("experience_level")
    ) \
    .agg(
        count("job_id").alias("job_count"),
        avg("salary_avg").alias("avg_salary")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("job_category"),
        col("experience_level"),
        col("job_count"),
        col("avg_salary")
    ) \
    .withColumn("@timestamp", col("window_start"))\
    .withColumn("doc_id", concat_ws("_", col("job_category"), col("experience_level"), col("window_start").cast("string")))

query_category = windowed_category.writeStream \
    .outputMode("update") \
    .format("org.elasticsearch.spark.sql") \
    .option("checkpointLocation", "/tmp/checkpoint_es_category") \
    .option("es.resource", "jobs_realtime_category_agg/_doc") \
    .option("es.index.auto.create", "true") \
    .option("es.mapping.id", "doc_id") \
    .start()

# ===== OUTPUT 5: AGGREGATION - SOURCE COMPARISON (5-MIN WINDOW) =====
print("Setting up source comparison stream...")

windowed_source = df_transformed \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("source")
    ) \
    .agg(
        count("job_id").alias("total_jobs"),
        avg("salary_avg").alias("avg_salary"),
        sum(when(col("remote_allowed") == True, 1).otherwise(0)).alias("remote_jobs"),
        count(when(col("salary_avg").isNotNull(), 1)).alias("jobs_with_salary")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("source"),
        col("total_jobs"),
        col("avg_salary"),
        col("remote_jobs"),
        col("jobs_with_salary")
    ) \
    .withColumn("@timestamp", col("window_start")) \
    .withColumn("doc_id", concat_ws("_", col("source"), col("window_start").cast("string")))


query_source = windowed_source.writeStream \
    .outputMode("update") \
    .format("org.elasticsearch.spark.sql") \
    .option("checkpointLocation", "/tmp/checkpoint_es_source") \
    .option("es.resource", "jobs_realtime_source_agg/_doc") \
    .option("es.index.auto.create", "true") \
    .option("es.mapping.id", "doc_id") \
    .start()

# ===== OUTPUT 6: CONSOLE OUTPUT (FOR DEBUGGING) =====
print("Setting up console output...")

query_console = windowed_company.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", "10") \
    .start()

print("All streaming queries started successfully!")
spark.streams.awaitAnyTermination()