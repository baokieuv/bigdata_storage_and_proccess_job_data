from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_replace, current_timestamp, lit, to_date, from_unixtime, avg, count
from datetime import datetime, timedelta

# --- Config ---
# YESTERDAY = (datetime.now() - timedelta(1)).strftime("%Y-%m-%d")

YESTERDAY = (datetime.now()).strftime("%Y-%m-%d")

print(f"--- Running Batch Job for Date: {YESTERDAY} ---")

# Khởi tạo Spark Session với config cho MinIO và Cassandr
spark = SparkSession.builder \
    .appName(f"JobAnalyticsBatch_{YESTERDAY}") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio.default.svc.cluster.local:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.cores.max", "8") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.memory", "512m") \
    .config("spark.cassandra.connection.host", "cassandra.default.svc.cluster.local") \
    .config("spark.es.nodes", "elasticsearch.default.svc.cluster.local") \
    .config("spark.es.port", "9200") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# MinIO path: s3a://job-raw-data/raw/event_date=YYYY-MM-DD/
input_path = f"s3a://job-raw-data/raw/event_date={YESTERDAY}/*.json"

try:
    df_raw = spark.read.json(input_path)
    
    # Check nếu không có data
    if df_raw.rdd.isEmpty():
        print("No data found for yesterday. Exiting.")
        spark.stop()
        exit(0)
        
    # 2. Data Cleaning & Normalization
    df_clean = df_raw \
        .withColumn("normalized_salary", 
                    regexp_replace(col("max_salary"), "[^0-9.]", "").cast("double")) \
        .withColumn("min_salary_clean", 
                    regexp_replace(col("min_salary"), "[^0-9.]", "").cast("double")) \
        .withColumn("location_clean", 
                    when(col("location").contains(","), 
                         regexp_replace(col("location"), ",.*", "")).otherwise(col("location"))) \
        .withColumn("listed_date", to_date(from_unixtime(col("listed_time") / 1000))) \
        .withColumn("event_date", lit(YESTERDAY).cast("date")) \
        .withColumn("ingest_type", lit("batch"))
        
    # Xử lý Experience Level (Logic đơn giản hóa)
    df_clean = df_clean.withColumn("experience_level_std",
        when(col("title").rlike("(?i)senior|sr\\.|lead|manager"), "Senior")
        .when(col("title").rlike("(?i)junior|jr\\.|fresh|entry"), "Junior")
        .when(col("title").rlike("(?i)intern"), "Intern")
        .otherwise("Mid-Level")
    )   
    
    print("Filtering invalid company names...")
    df_clean = df_clean.filter(
        col("company_name").isNotNull() & (col("company_name") != "")
    )

    # 3. Write to Elasticsearch (Detail Data cho Search/Dashboard)
    print("Writing to Elasticsearch...")
    df_clean.select(
        col("job_id"), col("company_name"), col("title"), 
        col("location_clean").alias("location"), 
        col("normalized_salary"), col("work_type"), 
        col("experience_level_std").alias("experience_level"),
        col("event_date"), col("ingest_type")
    ).write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.resource", "jobs_batch/_doc") \
        .option("es.mapping.id", "job_id") \
        .mode("append") \
        .save()
        
    print("Aggregating and Writing to Cassandra...")
    df_agg = df_clean.groupBy("company_name").agg(
        count("job_id").alias("job_count"),
        avg("normalized_salary").alias("avg_salary")
    ).withColumn("report_date", lit(YESTERDAY))

    df_agg.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="daily_company_stats", keyspace="job_metrics") \
        .mode("append") \
        .save()
        
    print("Batch Job Completed Successfully.")
    
except Exception as e:
    print(f"Error in Batch Job: {e}")

spark.stop()
