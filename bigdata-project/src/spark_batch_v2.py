from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, regexp_replace, current_timestamp, lit, to_date, 
    from_unixtime, avg, count, sum, min, max, stddev, percentile_approx,
    upper, trim, lower, split, size, array_contains, expr, coalesce,
    datediff, dayofweek, hour, month, year, quarter
)
from pyspark.sql.types import DoubleType, IntegerType
from datetime import datetime, timedelta

# --- Config ---
YESTERDAY = datetime.now().strftime("%Y-%m-%d")
print(f"--- Running Enhanced Batch Job for Date: {YESTERDAY} ---")

# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName(f"JobAnalyticsEnhanced_{YESTERDAY}") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio.default.svc.cluster.local:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.cores.max", "12") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.memory", "512m") \
    .config("spark.cassandra.connection.host", "cassandra.default.svc.cluster.local") \
    .config("spark.es.nodes", "elasticsearch.default.svc.cluster.local") \
    .config("spark.es.port", "9200") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

input_path = f"s3a://job-raw-data/raw/event_date={YESTERDAY}/*.json"

try:
    df_raw = spark.read.json(input_path)
    
    if df_raw.rdd.isEmpty():
        print("No data found for yesterday. Exiting.")
        spark.stop()
        exit(0)
    
    print(f"Raw records: {df_raw.count()}")
    
    # ===== BƯỚC 1: DATA CLEANING & VALIDATION =====
    print("Step 1: Data Cleaning & Validation...")
    
    # Loại bỏ duplicate dựa trên job_id
    df_dedup = df_raw.dropDuplicates(['job_id'])
    print(f"After deduplication: {df_dedup.count()} records")
    
    # Loại bỏ records với missing critical fields
    df_clean = df_dedup.filter(
        col("job_id").isNotNull() & 
        (col("job_id") != "") &
        col("company_name").isNotNull() & 
        (col("company_name") != "") &
        col("title").isNotNull() & 
        (col("title") != "")
    )
    print(f"After validation: {df_clean.count()} records")
    
    # ===== BƯỚC 2: DATA TRANSFORMATION & ENRICHMENT =====
    print("Step 2: Data Transformation & Enrichment...")
    
    # Chuẩn hóa text fields
    df_transformed = df_clean \
        .withColumn("company_name_clean", upper(trim(col("company_name")))) \
        .withColumn("title_clean", trim(col("title"))) \
        .withColumn("location_clean", upper(trim(col("location")))) \
        .withColumn("location_country_clean", upper(trim(col("location_country"))))
    
    # Xử lý salary - tính average salary
    df_transformed = df_transformed \
        .withColumn("salary_min_clean", 
                   when(col("salary_min").isNotNull() & (col("salary_min") > 0), 
                        col("salary_min")).otherwise(None)) \
        .withColumn("salary_max_clean", 
                   when(col("salary_max").isNotNull() & (col("salary_max") > 0), 
                        col("salary_max")).otherwise(None)) 
        
    # Quy đổi GBP sang USD (tỷ giá ước lượng: 1 GBP = 1.27 USD)
    df_transformed = df_transformed \
        .withColumn("salary_min_usd",
                   when(col("salary_currency") == "GBP", col("salary_min_clean") * 1.27)
                   .otherwise(col("salary_min_clean"))) \
        .withColumn("salary_max_usd",
                   when(col("salary_currency") == "GBP", col("salary_max_clean") * 1.27)
                   .otherwise(col("salary_max_clean")))
        # .withColumn("salary_avg", 
        #            when((col("salary_min_clean").isNotNull()) & (col("salary_max_clean").isNotNull()),
        #                 (col("salary_min_clean") + col("salary_max_clean")) / 2)
        #            .when(col("salary_max_clean").isNotNull(), col("salary_max_clean"))
        #            .when(col("salary_min_clean").isNotNull(), col("salary_min_clean"))
        #            .otherwise(None))
    # Tính average salary
    df_transformed = df_transformed \
        .withColumn("salary_avg", 
                   when((col("salary_min_usd").isNotNull()) & (col("salary_max_usd").isNotNull()),
                        (col("salary_min_usd") + col("salary_max_usd")) / 2)
                   .when(col("salary_max_usd").isNotNull(), col("salary_max_usd"))
                   .when(col("salary_min_usd").isNotNull(), col("salary_min_usd"))
                   .otherwise(None))
    
    # Tính salary range
    df_transformed = df_transformed \
        .withColumn("salary_range", 
                   when(col("salary_max_usd").isNotNull() & col("salary_min_usd").isNotNull(),
                        col("salary_max_usd") - col("salary_min_usd"))
                   .otherwise(None))
    
    # Phân loại salary range
    df_transformed = df_transformed \
        .withColumn("salary_category",
                   when(col("salary_avg").isNull(), "Not Specified")
                   .when(col("salary_avg") < 30000, "Entry Level")
                   .when((col("salary_avg") >= 30000) & (col("salary_avg") < 60000), "Mid Level")
                   .when((col("salary_avg") >= 60000) & (col("salary_avg") < 100000), "Senior Level")
                   .otherwise("Executive Level"))
    
    # Xử lý Experience Level với NLP-based logic
    df_transformed = df_transformed \
        .withColumn("experience_level_derived",
                   when(col("title_clean").rlike("(?i)intern|internship"), "Intern")
                   .when(col("title_clean").rlike("(?i)junior|jr\\.|entry|graduate|associate"), "Junior")
                   .when(col("title_clean").rlike("(?i)senior|sr\\.|lead|principal|staff"), "Senior")
                   .when(col("title_clean").rlike("(?i)manager|director|head|vp|chief|cto|ceo"), "Manager/Executive")
                   .otherwise("Mid-Level"))
    
    # Sử dụng experience_level từ source nếu có, nếu không thì dùng derived
    df_transformed = df_transformed \
        .withColumn("experience_level_final",
                   when(col("experience_level").isNotNull() & (col("experience_level") != "Not Specified"),
                        col("experience_level"))
                   .otherwise(col("experience_level_derived")))
    
    # Extract job category từ title (simplified)
    df_transformed = df_transformed \
        .withColumn("job_category",
                   when(col("title_clean").rlike("(?i)software|developer|engineer|programming|backend|frontend|fullstack"), "Software Engineering")
                   .when(col("title_clean").rlike("(?i)data|analyst|scientist|analytics|bi|business intelligence"), "Data & Analytics")
                   .when(col("title_clean").rlike("(?i)manager|management|director|product manager"), "Management")
                   .when(col("title_clean").rlike("(?i)marketing|social media|seo|content|digital marketing"), "Marketing")
                   .when(col("title_clean").rlike("(?i)sales|account|business development"), "Sales")
                   .when(col("title_clean").rlike("(?i)design|designer|ux|ui|graphic"), "Design")
                   .when(col("title_clean").rlike("(?i)devops|cloud|infrastructure|sre"), "DevOps/Cloud")
                   .when(col("title_clean").rlike("(?i)qa|quality|test|tester"), "QA/Testing")
                   .when(col("title_clean").rlike("(?i)hr|human resource|recruiter"), "Human Resources")
                   .otherwise("Other"))
    
    # Chuẩn hóa work_type
    df_transformed = df_transformed \
        .withColumn("work_type_clean",
                   when(col("work_type").rlike("(?i)full"), "FULL_TIME")
                   .when(col("work_type").rlike("(?i)part"), "PART_TIME")
                   .when(col("work_type").rlike("(?i)contract"), "CONTRACT")
                   .when(col("work_type").rlike("(?i)temporary"), "TEMPORARY")
                   .otherwise("Other"))
    
    # Tính toán thời gian từ khi post
    df_transformed = df_transformed \
        .withColumn("listed_date", to_date(from_unixtime(col("listed_time") / 1000))) \
        .withColumn("event_date", lit(YESTERDAY).cast("date")) \
        .withColumn("days_since_posted", 
                   datediff(col("event_date"), col("listed_date")))
    
    # Phân loại freshness
    df_transformed = df_transformed \
        .withColumn("job_freshness",
                   when(col("days_since_posted") <= 1, "Fresh (< 24h)")
                   .when((col("days_since_posted") > 1) & (col("days_since_posted") <= 7), "Recent (1-7 days)")
                   .when((col("days_since_posted") > 7) & (col("days_since_posted") <= 30), "Active (1-4 weeks)")
                   .otherwise("Old (> 30 days)"))
    
    
    # Extract day of week và month để phân tích xu hướng
    df_transformed = df_transformed \
        .withColumn("posted_day_of_week", dayofweek(col("listed_date"))) \
        .withColumn("posted_month", month(col("listed_date"))) \
        .withColumn("posted_quarter", quarter(col("listed_date")))
        
    # Thêm metadata
    df_transformed = df_transformed \
        .withColumn("ingest_type", lit("batch")) \
        .withColumn("processing_timestamp", current_timestamp())
    
    # ===== BƯỚC 3: FEATURE ENGINEERING =====
    print("Step 3: Feature Engineering...")
    
    # Tính competitive score dựa trên views và applies
    df_transformed = df_transformed \
        .withColumn("competition_score",
                   when((col("views") > 0) & (col("applies") > 0),
                        (col("applies").cast("double") / col("views").cast("double")) * 100)
                   .otherwise(0.0))
    
    # Đánh dấu high-demand jobs (nhiều views nhưng ít applies)
    df_transformed = df_transformed \
        .withColumn("is_high_demand",
                   when((col("views") > 100) & (col("competition_score") < 5), True)
                   .otherwise(False))
        
    # Phân loại theo region (US vs UK/EU)
    df_transformed = df_transformed \
        .withColumn("region",
                   when(col("location_country_clean") == "US", "North America")
                   .when(col("location_country_clean") == "UK", "Europe")
                   .otherwise("Other"))
    
    # ===== BƯỚC 4: WRITE TO ELASTICSEARCH (Detail Data) =====
    print("Step 4: Writing detail data to Elasticsearch...")
    

    df_es = df_transformed.select(
        col("job_id"),
        col("company_name_clean").alias("company_name"),
        col("title_clean").alias("title"),
        col("location_clean").alias("location"),
        col("location_country_clean").alias("country"),
        col("location_city").alias("city"),
        col("region"),
        col("salary_min_usd").alias("salary_min"),
        col("salary_max_usd").alias("salary_max"),
        col("salary_avg"),
        col("salary_range"),
        col("salary_category"),
        col("work_type_clean").alias("work_type"),
        col("contract_type"),
        col("experience_level_final").alias("experience_level"),
        col("job_category"),
        col("remote_allowed"),
        col("listed_date"),
        col("event_date"),
        col("days_since_posted"),
        col("job_freshness"),
        col("posted_day_of_week"),
        col("posted_month"),
        col("views"),
        col("applies"),
        col("competition_score"),
        col("is_high_demand"),
        col("ingest_type"),
        col("processing_timestamp")
    )
    
    df_es.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.resource", "jobs_batch/_doc") \
        .option("es.mapping.id", "job_id") \
        .mode("append") \
        .save()
    
    print(f"Written {df_es.count()} records to Elasticsearch")
    
    # ===== BƯỚC 5: AGGREGATIONS FOR CASSANDRA =====
    print("Step 5: Creating aggregations for Cassandra...")
    
    # Agg 1: Company-level statistics

    # Agg 1: Company-level statistics
    df_company_stats = df_transformed.groupBy("company_name_clean").agg(
        count("job_id").alias("job_count"),
        avg("salary_avg").alias("avg_salary"),
        min("salary_min_usd").alias("min_salary"),
        max("salary_max_usd").alias("max_salary"),
        stddev("salary_avg").alias("salary_stddev"),
        avg("views").alias("avg_views"),
        avg("applies").alias("avg_applies"),
        sum(when(col("remote_allowed") == True, 1).otherwise(0)).alias("remote_jobs_count")
    ).withColumn("report_date", lit(YESTERDAY))
    
    df_company_stats.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="company_stats", keyspace="job_metrics") \
        .mode("append") \
        .save()
    
    # Agg 2: Location-level statistics
    df_location_stats = df_transformed.groupBy("location_country_clean", "location_city", "region").agg(
        count("job_id").alias("job_count"),
        avg("salary_avg").alias("avg_salary"),
        sum(when(col("remote_allowed") == True, 1).otherwise(0)).alias("remote_jobs_count")
    ).withColumn("report_date", lit(YESTERDAY))
    
    df_location_stats.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="location_stats", keyspace="job_metrics") \
        .mode("append") \
        .save()
    
    # Agg 3: Category & Experience level statistics
    df_category_stats = df_transformed.groupBy("job_category", "experience_level_final").agg(
        count("job_id").alias("job_count"),
        avg("salary_avg").alias("avg_salary"),
        percentile_approx("salary_avg", 0.5).alias("median_salary"),
        percentile_approx("salary_avg", 0.25).alias("p25_salary"),
        percentile_approx("salary_avg", 0.75).alias("p75_salary")
    ).withColumn("report_date", lit(YESTERDAY))
    
    df_category_stats.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="category_stats", keyspace="job_metrics") \
        .mode("append") \
        .save()
    
    # Agg 4: Work type statistics
    df_worktype_stats = df_transformed.groupBy("work_type_clean").agg(
        count("job_id").alias("total_jobs"),
        avg("salary_avg").alias("avg_salary"),
        count(when(col("salary_avg").isNotNull(), 1)).alias("jobs_with_salary")
    ).withColumn("report_date", lit(YESTERDAY))
    
    df_worktype_stats.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="worktype_stats", keyspace="job_metrics") \
        .mode("append") \
        .save()
        
    
    # Agg 5: Temporal statistics (posting trends)
    df_temporal_stats = df_transformed.groupBy("posted_day_of_week", "posted_month").agg(
        count("job_id").alias("job_count"),
        avg("salary_avg").alias("avg_salary")
    ).withColumn("report_date", lit(YESTERDAY))
    
    df_temporal_stats.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="temporal_stats", keyspace="job_metrics") \
        .mode("append") \
        .save()
        
    # Agg 6: Salary range distribution
    df_salary_distribution = df_transformed.groupBy("salary_category", "job_category").agg(
        count("job_id").alias("job_count")
    ).withColumn("report_date", lit(YESTERDAY))
    
    df_salary_distribution.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="salary_distribution", keyspace="job_metrics") \
        .mode("append") \
        .save()
    
    print("Batch Job Completed Successfully!")
    print(f"Total records processed: {df_transformed.count()}")
    
except Exception as e:
    print(f"Error in Batch Job: {e}")
    import traceback
    traceback.print_exc()

spark.stop()