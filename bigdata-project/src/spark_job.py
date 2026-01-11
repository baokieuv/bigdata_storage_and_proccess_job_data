from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, count, col, when, current_timestamp, lit, coalesce
)
from datetime import datetime, date
import uuid
import sys
import traceback

def get_date_parameter():
    """Lấy tham số --date từ command line"""
    for i, arg in enumerate(sys.argv):
        if arg == "--date" and i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    raise ValueError("Missing required parameter: --date (format: YYYY-MM-DD)")

def create_audit_log(spark, process_date, status, records_read=0, 
                     records_written_exp=0, records_written_work=0, 
                     error_message=None, started_at=None):
    """Tạo audit log record"""
    audit_df = spark.createDataFrame([{
        "id": str(uuid.uuid4()),
        "process_date": process_date,
        "job_name": "job_processing_spark",
        "status": status,
        "records_read": records_read,
        "records_written_exp": records_written_exp,
        "records_written_work": records_written_work,
        "error_message": error_message,
        "started_at": started_at,
        "completed_at": datetime.now()
    }])
    
    audit_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="job_processing_audit", keyspace="metrics") \
        .mode("append").save()

# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("JobProcessingMinIOToCassandra") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio.default.svc.cluster.local:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.cassandra.connection.host", "cassandra.default.svc.cluster.local") \
    .getOrCreate()

process_date = None
started_at = datetime.now()

try:
    # Lấy date parameter từ command line
    date_str = get_date_parameter()
    process_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    
    # Đọc TẤT CẢ file JSON từ MinIO cho ngày cụ thể (hỗ trợ nhiều batch)
    file_path = f"s3a://sensor-data/jobs_{date_str}_*.json"
    print(f"Reading data from: {file_path}")
    
    df = spark.read.json(file_path)
    
    if df.rdd.isEmpty():
        print(f"No data found for date: {date_str}")
        create_audit_log(spark, process_date, "SUCCESS", 0, 0, 0, 
                        "No data to process", started_at)
    else:
        records_read = df.count()
        print(f"Read {records_read} records from {file_path}")
        
        # Xử lý null values: thay thế empty string và null bằng "Unknown"
        df_cleaned = df \
            .withColumn("experience_level", 
                       when(col("formatted_experience_level").isNull() | 
                            (col("formatted_experience_level") == ""), 
                            "Unknown")
                       .otherwise(col("formatted_experience_level"))) \
            .withColumn("work_type", 
                       when(col("formatted_work_type").isNull() | 
                            (col("formatted_work_type") == ""), 
                            "Unknown")
                       .otherwise(col("formatted_work_type"))) \
            .withColumn("salary", 
                       col("normalized_salary").cast("double"))
        
        # 1. Tính lương trung bình theo Experience Level
        salary_by_exp = df_cleaned \
            .groupBy("experience_level") \
            .agg(
                avg("salary").alias("avg_salary"),
                count("*").alias("job_count")
            ) \
            .withColumn("id", lit(str(uuid.uuid4()))) \
            .withColumn("process_date", lit(process_date)) \
            .withColumn("processed_at", current_timestamp()) \
            .select("id", "experience_level", "avg_salary", "job_count", 
                   "process_date", "processed_at")
        
        # Ghi vào Cassandra
        salary_by_exp.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="avg_salary_by_experience", keyspace="metrics") \
            .mode("append").save()
        
        records_written_exp = salary_by_exp.count()
        print(f"Written {records_written_exp} records to avg_salary_by_experience")
        
        # 2. Đếm số job theo Work Type
        jobs_by_type = df_cleaned \
            .groupBy("work_type") \
            .agg(count("*").alias("job_count")) \
            .withColumn("id", lit(str(uuid.uuid4()))) \
            .withColumn("process_date", lit(process_date)) \
            .withColumn("processed_at", current_timestamp()) \
            .select("id", "work_type", "job_count", "process_date", "processed_at")
        
        # Ghi vào Cassandra
        jobs_by_type.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="jobs_by_work_type", keyspace="metrics") \
            .mode("append").save()
        
        records_written_work = jobs_by_type.count()
        print(f"Written {records_written_work} records to jobs_by_work_type")
        
        # Ghi audit log thành công
        create_audit_log(spark, process_date, "SUCCESS", 
                        records_read, records_written_exp, records_written_work,
                        None, started_at)
        
        print(f"✓ Job completed successfully for {date_str}")
        
except Exception as e:
    error_msg = f"{str(e)}\n{traceback.format_exc()}"
    print(f"Spark Job Error: {error_msg}")
    
    # Ghi audit log lỗi
    if process_date:
        create_audit_log(spark, process_date, "FAILED", 
                        0, 0, 0, error_msg[:500], started_at)
    
    sys.exit(1)

finally:
    spark.stop()

