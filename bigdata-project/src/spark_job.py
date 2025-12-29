import os
import sys

# Fix cho Windows - ĐƯỜNG DẪN CHÍNH XÁC
os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['PATH'] = os.environ['PATH'] + ';' + r'C:\hadoop\bin'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, count, sum, current_timestamp
import uuid

# Khởi tạo Spark Session với config cho MinIO và Cassandra
spark = SparkSession.builder \
    .appName("JobDataProcessingJSON") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.jars.packages", 
            "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .getOrCreate()

# Đặt log level để dễ debug
spark.sparkContext.setLogLevel("WARN")

try:
    # Đọc dữ liệu JSON từ MinIO với nhiều options
    print("📂 Đang đọc dữ liệu từ MinIO...")
    df = spark.read.option("multiLine", "true") \
                   .option("mode", "PERMISSIVE") \
                   .option("inferSchema", "true") \
                   .json("s3a://raw-json/job-postings.json")
    
    print(f"📊 Tổng số records: {df.count()}")
    print("Schema thực tế:")
    df.printSchema()
    
    print("\n🔍 Các columns có trong data:")
    for i, col_name in enumerate(df.columns, 1):
        print(f"  {i}. {col_name}")
    
    print("\n👀 Sample data (3 dòng đầu):")
    df.show(3, truncate=100, vertical=True)
    
    if df.count() > 0:
        # Kiểm tra xem có column `max_salary` không
        if "max_salary" not in df.columns:
            print("\n⚠️ WARNING: Column 'max_salary' không tồn tại!")
            print("Tìm columns tương tự:")
            salary_columns = [c for c in df.columns if 'salary' in c.lower()]
            print(f"  Các columns liên quan đến salary: {salary_columns}")
            
            # Tìm tất cả columns
            print("\nTất cả columns:")
            for col in df.columns:
                print(f"  - {col}: {df.select(col).dtypes[0][1]}")
            
            # Nếu chỉ có _corrupt_record, file JSON bị lỗi
            if "_corrupt_record" in df.columns:
                print("\n❌ File JSON bị lỗi, chỉ có _corrupt_record")
                print("Một vài dòng lỗi:")
                df.select("_corrupt_record").show(3, truncate=200)
                
                # Thoát vì không thể xử lý
                spark.stop()
                exit(1)
        
        # Tìm columns thực tế cho các field cần thiết
        column_mapping = {}
        
        # Tự động map columns dựa trên tên tương tự
        for expected_col in ["max_salary", "min_salary", "normalized_salary", "views", "location", "job_id", "formatted_work_type"]:
            # Tìm column tương tự
            matching_cols = [c for c in df.columns if expected_col.lower() in c.lower()]
            if matching_cols:
                column_mapping[expected_col] = matching_cols[0]
                print(f"  Map '{expected_col}' -> '{matching_cols[0]}'")
            else:
                column_mapping[expected_col] = None
                print(f"  ⚠️ Không tìm thấy column cho '{expected_col}'")
        
        # Tạo dataframe với columns đã map
        df_mapped = df
        
        # Đổi tên columns nếu cần
        for expected_col, actual_col in column_mapping.items():
            if actual_col and actual_col != expected_col:
                df_mapped = df_mapped.withColumnRenamed(actual_col, expected_col)
        
        print(f"\n✅ Columns sau khi map: {df_mapped.columns}")
        
        # Kiểm tra columns có tồn tại trước khi cast
        existing_columns = [col for col in ["max_salary", "min_salary", "normalized_salary", "views"] 
                          if col in df_mapped.columns]
        
        if len(existing_columns) >= 2:  # Có ít nhất 2 columns salary
            # Làm sạch dữ liệu - chỉ cast columns tồn tại
            df_clean = df_mapped
            
            for col_name in ["max_salary", "min_salary", "normalized_salary", "views"]:
                if col_name in df_mapped.columns:
                    df_clean = df_clean.withColumn(col_name, col(col_name).cast("float"))
            
            # Fill NA cho các columns tồn tại
            salary_cols = [col for col in ["max_salary", "min_salary", "normalized_salary"] 
                          if col in df_mapped.columns]
            
            if salary_cols:
                df_clean = df_clean.na.fill(0, salary_cols)
            
            if "views" in df_mapped.columns:
                df_clean = df_clean.na.fill(0, ["views"])
            
            print(f"\n✅ Data sau khi clean:")
            df_clean.show(3, truncate=True)
            
            # Tính toán - chỉ tính với columns có sẵn
            print("\n📈 Tính toán thống kê...")
            
            # 1. Lương trung bình (nếu có normalized_salary)
            if "normalized_salary" in df_clean.columns:
                avg_salary = df_clean.agg(avg("normalized_salary").alias("avg_salary")).collect()[0]["avg_salary"]
                print(f"💰 Lương trung bình toàn bộ: {avg_salary}")
            
            # 2. Group by location (nếu có location)
            if "location" in df_clean.columns:
                print("\n📍 Thống kê theo location:")
                agg_columns = []
                
                if "normalized_salary" in df_clean.columns:
                    agg_columns.append(avg("normalized_salary").alias("avg_salary"))
                
                if "job_id" in df_clean.columns:
                    agg_columns.append(count("job_id").alias("job_count"))
                else:
                    # Dùng count(*) nếu không có job_id
                    agg_columns.append(count("*").alias("job_count"))
                
                if "views" in df_clean.columns:
                    agg_columns.append(sum("views").alias("total_views"))
                
                if agg_columns:
                    agg_location = df_clean.groupBy("location").agg(*agg_columns)
                    agg_location.show(truncate=False)
            
            # 3. Group by work type (nếu có formatted_work_type)
            if "formatted_work_type" in df_clean.columns:
                print("\n💼 Thống kê theo work type:")
                agg_columns = []
                
                if "views" in df_clean.columns:
                    agg_columns.append(sum("views").alias("total_views"))
                
                if "normalized_salary" in df_clean.columns:
                    agg_columns.append(avg("normalized_salary").alias("avg_salary"))
                
                if "job_id" in df_clean.columns:
                    agg_columns.append(count("job_id").alias("job_count"))
                else:
                    agg_columns.append(count("*").alias("job_count"))
                
                if agg_columns:
                    agg_work_type = df_clean.groupBy("formatted_work_type").agg(*agg_columns)
                    agg_work_type.show(truncate=False)
                    
                    print("\n🗄️ Chuẩn bị lưu vào Cassandra...")
                    
                    # Chỉ lưu nếu Cassandra đang chạy
                    try:
                        # Lưu agg_work_type
                        agg_work_type.withColumn("id", col("formatted_work_type")) \
                                     .withColumn("processed_at", current_timestamp()) \
                                     .write \
                                     .format("org.apache.spark.sql.cassandra") \
                                     .options(table="job_stats_by_work_type", keyspace="job_data") \
                                     .mode("append") \
                                     .save()
                        
                        print("✅ Đã lưu vào Cassandra table: job_stats_by_work_type")
                    except Exception as cassandra_error:
                        print(f"⚠️ Không thể lưu vào Cassandra: {cassandra_error}")
                        print("Tạo file CSV thay thế...")
                        agg_work_type.write.csv("output/job_stats_by_work_type.csv", header=True)
            
            print("\n🎉 Xử lý hoàn tất!")
        else:
            print("\n❌ Không đủ columns salary để xử lý")
            print("Hãy kiểm tra file JSON có đúng format không")
            
    else:
        print("⚠️ Không có dữ liệu trong file JSON.")
        
except Exception as e:
    print(f"❌ Lỗi: {e}")
    import traceback
    traceback.print_exc()

finally:
    spark.stop()
    print("🔚 Đã dừng Spark session.")