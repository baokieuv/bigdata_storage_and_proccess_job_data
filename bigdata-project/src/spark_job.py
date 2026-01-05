import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round, desc, when, lower, regexp_replace, split, lit, current_timestamp
import pyspark.sql.functions as F
import matplotlib.pyplot as plt
import numpy as np
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

def setup_spark():
    """Setup SparkSession cho Windows"""
    
    # Tạo winutils giả
    hadoop_dir = "C:/tmp/hadoop_spark"
    bin_dir = os.path.join(hadoop_dir, "bin")
    os.makedirs(bin_dir, exist_ok=True)
    
    winutils_path = os.path.join(bin_dir, "winutils.exe")
    if not os.path.exists(winutils_path):
        with open(winutils_path, 'wb') as f:
            f.write(b'winutils dummy')
    
    os.environ['HADOOP_HOME'] = hadoop_dir
    os.environ['PATH'] = os.environ.get('PATH', '') + f';{bin_dir}'
    os.environ['HADOOP_TMP_DIR'] = 'C:/Windows/Temp'
    
    # Tạo SparkSession
    spark = SparkSession.builder \
        .appName("Data-Visualization-MinIO") \
        .master("local[1]") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.hadoop.tmp.dir", "C:/Windows/Temp") \
        .config("spark.ui.enabled", "false") \
        .getOrCreate()
    
    return spark

def prepare_and_clean_data(df):
    """Chuẩn bị và làm sạch dữ liệu"""
    
    print("\n🔄 CHUẨN BỊ DỮ LIỆU...")
    
    # 1. Chuyển đổi cột salary từ string sang numeric
    salary_columns = ['normalized_salary', 'min_salary', 'max_salary', 'med_salary']
    
    for col_name in salary_columns:
        if col_name in df.columns:
            # Loại bỏ ký tự không phải số và chuyển đổi
            df = df.withColumn(
                f"{col_name}_numeric",
                F.regexp_replace(F.col(col_name), "[^0-9.-]", "").cast("double")
            )
    
    # 2. Tạo experience level từ title (vì cột formatted_experience_level rỗng)
    if 'title' in df.columns:
        df = df.withColumn(
            "experience_level_derived",
            F.when(
                F.lower(F.col('title')).contains("senior") | 
                F.lower(F.col('title')).contains("sr.") |
                F.lower(F.col('title')).contains("lead") |
                F.lower(F.col('title')).contains("principal") |
                F.lower(F.col('title')).contains("director") |
                F.lower(F.col('title')).contains("manager"),
                "Senior"
            ).when(
                F.lower(F.col('title')).contains("mid") |
                F.lower(F.col('title')).contains("middle") |
                F.lower(F.col('title')).contains("experienced"),
                "Mid-level"
            ).when(
                F.lower(F.col('title')).contains("junior") |
                F.lower(F.col('title')).contains("jr.") |
                F.lower(F.col('title')).contains("entry") |
                F.lower(F.col('title')).contains("fresh"),
                "Junior"
            ).when(
                F.lower(F.col('title')).contains("intern") |
                F.lower(F.col('title')).contains("trainee"),
                "Intern"
            ).otherwise("Not specified")
        )
    
    # 3. Chuẩn hóa work type
    if 'formatted_work_type' in df.columns:
        df = df.withColumn(
            "work_type_clean",
            F.when(
                F.lower(F.col('formatted_work_type')).contains("full"), "Full-time"
            ).when(
                F.lower(F.col('formatted_work_type')).contains("part"), "Part-time"
            ).when(
                F.lower(F.col('formatted_work_type')).contains("contract"), "Contract"
            ).when(
                F.lower(F.col('formatted_work_type')).contains("intern"), "Internship"
            ).when(
                F.lower(F.col('formatted_work_type')).contains("remote"), "Remote"
            ).when(
                F.lower(F.col('formatted_work_type')).contains("hybrid"), "Hybrid"
            ).otherwise(F.col('formatted_work_type'))
        )
    
    # 4. Chuẩn hóa location (lấy thành phố)
    if 'location' in df.columns:
        df = df.withColumn(
            "city",
            F.when(
                F.col('location').contains(","),
                F.trim(F.split(F.col('location'), ",")[0])
            ).otherwise(F.col('location'))
        )
    
    # 5. Chuẩn hóa numeric columns
    if 'applies' in df.columns:
        df = df.withColumn(
            "applies_numeric",
            F.regexp_replace(F.col('applies'), "[^0-9]", "").cast("integer")
        )
    
    if 'views' in df.columns:
        df = df.withColumn(
            "views_numeric",
            F.regexp_replace(F.col('views'), "[^0-9]", "").cast("integer")
        )
    
    print("✅ Đã chuẩn bị dữ liệu xong!")
    return df

def create_chart_6_salary_by_experience(df):
    """Biểu đồ 6: Lương trung bình theo Experience Level"""
    
    print("\n" + "=" * 50)
    print("📊 BIỂU ĐỒ 6 – Lương trung bình theo Experience Level")
    print("=" * 50)
    
    # Sử dụng cột experience_level_derived đã tạo
    if 'experience_level_derived' in df.columns and 'normalized_salary_numeric' in df.columns:
        # Lọc bỏ các giá trị "Not specified"
        filtered_df = df.filter(
            (F.col('experience_level_derived') != 'Not specified') &
            (F.col('normalized_salary_numeric').isNotNull())
        )
        
        if filtered_df.count() > 0:
            salary_by_exp = filtered_df.groupBy('experience_level_derived') \
                .agg(
                    F.round(F.avg('normalized_salary_numeric'), 2).alias("avg_salary"),
                    F.count("*").alias("job_count")
                ) \
                .orderBy('experience_level_derived')
            
            print("💰 LƯƠNG TRUNG BÌNH THEO EXPERIENCE LEVEL:")
            salary_by_exp.show(truncate=False)
            
            # Chuẩn bị dữ liệu cho biểu đồ
            data = salary_by_exp.collect()
            exp_levels = [row['experience_level_derived'] for row in data]
            avg_salaries = [row['avg_salary'] for row in data]
            job_counts = [row['job_count'] for row in data]
            
            # Tạo biểu đồ
            plt.figure(figsize=(14, 6))
            
            plt.subplot(1, 2, 1)
            bars1 = plt.bar(exp_levels, avg_salaries, color=['#FF6B6B', '#4ECDC4', '#45B7D1', '#FFD166', '#EF476F'])
            plt.xlabel('Experience Level')
            plt.ylabel('Average Salary ($)')
            plt.title('Biểu đồ 6: Lương trung bình theo Experience Level')
            plt.xticks(rotation=45)
            
            for bar, salary in zip(bars1, avg_salaries):
                plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5, 
                        f'${salary:,.0f}', ha='center', va='bottom', fontsize=9)
            
            plt.subplot(1, 2, 2)
            bars2 = plt.bar(exp_levels, job_counts, color=['#06D6A0', '#118AB2', '#073B4C', '#FF9E00', '#7209B7'])
            plt.xlabel('Experience Level')
            plt.ylabel('Number of Jobs')
            plt.title('Số lượng Job theo Experience Level')
            plt.xticks(rotation=45)
            
            for bar, count in zip(bars2, job_counts):
                plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5, 
                        f'{count}', ha='center', va='bottom', fontsize=9)
            
            plt.tight_layout()
            plt.savefig('experience_level_salary.png', dpi=300, bbox_inches='tight')
            print("✅ Đã lưu biểu đồ: experience_level_salary.png")
            plt.show()
            return True
        else:
            print("⚠️  Không có đủ dữ liệu để tạo biểu đồ 6")
            return False
    else:
        print("⚠️  Thiếu cột cần thiết cho biểu đồ 6")
        return False

def create_chart_7_jobs_by_work_type(df):
    """Biểu đồ 7: Số job theo Work Type"""
    
    print("\n" + "=" * 50)
    print("📊 BIỂU ĐỒ 7 – Số job theo Work Type")
    print("=" * 50)
    
    if 'work_type_clean' in df.columns:
        work_stats = df.groupBy('work_type_clean') \
            .agg(F.count("*").alias("job_count")) \
            .orderBy(F.desc("job_count"))
        
        print("👥 SỐ JOB THEO WORK TYPE:")
        work_stats.show(truncate=False)
        
        data = work_stats.collect()
        work_types = [row['work_type_clean'] for row in data]
        counts = [row['job_count'] for row in data]
        
        # Tính phần trăm
        total = sum(counts)
        percentages = [(c/total)*100 for c in counts]
        
        plt.figure(figsize=(14, 6))
        
        plt.subplot(1, 2, 1)
        colors = ['#FF9999', '#66B2FF', '#99FF99', '#FFCC99', '#C77DFF', '#FF6B6B']
        plt.pie(counts, labels=work_types, autopct='%1.1f%%', colors=colors[:len(work_types)], 
                startangle=90, textprops={'fontsize': 10})
        plt.title('Phân bố Job theo Work Type (Pie Chart)')
        
        plt.subplot(1, 2, 2)
        bars = plt.bar(work_types, counts, color=colors[:len(work_types)])
        plt.xlabel('Work Type')
        plt.ylabel('Number of Jobs')
        plt.title('Phân bố Job theo Work Type (Bar Chart)')
        plt.xticks(rotation=45)
        
        for bar, count, percent in zip(bars, counts, percentages):
            plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5, 
                    f'{count} ({percent:.1f}%)', ha='center', va='bottom', fontsize=9)
        
        plt.tight_layout()
        plt.savefig('work_type_distribution.png', dpi=300, bbox_inches='tight')
        print("✅ Đã lưu biểu đồ: work_type_distribution.png")
        plt.show()
        return True
    else:
        print("⚠️  Thiếu cột 'work_type_clean'")
        return False

def create_chart_8_top_locations(df):
    """Biểu đồ 8: Top Location có nhiều job nhất"""
    
    print("\n" + "=" * 50)
    print("📊 BIỂU ĐỒ 8 – Top Location có nhiều job nhất")
    print("=" * 50)
    
    if 'city' in df.columns:
        location_stats = df.groupBy('city') \
            .agg(F.count("*").alias("job_count")) \
            .orderBy(F.desc("job_count")) \
            .limit(10)
        
        print("📍 TOP 10 LOCATION CÓ NHIỀU JOB NHẤT:")
        location_stats.show(truncate=False)
        
        data = location_stats.collect()
        locations = [row['city'] for row in data]
        counts = [row['job_count'] for row in data]
        
        plt.figure(figsize=(14, 8))
        colors = plt.cm.viridis(np.linspace(0.2, 0.8, len(locations)))
        
        bars = plt.barh(locations, counts, color=colors)
        plt.xlabel('Number of Jobs')
        plt.ylabel('Location')
        plt.title('Biểu đồ 8: Top 10 Location có nhiều Job nhất')
        
        for bar, count in zip(bars, counts):
            plt.text(bar.get_width() + 0.3, bar.get_y() + bar.get_height()/2, 
                    f' {count} jobs', va='center', fontsize=10, fontweight='bold')
        
        plt.tight_layout()
        plt.savefig('top_locations.png', dpi=300, bbox_inches='tight')
        print("✅ Đã lưu biểu đồ: top_locations.png")
        plt.show()
        return True
    else:
        print("⚠️  Thiếu cột 'city'")
        return False

def create_cassandra_schema():
    """Tạo schema trong Cassandra"""
    
    try:
        print("\n🔧 ĐANG TẠO CASSANDRA SCHEMA...")
        
        auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
        cluster = Cluster(['localhost'], port=9042, auth_provider=auth_provider)
        session = cluster.connect()
        
        # Tạo keyspace
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS bigdata_project 
            WITH replication = {
                'class': 'SimpleStrategy', 
                'replication_factor': 1
            }
        """)
        
        print("✅ Đã tạo keyspace: bigdata_project")
        
        # Chuyển sang keyspace mới
        session.set_keyspace('bigdata_project')
        
        # Tạo tables
        session.execute("""
            CREATE TABLE IF NOT EXISTS processed_jobs (
                job_id TEXT PRIMARY KEY,
                title TEXT,
                company_name TEXT,
                location TEXT,
                city TEXT,
                experience_level TEXT,
                work_type TEXT,
                normalized_salary DOUBLE,
                min_salary DOUBLE,
                max_salary DOUBLE,
                applies INT,
                views INT,
                remote_allowed TEXT,
                sponsored TEXT,
                processed_time TIMESTAMP
            )
        """)
        
        session.execute("""
            CREATE TABLE IF NOT EXISTS salary_by_experience_stats (
                experience_level TEXT PRIMARY KEY,
                avg_salary DOUBLE,
                job_count INT,
                last_updated TIMESTAMP
            )
        """)
        
        session.execute("""
            CREATE TABLE IF NOT EXISTS jobs_by_work_type_stats (
                work_type TEXT PRIMARY KEY,
                job_count INT,
                percentage DOUBLE,
                last_updated TIMESTAMP
            )
        """)
        
        session.execute("""
            CREATE TABLE IF NOT EXISTS top_locations_stats (
                city TEXT PRIMARY KEY,
                job_count INT,
                rank INT,
                last_updated TIMESTAMP
            )
        """)
        
        session.execute("""
            CREATE TABLE IF NOT EXISTS charts_metadata (
                chart_id UUID PRIMARY KEY,
                chart_name TEXT,
                file_path TEXT,
                created_date DATE,
                record_count INT
            )
        """)
        
        print("✅ Đã tạo 5 tables trong Cassandra")
        
        # Kiểm tra tables
        rows = session.execute("""
            SELECT table_name 
            FROM system_schema.tables 
            WHERE keyspace_name = 'bigdata_project'
        """)
        
        tables = [row.table_name for row in rows]
        print(f"📊 Tables đã tạo: {tables}")
        
        cluster.shutdown()
        return True
        
    except Exception as e:
        print(f"❌ Lỗi khi tạo Cassandra schema: {e}")
        return False

def save_data_to_cassandra(spark, df):
    """Lưu dữ liệu vào Cassandra"""
    
    try:
        print("\n💾 ĐANG LƯU DỮ LIỆU VÀO CASSANDRA...")
        
        # Tải JAR files từ Maven Central (hoặc dùng JAR local)
        cassandra_connector_jar = "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0"
        
        # Tạo SparkSession mới với Cassandra connector
        spark_cass = SparkSession.builder \
            .appName("Cassandra-Writer") \
            .master("local[1]") \
            .config("spark.jars.packages", 
                   "org.apache.hadoop:hadoop-aws:3.3.4," +
                   "com.amazonaws:aws-java-sdk-bundle:1.12.262," +
                   f"{cassandra_connector_jar}") \
            .config("spark.cassandra.connection.host", "localhost") \
            .config("spark.cassandra.connection.port", "9042") \
            .config("spark.cassandra.auth.username", "cassandra") \
            .config("spark.cassandra.auth.password", "cassandra") \
            .config("spark.cassandra.connection.keep_alive_ms", "60000") \
            .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
            .getOrCreate()
        
        print("✅ Spark Cassandra connector đã tải!")
        
        # 1. Lưu dữ liệu jobs đã xử lý
        print("📤 Đang lưu dữ liệu jobs...")
        
        jobs_df = df.select(
            'job_id', 'title', 'company_name', 'location', 'city',
            'experience_level_derived', 'work_type_clean',
            'normalized_salary_numeric', 'min_salary_numeric', 'max_salary_numeric',
            'applies_numeric', 'views_numeric',
            'remote_allowed', 'sponsored'
        ).withColumnRenamed('experience_level_derived', 'experience_level') \
         .withColumnRenamed('work_type_clean', 'work_type') \
         .withColumnRenamed('normalized_salary_numeric', 'normalized_salary') \
         .withColumnRenamed('min_salary_numeric', 'min_salary') \
         .withColumnRenamed('max_salary_numeric', 'max_salary') \
         .withColumnRenamed('applies_numeric', 'applies') \
         .withColumnRenamed('views_numeric', 'views') \
         .withColumn('processed_time', F.current_timestamp())
        
        # Lọc bỏ các dòng không có job_id
        jobs_df = jobs_df.filter(F.col('job_id').isNotNull())
        
        # Lưu vào Cassandra
        jobs_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="processed_jobs", keyspace="bigdata_project") \
            .save()
        
        print(f"✅ Đã lưu {jobs_df.count()} jobs vào Cassandra")
        
        # ... (phần còn lại giữ nguyên)
        
        spark_cass.stop()
        return True
        
    except Exception as e:
        print(f"❌ Lỗi khi lưu vào Cassandra: {e}")
        import traceback
        traceback.print_exc()
        return False
def display_summary_statistics(df):
    """Hiển thị thống kê tổng quan"""
    
    print("\n" + "=" * 60)
    print("📈 THỐNG KÊ TỔNG QUAN")
    print("=" * 60)
    
    total_jobs = df.count()
    print(f"📊 Tổng số Job: {total_jobs}")
    
    # Thống kê salary
    if 'normalized_salary_numeric' in df.columns:
        salary_stats = df.select(
            F.round(F.avg('normalized_salary_numeric'), 2).alias("avg_salary"),
            F.min('normalized_salary_numeric').alias("min_salary"),
            F.max('normalized_salary_numeric').alias("max_salary"),
            F.count('normalized_salary_numeric').alias("jobs_with_salary")
        ).collect()[0]
        
        print(f"\n💰 THỐNG KÊ LƯƠNG:")
        print(f"  • Lương trung bình: ${salary_stats['avg_salary']:,.2f}")
        print(f"  • Lương thấp nhất: ${salary_stats['min_salary']:,.2f}")
        print(f"  • Lương cao nhất: ${salary_stats['max_salary']:,.2f}")
        print(f"  • Có lương: {salary_stats['jobs_with_salary']}/{total_jobs} jobs")
    
    # Thống kê work type
    if 'work_type_clean' in df.columns:
        work_type_counts = df.groupBy('work_type_clean').count().orderBy(F.desc('count'))
        print(f"\n👥 PHÂN BỐ WORK TYPE:")
        for row in work_type_counts.collect():
            percentage = (row['count'] / total_jobs) * 100
            print(f"  • {row['work_type_clean']}: {row['count']} jobs ({percentage:.1f}%)")
    
    # Thống kê location
    if 'city' in df.columns:
        top_locations = df.groupBy('city').count().orderBy(F.desc('count')).limit(5)
        print(f"\n📍 TOP 5 LOCATIONS:")
        for row in top_locations.collect():
            print(f"  • {row['city']}: {row['count']} jobs")
    
    # Thống kê experience level
    if 'experience_level_derived' in df.columns:
        exp_stats = df.groupBy('experience_level_derived').count().orderBy(F.desc('count'))
        print(f"\n🎯 PHÂN BỐ EXPERIENCE LEVEL:")
        for row in exp_stats.collect():
            percentage = (row['count'] / total_jobs) * 100
            print(f"  • {row['experience_level_derived']}: {row['count']} jobs ({percentage:.1f}%)")
import socket
import time

def check_port_forward():
    """Kiểm tra port-forward có đang chạy không"""
    
    print("\n🔍 KIỂM TRA PORT-FORWARD...")
    
    max_retries = 5
    for attempt in range(max_retries):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        
        try:
            result = sock.connect_ex(('localhost', 9042))
            sock.close()
            
            if result == 0:
                print("✅ Port-forward đang chạy trên localhost:9042")
                return True
            else:
                print(f"   ❌ Port-forward không chạy (thử {attempt + 1}/{max_retries})")
                
        except Exception as e:
            print(f"   ❌ Lỗi kiểm tra port: {e}")
            sock.close()
        
        if attempt < max_retries - 1:
            print(f"   ⏳ Chờ 3 giây trước khi thử lại...")
            time.sleep(3)
    
    print("\n❌ KHÔNG TÌM THẤY PORT-FORWARD!")
    print("\n💡 HÃY MỞ TERMINAL MỚI VÀ CHẠY:")
    print("   kubectl port-forward pod/cassandra-0 9042:9042")
    print("\n   Giữ terminal đó mở, sau đó chạy lại script này")
    return False

def connect_to_cassandra():
    """Kết nối Cassandra với kiểm tra port-forward"""
    
    # Kiểm tra port-forward trước
    if not check_port_forward():
        return None, None
    
    print("\n🔗 ĐANG KẾT NỐI CASSANDRA...")
    
    try:
        from cassandra.cluster import Cluster
        from cassandra.auth import PlainTextAuthProvider
        
        auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
        
        # Dùng timeout ngắn hơn để nhanh fail
        cluster = Cluster(
            ['localhost'],
            port=9042,
            auth_provider=auth_provider,
            connect_timeout=10
        )
        
        session = cluster.connect()
        
        # Test nhanh
        row = session.execute("SELECT release_version FROM system.local", timeout=5).one()
        print(f"✅ Kết nối thành công! Cassandra version: {row.release_version}")
        
        return cluster, session
        
    except Exception as e:
        print(f"❌ Lỗi kết nối: {e}")
        
        # Diagnostic
        if "timed out" in str(e).lower():
            print("💡 Có thể Cassandra pod chưa sẵn sàng hoặc đang restart")
            print("   Kiểm tra: kubectl get pods -l app=cassandra")
        
        return None, None
def main():
    """Hàm chính"""
    
    spark = None
    try:
        print("=" * 70)
        print("🚀 BẮT ĐẦU XỬ LÝ DỮ LIỆU TỪ MINIO")
        print("=" * 70)
        
        # 1. Setup Spark
        spark = setup_spark()
        
        # 2. Đọc dữ liệu từ MinIO
        print("\n📥 ĐANG ĐỌC DỮ LIỆU TỪ MINIO...")
        df = spark.read \
            .option("multiline", "true") \
            .json("s3a://test-bucket/postings*.json")
        
        original_count = df.count()
        print(f"✅ Đã đọc {original_count} bản ghi từ {len(df.inputFiles())} file")
        
        # 3. Chuẩn bị và làm sạch dữ liệu
        df = prepare_and_clean_data(df)
        
        # 4. Tạo các biểu đồ
        print("\n🎨 ĐANG TẠO BIỂU ĐỒ...")
        
        chart6_success = create_chart_6_salary_by_experience(df)
        chart7_success = create_chart_7_jobs_by_work_type(df)
        chart8_success = create_chart_8_top_locations(df)
        
        # 5. Hiển thị thống kê tổng quan
        display_summary_statistics(df)
        
        # 6. Tích hợp với Cassandra
        print("\n" + "=" * 70)
        print("🗄️  TÍCH HỢP VỚI CASSANDRA")
        print("=" * 70)
        
         # Kết nối Cassandra
        cluster, session = connect_to_cassandra()
        
        if cluster and session:
            try:
                # Tạo schema
                schema_created = create_cassandra_schema(session)
                
                if schema_created:
                    # Lưu dữ liệu
                    save_success = save_data_to_cassandra(df, session)
                    
                    if save_success:
                        print("\n🎉 ĐÃ LƯU DỮ LIỆU VÀO CASSANDRA THÀNH CÔNG!")
                        cassandra_success = True
                    else:
                        print("\n⚠️  Lỗi khi lưu dữ liệu vào Cassandra")
                        cassandra_success = False
                else:
                    print("\n⚠️  Lỗi khi tạo schema Cassandra")
                    cassandra_success = False
                    
            except Exception as e:
                print(f"❌ Lỗi khi làm việc với Cassandra: {e}")
                cassandra_success = False
        else:
            print("⚠️  Không thể kết nối Cassandra")
            cassandra_success = False
        
        # 7. Tóm tắt kết quả
        print("\n" + "=" * 70)
        print("🎯 TÓM TẮT KẾT QUẢ")
        print("=" * 70)
        
        print(f"""
    ✅ ĐÃ HOÀN THÀNH XỬ LÝ DỮ LIỆU!
    
    📊 Dữ liệu đã xử lý: {original_count} bản ghi
    🎨 Biểu đồ đã tạo:
        • Biểu đồ 6: {'✅' if chart6_success else '❌'} Lương theo Experience Level
        • Biểu đồ 7: {'✅' if chart7_success else '❌'} Job theo Work Type  
        • Biểu đồ 8: {'✅' if chart8_success else '❌'} Top Locations
    
    📁 Files đã lưu:
        • experience_level_salary.png
        • work_type_distribution.png  
        • top_locations.png
    
    🗄️  Cassandra: {'✅ Đã lưu' if 'save_success' in locals() and save_success else '⚠️ Chưa lưu'}
    
    🎉 HOÀN TẤT DỰ ÁN BIG DATA!
        """)
        
        return 0
        
    except Exception as e:
        print(f"\n❌ CÓ LỖI XẢY RA: {e}")
        import traceback
        traceback.print_exc()
        return 1
        
    finally:
        if spark:
            spark.stop()
            print("\n🔴 Đã đóng SparkSession")

if __name__ == "__main__":
    sys.exit(main())