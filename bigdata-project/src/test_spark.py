import os
import sys
import tempfile
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round, desc, when
import pyspark.sql.functions as F  # Thêm dòng này
import matplotlib.pyplot as plt
import numpy as np

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

def read_data_from_minio(spark, bucket="test-bucket"):
    """Đọc dữ liệu từ MinIO"""
    
    print("📥 Đang đọc dữ liệu từ MinIO...")
    
    # Đọc tất cả file JSON
    df = spark.read \
        .option("multiline", "true") \
        .option("inferSchema", "true") \
        .json(f"s3a://{bucket}/postings*.json")
    
    print(f"✅ Đã đọc {df.count()} bản ghi từ {len(df.inputFiles())} file")
    print(f"📋 Số cột: {len(df.columns)}")
    
    # Hiển thị schema để kiểm tra
    print("\n📄 SCHEMA DỮ LIỆU:")
    df.printSchema()
    
    return df

def analyze_and_visualize(df):
    """Phân tích dữ liệu và tạo visualizations"""
    
    print("\n" + "=" * 60)
    print("📊 BẮT ĐẦU PHÂN TÍCH DỮ LIỆU")
    print("=" * 60)
    
    # Kiểm tra các cột có sẵn
    print("\n🔍 CÁC CỘT CÓ SẴN:")
    for i, column in enumerate(df.columns, 1):
        print(f"  {i:2d}. {column}")
    
    # Tạo mapping giữa tên cột thực tế và tên cột yêu cầu
    column_mapping = {}
    
    # Tìm các cột tương ứng
    for col_name in df.columns:
        col_lower = col_name.lower()
        
        if 'experience' in col_lower and 'level' in col_lower:
            column_mapping['formatted_experience_level'] = col_name
        elif 'salary' in col_lower:
            if 'normalized' in col_lower or 'salary' == col_lower:
                column_mapping['normalized_salary'] = col_name
        elif 'work' in col_lower and 'type' in col_lower:
            column_mapping['formatted_work_type'] = col_name
        elif 'location' in col_lower:
            column_mapping['location'] = col_name
    
    print(f"\n🔍 MAPPING CỘT TÌM ĐƯỢC:")
    for key, value in column_mapping.items():
        print(f"  • {key} -> {value}")
    
    # BIỂU ĐỒ 6: Lương trung bình theo Experience Level
    print("\n" + "-" * 50)
    print("📊 BIỂU ĐỒ 6 – Lương trung bình theo Experience Level")
    print("-" * 50)
    
    if 'formatted_experience_level' in column_mapping and 'normalized_salary' in column_mapping:
        exp_col = column_mapping['formatted_experience_level']
        salary_col = column_mapping['normalized_salary']
        
        # Tính lương trung bình theo experience level
        salary_by_exp = df.groupBy(exp_col) \
            .agg(
                round(avg(salary_col), 2).alias("avg_salary"),
                F.count("*").alias("job_count")
            ) \
            .orderBy(exp_col)
        
        print("💰 LƯƠNG TRUNG BÌNH THEO EXPERIENCE LEVEL:")
        salary_by_exp.show(truncate=False)
        
        # Chuẩn bị dữ liệu cho biểu đồ
        exp_levels = [row[exp_col] for row in salary_by_exp.collect()]
        avg_salaries = [row["avg_salary"] for row in salary_by_exp.collect()]
        job_counts = [row["job_count"] for row in salary_by_exp.collect()]
        
        # Tạo biểu đồ 1
        plt.figure(figsize=(12, 6))
        
        # Subplot 1: Bar chart lương trung bình
        plt.subplot(1, 2, 1)
        bars = plt.bar(exp_levels, avg_salaries, color=['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4'])
        plt.xlabel('Experience Level')
        plt.ylabel('Average Salary')
        plt.title('Biểu đồ 6: Lương trung bình theo Experience Level')
        plt.xticks(rotation=45)
        
        # Thêm giá trị lên các cột
        for bar, salary in zip(bars, avg_salaries):
            plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5, 
                    f'{salary:.2f}', ha='center', va='bottom', fontsize=9)
        
        # Subplot 2: Bar chart số lượng job
        plt.subplot(1, 2, 2)
        bars2 = plt.bar(exp_levels, job_counts, color=['#FFD166', '#06D6A0', '#118AB2', '#EF476F'])
        plt.xlabel('Experience Level')
        plt.ylabel('Number of Jobs')
        plt.title('Số lượng Job theo Experience Level')
        plt.xticks(rotation=45)
        
        # Thêm giá trị lên các cột
        for bar, count in zip(bars2, job_counts):
            plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5, 
                    f'{count}', ha='center', va='bottom', fontsize=9)
        
        plt.tight_layout()
        plt.savefig('experience_level_salary.png', dpi=300, bbox_inches='tight')
        print("✅ Đã lưu biểu đồ: experience_level_salary.png")
        
    else:
        print("⚠️  Không tìm thấy cột 'formatted_experience_level' hoặc 'normalized_salary'")
    
    # BIỂU ĐỒ 7: Số job theo Work Type
    print("\n" + "-" * 50)
    print("📊 BIỂU ĐỒ 7 – Số job theo Work Type")
    print("-" * 50)
    
    if 'formatted_work_type' in column_mapping:
        work_type_col = column_mapping['formatted_work_type']
        
        # Đếm số job theo work type
        jobs_by_work_type = df.groupBy(work_type_col) \
            .agg(F.count("*").alias("job_count")) \
            .orderBy(desc("job_count"))
        
        print("👥 SỐ JOB THEO WORK TYPE:")
        jobs_by_work_type.show(truncate=False)
        
        # Chuẩn bị dữ liệu
        work_types = [row[work_type_col] for row in jobs_by_work_type.collect()]
        work_type_counts = [row["job_count"] for row in jobs_by_work_type.collect()]
        
        # Tính phần trăm
        total_jobs = sum(work_type_counts)
        percentages = [(count/total_jobs)*100 for count in work_type_counts]
        
        # Tạo biểu đồ 2
        plt.figure(figsize=(12, 6))
        
        # Subplot 1: Pie chart
        plt.subplot(1, 2, 1)
        colors = ['#FF9999', '#66B2FF', '#99FF99', '#FFCC99', '#FF99CC']
        wedges, texts, autotexts = plt.pie(
            work_type_counts, 
            labels=work_types, 
            colors=colors[:len(work_types)],
            autopct='%1.1f%%',
            startangle=90
        )
        plt.title('Biểu đồ 7: Phân bố Job theo Work Type (Pie Chart)')
        
        # Subplot 2: Bar chart
        plt.subplot(1, 2, 2)
        bars = plt.bar(work_types, work_type_counts, color=colors[:len(work_types)])
        plt.xlabel('Work Type')
        plt.ylabel('Number of Jobs')
        plt.title('Phân bố Job theo Work Type (Bar Chart)')
        plt.xticks(rotation=45)
        
        # Thêm giá trị và phần trăm lên các cột
        for bar, count, percent in zip(bars, work_type_counts, percentages):
            plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5, 
                    f'{count}\n({percent:.1f}%)', ha='center', va='bottom', fontsize=9)
        
        plt.tight_layout()
        plt.savefig('work_type_distribution.png', dpi=300, bbox_inches='tight')
        print("✅ Đã lưu biểu đồ: work_type_distribution.png")
        
    else:
        print("⚠️  Không tìm thấy cột 'formatted_work_type'")
    
    # BIỂU ĐỒ 8: Top Location có nhiều job nhất
    print("\n" + "-" * 50)
    print("📊 BIỂU ĐỒ 8 – Top Location có nhiều job nhất")
    print("-" * 50)
    
    if 'location' in column_mapping:
        location_col = column_mapping['location']
        
        # Đếm số job theo location và lấy top 10
        jobs_by_location = df.groupBy(location_col) \
            .agg(F.count("*").alias("job_count")) \
            .orderBy(desc("job_count")) \
            .limit(10)
        
        print("📍 TOP LOCATION CÓ NHIỀU JOB NHẤT:")
        jobs_by_location.show(truncate=False)
        
        # Chuẩn bị dữ liệu
        locations = [row[location_col] for row in jobs_by_location.collect()]
        location_counts = [row["job_count"] for row in jobs_by_location.collect()]
        
        # Tạo biểu đồ 3
        plt.figure(figsize=(14, 8))
        
        # Tạo gradient màu
        colors = plt.cm.viridis(np.linspace(0.2, 0.8, len(locations)))
        
        # Horizontal bar chart
        bars = plt.barh(locations, location_counts, color=colors)
        plt.xlabel('Number of Jobs')
        plt.ylabel('Location')
        plt.title('Biểu đồ 8: Top 10 Location có nhiều Job nhất')
        
        # Thêm giá trị lên các cột
        for bar, count in zip(bars, location_counts):
            plt.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2, 
                    f' {count}', va='center', fontsize=10)
        
        plt.tight_layout()
        plt.savefig('top_locations.png', dpi=300, bbox_inches='tight')
        print("✅ Đã lưu biểu đồ: top_locations.png")
        
    else:
        print("⚠️  Không tìm thấy cột 'location'")
    
    # Tổng hợp thống kê
    print("\n" + "=" * 60)
    print("📈 TỔNG HỢP THỐNG KÊ")
    print("=" * 60)
    
    total_jobs = df.count()
    print(f"📊 Tổng số Job: {total_jobs}")
    
    # Nếu có salary column, thêm thống kê
    if 'normalized_salary' in column_mapping:
        salary_col = column_mapping['normalized_salary']
        salary_stats = df.select(
            round(avg(salary_col), 2).alias("avg_salary"),
            round(avg(when(col(salary_col).isNotNull(), col(salary_col)).otherwise(0)), 2).alias("avg_salary_non_null")
        ).collect()[0]
        
        print(f"💰 Lương trung bình: {salary_stats['avg_salary']}")
        print(f"💰 Lương trung bình (non-null): {salary_stats['avg_salary_non_null']}")
    
    # Hiển thị tất cả biểu đồ
    plt.show()
    
    return column_mapping

def save_processed_data(spark, df, column_mapping):
    """Chỉ hiển thị kết quả, không lưu file (tránh winutils error)"""
    
    print("\n💾 KẾT QUẢ XỬ LÝ (KHÔNG LƯU FILE ĐỂ TRÁNH WINUTILS ERROR):")
    
    # 1. Thống kê lương theo experience level
    if 'formatted_experience_level' in column_mapping and 'normalized_salary' in column_mapping:
        exp_col = column_mapping['formatted_experience_level']
        salary_col = column_mapping['normalized_salary']
        
        salary_by_exp = df.groupBy(exp_col) \
            .agg(
                round(avg(salary_col), 2).alias("avg_salary"),
                F.count("*").alias("job_count")
            ) \
            .orderBy(exp_col)
        
        print("\n📊 LƯƠNG THEO EXPERIENCE LEVEL:")
        salary_by_exp.show(truncate=False)
    
    # 2. Thống kê job theo work type
    if 'formatted_work_type' in column_mapping:
        work_type_col = column_mapping['formatted_work_type']
        
        work_type_stats = df.groupBy(work_type_col) \
            .agg(F.count("*").alias("job_count")) \
            .orderBy(F.desc("job_count"))
        
        print("\n📊 JOB THEO WORK TYPE:")
        work_type_stats.show(truncate=False)
    
    # 3. Thống kê job theo location
    if 'location' in column_mapping:
        location_col = column_mapping['location']
        
        location_stats = df.groupBy(location_col) \
            .agg(F.count("*").alias("job_count")) \
            .orderBy(F.desc("job_count")) \
            .limit(10)
        
        print("\n📍 TOP 10 LOCATION CÓ NHIỀU JOB NHẤT:")
        location_stats.show(truncate=False)
    
    print("\n✅ Kết quả đã được xử lý và hiển thị thành công!")
    print("📈 Biểu đồ đã được lưu thành file PNG")
    
    return True  # Thay vì lưu file
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

def test_cassandra_connection():
    try:
        # Thay đổi thông tin kết nối theo config của bạn
        auth_provider = PlainTextAuthProvider(
            username='cassandra', 
            password='cassandra'
        )
        
        cluster = Cluster(
            ['localhost'],  # Hoặc IP Cassandra service
            port=9042,
            auth_provider=auth_provider
        )
        
        session = cluster.connect()
        
        print("✅ Kết nối Cassandra thành công!")
        
        # Kiểm tra keyspace
        rows = session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
        keyspaces = [row.keyspace_name for row in rows]
        
        print(f"📁 Keyspaces có sẵn: {keyspaces}")
        
        # Kiểm tra nếu keyspace của bạn tồn tại
        target_keyspace = "bigdata_project"
        if target_keyspace in keyspaces:
            print(f"✅ Keyspace '{target_keyspace}' tồn tại")
            
            # Kiểm tra tables
            session.set_keyspace(target_keyspace)
            rows = session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s", [target_keyspace])
            tables = [row.table_name for row in rows]
            print(f"📊 Tables trong keyspace: {tables}")
        else:
            print(f"⚠️ Keyspace '{target_keyspace}' không tồn tại")
            print("Tạo keyspace với:")
            print(f"CREATE KEYSPACE IF NOT EXISTS {target_keyspace} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};")
        
        cluster.shutdown()
        return True
    
    except Exception as e:
        print(f"❌ Lỗi kết nối Cassandra: {e}")
        return False
def main():
    """Hàm chính"""
    
    spark = None
    try:
        print("=" * 60)
        print("🚀 BẮT ĐẦU XỬ LÝ DỮ LIỆU TỪ MINIO")
        print("=" * 60)
        
        # 1. Setup Spark
        spark = setup_spark()
        
        # 2. Đọc dữ liệu từ MinIO
        df = read_data_from_minio(spark)
        
        # 3. Phân tích và tạo biểu đồ
        column_mapping = analyze_and_visualize(df)
        
        # 4. Lưu dữ liệu đã xử lý
        save_processed_data(spark, df, column_mapping)
        
        print("\n" + "=" * 60)
        print("🎉 HOÀN TẤT XỬ LÝ VÀ VISUALIZATION!")
        print("=" * 60)
        
        return 0
        
    except Exception as e:
        print(f"\n❌ CÓ LỖI XẢY RA: {e}")
        import traceback
        traceback.print_exc()
        return 1
        
    finally:
        # Đóng SparkSession
        if spark:
            spark.stop()
            print("\n🔴 Đã đóng SparkSession")

if __name__ == "__main__":
    sys.exit(main())