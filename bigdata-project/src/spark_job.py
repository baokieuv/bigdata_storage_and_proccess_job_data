from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, max, min, current_timestamp, lit
import sys

spark = SparkSession.builder \
    .appName("JobAnalyticsBatch") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio.default.svc.cluster.local:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.cassandra.connection.host", "cassandra.default.svc.cluster.local") \
    .config("spark.cores.max", "8") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.memory", "512m") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/*") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/*") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

BUCKET_URL = "s3a://job-raw-data/"
STATE_FILE = "batch_state.txt" 
BATCH_SIZE = 10

def get_hadoop_fs(spark_session):
    sc = spark_session.sparkContext
    conf = sc._jsc.hadoopConfiguration()
    uri = sc._gateway.jvm.java.net.URI(BUCKET_URL)
    fs = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)
    return fs, sc._gateway.jvm.org.apache.hadoop.fs.Path

def get_files_to_process():
    fs, Path = get_hadoop_fs(spark)
    try:
        file_statuses = fs.listStatus(Path(BUCKET_URL))
    except Exception as e:
        print(f"Lỗi đọc Bucket: {e}")
        return [], None

    all_files = []
    for status in file_statuses:
        fname = status.getPath().getName()
        if fname.startswith("jobs_") and fname.endswith(".json"):
            all_files.append(fname)
    all_files.sort()

    if not all_files: return [], None

    last_processed_file = ""
    state_path = Path(BUCKET_URL + STATE_FILE)
    if fs.exists(state_path):
        stream = fs.open(state_path)
        reader = spark.sparkContext._gateway.jvm.java.io.BufferedReader(
            spark.sparkContext._gateway.jvm.java.io.InputStreamReader(stream)
        )
        line = reader.readLine()
        if line: last_processed_file = line.strip()
        reader.close()
        print(f"--> Last state: {last_processed_file}")

    start_index = 0
    if last_processed_file in all_files:
        start_index = all_files.index(last_processed_file) + 1
    elif last_processed_file and last_processed_file < all_files[-1]:
        for i, f in enumerate(all_files):
            if f > last_processed_file:
                start_index = i
                break
    
    batch_files = all_files[start_index : start_index + BATCH_SIZE]
    full_paths = [BUCKET_URL + f for f in batch_files]
    return full_paths, (batch_files[-1] if batch_files else last_processed_file)

def save_state(last_file):
    if not last_file: return
    fs, Path = get_hadoop_fs(spark)
    state_path = Path(BUCKET_URL + STATE_FILE)
    out_stream = fs.create(state_path, True) 
    out_stream.write(last_file.encode('utf-8'))
    out_stream.close()
    print(f"--> Saved state: {last_file}")
    
    
try:
    files_to_read, new_last_file = get_files_to_process()
    
    if len(files_to_read) > 0:
        print(f"Processing {len(files_to_read)} files...")
        df = spark.read.json(files_to_read)
        print(f"Read done {len(files_to_read)} files...")
        if not df.rdd.isEmpty():
            df_clean = df \
                .withColumn("salary_val", col("max_salary").cast("double")) \
                .filter(col("company_name").isNotNull()) \
                .filter(col("company_name") != "")
                
            print("Proccess 1...")
            company_stats = df_clean.groupBy("company_name").agg(
                count("job_id").alias("job_count"),
                avg("salary_val").alias("avg_salary"),
                max("salary_val").alias("max_salary"),
                min("salary_val").alias("min_salary")
            ).withColumn("batch_id", lit(new_last_file)) \
             .withColumn("last_updated", current_timestamp())
             
            print("Proccess 2...")
            company_stats.cache()
            
            print(">>> Writing Company Analytics to Cassandra...")
            print(">>> Writing to Cassandra (Table: company_analytics)...")
            try:
                company_stats.write \
                    .format("org.apache.spark.sql.cassandra") \
                    .options(table="company_analytics", keyspace="job_metrics") \
                    .mode("append") \
                    .save()
                print("SUCCESS: Cassandra Write OK.")
            except Exception as e:
                print(f"FAILED: Cassandra Write Error: {e}")
                
            print(">>> Writing to Elasticsearch (Index: jobs-analytics)...")
            try:
                company_stats.write \
                    .format("org.elasticsearch.spark.sql") \
                    .option("es.nodes", "elasticsearch.default.svc.cluster.local") \
                    .option("es.port", "9200") \
                    .option("es.resource", "jobs-analytics/_doc") \
                    .option("es.index.auto.create", "true") \
                    .mode("append") \
                    .save()
                print("SUCCESS: Elasticsearch Write OK.")
            except Exception as e:
                print(f"FAILED: Elasticsearch Write Error: {e}")
   
            save_state(new_last_file)
            print(f"Batch completed. New state: {new_last_file}")
            
            company_stats.unpersist()
    else:
        print("No new files found. Waiting for Archiver...")

except Exception as e:
    import traceback
    traceback.print_exc()

spark.stop()