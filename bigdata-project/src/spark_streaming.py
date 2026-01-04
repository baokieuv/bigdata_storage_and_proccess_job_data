from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, coalesce, lit
from pyspark.sql.types import StructType, StringType, DoubleType, LongType

spark = SparkSession.builder \
    .appName("JobStreamingToES") \
    .config("spark.cores.max", "8") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.memory", "512m") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/*") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/*") \
    .getOrCreate()
    
spark.sparkContext.setLogLevel("WARN")

kafka_bootstrap = "my-cluster-kafka-bootstrap.default.svc.cluster.local:9092"
topic = "jobs-topic"
    
schema = StructType() \
    .add("job_id", StringType()) \
    .add("company_name", StringType()) \
    .add("title", StringType()) \
    .add("location", StringType()) \
    .add("views", StringType()) \
    .add("max_salary", StringType()) \
    .add("min_salary", StringType()) \
    .add("listed_time", StringType()) \
    .add("formatted_work_type", StringType())
    
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("subscribe", topic) \
    .option("startingOffsets", "latest") \
    .load()    
    
df_parsed = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")
    
df_clean = df_parsed \
    .withColumn("views_count", col("views").cast(DoubleType())) \
    .withColumn("salary_max", col("max_salary").cast(DoubleType())) \
    .withColumn("salary_min", col("min_salary").cast(DoubleType())) \
    .withColumn("ingest_time", current_timestamp()) \
    .fillna(0, subset=["views_count", "salary_max", "salary_min"])
    
query = df_clean.writeStream \
    .format("org.elasticsearch.spark.sql") \
    .option("checkpointLocation", "/tmp/spark-checkpoint-es") \
    .option("es.nodes", "elasticsearch.default.svc.cluster.local") \
    .option("es.port", "9200") \
    .option("es.resource", "jobs-realtime/_doc") \
    .option("es.index.auto.create", "true") \
    .outputMode("append") \
    .start()

query.awaitTermination()