# Sử dụng Spark 3.4.1 (Scala 2.12)
FROM apache/spark:3.4.1

USER root

# Cài đặt các thư viện hệ thống cơ bản
RUN apt-get update && \
    apt-get install -y curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# --- Tối ưu cài đặt Python ---
# Biến này giúp cài cassandra-driver siêu nhanh (bỏ qua compile C)
ENV CASSANDRA_DRIVER_NO_CYTHON=1
# Thêm --prefer-binary để ưu tiên bản cài sẵn, tránh compile pyarrow
RUN pip install --no-cache-dir --prefer-binary \
    kafka-python \
    minio \
    cassandra-driver \
    pandas \
    pyarrow \
    requests

# --- Cài đặt JARs (Đã sửa lại Version cho khớp Spark 3.4.1) ---
# Folder chuẩn chứa JAR của Spark là $SPARK_HOME/jars (/opt/spark/jars)
# WORKDIR $SPARK_HOME/jars

# 1. Hadoop AWS & AWS SDK (Phiên bản này khớp với Hadoop 3.3.4 có sẵn trong Spark 3.4.1)
RUN curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# 2. Elasticsearch Spark (Dùng bản cho Spark 3.x)
RUN curl -O https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/8.11.1/elasticsearch-spark-30_2.12-8.11.1.jar

# 3. Spark Cassandra Connector (Sử dụng bản 3.4.1 để khớp với Spark 3.4.1)
RUN curl -L -O https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector-assembly_2.12/3.4.1/spark-cassandra-connector-assembly_2.12-3.4.1.jar

# 4. Kafka & Spark SQL Kafka (Sử dụng bản 3.4.1 để khớp với Spark 3.4.1)
# Lưu ý: Spark 3.4.1 thường đi kèm Kafka client cũ hơn, nhưng ta nên dùng bản match với Spark
RUN curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.1/spark-sql-kafka-0-10_2.12-3.4.1.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.4.1/spark-token-provider-kafka-0-10_2.12-3.4.1.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# --- Thiết lập App ---
WORKDIR /app
COPY src /app/src

# (Tuỳ chọn) Chuyển lại về user spark để bảo mật tốt hơn, 
# nhưng nếu dùng trong dev/docker-compose thì để root cũng được cho dễ mount volume.
# USER 185