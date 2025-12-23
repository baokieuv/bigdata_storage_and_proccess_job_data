FROM apache/spark:3.5.1

USER root

RUN apt-get update && \
    apt-get install -y curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV CASSANDRA_DRIVER_NO_CYTHON=1
RUN pip install --no-cache-dir kafka-python minio cassandra-driver pandas pyarrow

RUN curl -o $SPARK_HOME/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -o $SPARK_HOME/jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

COPY src /app/src
WORKDIR /app