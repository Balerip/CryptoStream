FROM apache/spark:3.4.2-python3

USER root
WORKDIR /opt/spark-apps

# Bake S3A JARs in at build time
RUN curl -so /opt/spark/jars/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -so /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Copy spark code into image
COPY consumer.py .

CMD ["/opt/spark/bin/spark-submit", \
     "--master", "local[2]", \
     "--driver-memory", "512m", \
     "--executor-memory", "512m", \
     "--conf", "spark.sql.shuffle.partitions=2", \
     "--packages", \
     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0", \
     "/opt/spark-apps/consumer.py"]