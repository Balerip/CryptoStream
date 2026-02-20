FROM apache/spark:3.5.3-python3

USER root
WORKDIR /app
COPY spark_stream.py .

CMD ["/opt/spark/bin/spark-submit", \
     "--packages", \
     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0", \
     "/app/spark_stream.py"]