FROM spark:3.5.3-python3

USER root
WORKDIR /app
COPY consumer.py .

CMD ["/opt/spark/bin/spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3", \
     "/app/consumer.py"]