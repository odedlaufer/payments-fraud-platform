# Streaming-path metrics: Postgres replication slots, Kafka Connect tasks, Flink jobs.
FROM python:3.12.11-slim
RUN pip install --no-cache-dir "psycopg[binary]==3.3.6" prometheus-client==0.26.0
COPY stream_metrics.py /app/stream_metrics.py
USER nobody
EXPOSE 9110
CMD ["python", "/app/stream_metrics.py"]
