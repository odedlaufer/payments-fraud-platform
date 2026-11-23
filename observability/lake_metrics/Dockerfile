# Lakehouse metrics exporter: queries Trino's Iceberg metadata once a minute.
FROM python:3.12.11-slim
RUN pip install --no-cache-dir trino==0.340.0 prometheus-client==0.26.0
COPY lake_metrics.py /app/lake_metrics.py
USER nobody
EXPOSE 9109
CMD ["python", "/app/lake_metrics.py"]
