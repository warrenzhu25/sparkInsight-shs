FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src

RUN pip install --no-cache-dir .

RUN mkdir -p /data/eventlogs /data/cache

ENV SPARK_INSIGHT_LOG_DIR=/data/eventlogs
ENV SPARK_INSIGHT_CACHE_DIR=/data/cache

EXPOSE 18080

CMD ["spark-insight", "serve", "--host", "0.0.0.0", "--port", "18080"]
