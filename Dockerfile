FROM python:3.11-slim AS base

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app
ENV PYTHONUNBUFFERED=1
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "9000"]

FROM minio/mc as minio-waiter
COPY tests/wait-for-buckets.sh /app/tests/wait-for-buckets.sh
ENTRYPOINT ["sh", "/app/tests/wait-for-buckets.sh"]
HEALTHCHECK --interval=5s --timeout=3s --retries=3 CMD test -f /tmp/buckets-ready || exit 1

# Test target
FROM python:3.11-slim AS test
WORKDIR /app
# Install dependencies
COPY tests/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY tests/ ./tests
