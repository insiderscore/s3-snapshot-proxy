FROM python:3.11-slim@sha256:6d85378d88a19cd4d76079817532d62232be95757cb45945a99fec8e8084b9c2 AS base


# Install curl and other dependencies
RUN apt-get update && \
    apt-get install -y curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app
ENV PYTHONUNBUFFERED=1
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "9000"]

FROM minio/mc@sha256:a7fe349ef4bd8521fb8497f55c6042871b2ae640607cf99d9bede5e9bdf11727 as minio-waiter
COPY tests/wait-for-buckets.sh /app/tests/wait-for-buckets.sh
ENTRYPOINT ["sh", "/app/tests/wait-for-buckets.sh"]
HEALTHCHECK --interval=5s --timeout=3s --retries=3 CMD test -f /tmp/buckets-ready || exit 1

# Test target
FROM base AS test
WORKDIR /app
COPY tests/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY tests/ ./tests
