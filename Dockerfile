FROM python:3.11-slim@sha256:da047cb8f9d1d98e5c070f5300ba9f7274e33b8fc0e5be5ed88740aed1b95ba9 AS base

# Install curl and other dependencies
RUN apt-get update && \
    apt-get install -y curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=ghcr.io/astral-sh/uv@sha256:10787c682e4184e4f290de1171fd4703dc63de99221f10fe1c99002ce7fa9acc /uv /uvx /bin/

COPY pyproject.toml uv.lock ./
# Makes it so we can use "python foo" instead of "uv run python foo"
ENV UV_PROJECT_ENVIRONMENT="/usr/local"

RUN uv sync --frozen --no-dev --no-install-project


COPY app/ ./app
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "-m", "app.main"]

FROM minio/mc@sha256:a7fe349ef4bd8521fb8497f55c6042871b2ae640607cf99d9bede5e9bdf11727 AS minio-waiter
COPY tests/wait-for-buckets.sh /app/tests/wait-for-buckets.sh
ENTRYPOINT ["sh", "/app/tests/wait-for-buckets.sh"]
HEALTHCHECK --interval=5s --timeout=3s --retries=3 CMD test -f /tmp/buckets-ready || exit 1

# Test target
FROM base AS test
WORKDIR /app
COPY tests/ ./tests
RUN uv sync --frozen --group test --no-dev --no-install-project

FROM test AS conformance
RUN apt-get update && \
    apt-get install -y git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV S3TESTS_ROOT=/opt/s3-tests
RUN python /app/tests/conformance/fetch_s3tests.py "$S3TESTS_ROOT" && \
    uv pip install --system -r "$S3TESTS_ROOT/requirements.txt"

WORKDIR /app
ENTRYPOINT ["python", "/app/tests/conformance/run_s3tests.py"]
