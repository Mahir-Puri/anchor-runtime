# Single image for both roles. The API and the worker differ only by the command
# they are started with, which keeps the deploy story to one artifact.
FROM python:3.12-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
COPY schema ./schema

RUN pip install --no-cache-dir .

# Non-root, because a runtime that executes model-chosen tool calls is not the
# place to be running as uid 0.
RUN useradd --create-home --uid 10001 anchor
USER anchor

ENV ANCHOR_SCHEMA_DIR=/app/schema

EXPOSE 8000

HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/healthz', timeout=2).status==200 else 1)"

CMD ["anchor", "api", "--host", "0.0.0.0", "--port", "8000"]
