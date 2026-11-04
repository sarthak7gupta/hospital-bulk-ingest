# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:0.6.14 AS uv_bin

# Builder stage
FROM python:3.12-slim-bookworm AS builder

ENV UV_COMPILE_BYTECODE=1
ENV UV_NO_INSTALLER_METADATA=1
ENV UV_LINK_MODE=copy

WORKDIR /app

COPY --from=uv_bin /uv /bin/uv

# Install dependencies first using cache and bind mounts to maximize Docker layer caching
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-dev --no-install-project

# Copy source code and build project wheel
COPY . .
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# Production runner stage
FROM python:3.12-slim-bookworm AS runner

LABEL org.opencontainers.image.title="hospital-bulk-processor" \
      org.opencontainers.image.description="FastAPI microservice for bulk hospital CSV ingestion and batch directory activation" \
      org.opencontainers.image.source="https://github.com/paribus/paribus_assignment"

WORKDIR /app

# Create unprivileged system user for security
RUN groupadd -r appuser && useradd -r -g appuser -d /app -s /sbin/nologin appuser

# Copy virtualenv and application from builder
COPY --from=builder --chown=appuser:appuser /app /app

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1
ENV PORT=8000

USER appuser

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD python -c "import os, urllib.request; urllib.request.urlopen('http://localhost:' + str(os.environ.get('PORT', 8000)) + '/health', timeout=5).read()" || exit 1

CMD exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000} --timeout-graceful-shutdown 20
