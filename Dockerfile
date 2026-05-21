# syntax=docker/dockerfile:1.6

# ---- Builder stage --------------------------------------------------------
# Use the full Python image so build-time toolchain (gcc, headers needed by
# wheels that don't have manylinux builds) is available. Wheels are built
# into /install and copied into the slim runtime stage.
FROM python:3.11-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Build tools needed to compile any sdist-only deps and git for VCS-pinned
# dependencies (e.g. observability-library). Cleaned up at the end so they
# don't bloat the builder layer.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
        git \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY requirements.txt ./
RUN pip install --upgrade pip \
    && pip install --prefix=/install -r requirements.txt


# ---- Runtime stage --------------------------------------------------------
FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app \
    WEB_CONCURRENCY=4

# Runtime libs only — no compilers. libpq5 for psycopg2 client linkage.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libpq5 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 1001 appuser \
    && useradd  --system --uid 1001 --gid appuser --home-dir /app --shell /usr/sbin/nologin appuser

# Bring pre-built site-packages and console scripts (uvicorn, alembic, etc.)
# from the builder stage.
COPY --from=builder /install /usr/local

WORKDIR /app

# Copy only what the runtime needs. No `COPY *.py` glob — test files stay
# on the host. `--chown` so the non-root appuser owns the tree.
COPY --chown=appuser:appuser app.py            /app/app.py
COPY --chown=appuser:appuser bible_loading.py  /app/bible_loading.py
COPY --chown=appuser:appuser key_fetch.py      /app/key_fetch.py
COPY --chown=appuser:appuser middleware.py     /app/middleware.py
COPY --chown=appuser:appuser models.py         /app/models.py
COPY --chown=appuser:appuser predict_errors.py /app/predict_errors.py
COPY --chown=appuser:appuser queries.py        /app/queries.py

COPY --chown=appuser:appuser agent_routes/      /app/agent_routes/
COPY --chown=appuser:appuser alembic/           /app/alembic/
COPY --chown=appuser:appuser assessment_routes/ /app/assessment_routes/
COPY --chown=appuser:appuser bible_routes/      /app/bible_routes/
COPY --chown=appuser:appuser database/          /app/database/
COPY --chown=appuser:appuser fixtures/          /app/fixtures/
COPY --chown=appuser:appuser predict_routes/    /app/predict_routes/
COPY --chown=appuser:appuser security_routes/   /app/security_routes/
COPY --chown=appuser:appuser train_routes/      /app/train_routes/
COPY --chown=appuser:appuser utils/             /app/utils/

USER appuser

EXPOSE 8000

# Worker count is configurable via $WEB_CONCURRENCY (default 4). `exec` keeps
# uvicorn as PID 1 so it receives SIGTERM cleanly during graceful shutdown.
CMD ["sh", "-c", "exec uvicorn app:app --host 0.0.0.0 --port 8000 --workers ${WEB_CONCURRENCY:-4}"]
