FROM python:3.11

# uv drives dependency installation (pinned for reproducible builds).
COPY --from=ghcr.io/astral-sh/uv:0.9.21 /uv /uvx /bin/

# Use the image's Python 3.11 (don't fetch a managed one), copy packages into the
# venv rather than hardlinking across layers, and precompile bytecode for faster
# cold starts.
ENV UV_PYTHON_DOWNLOADS=0 \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1

WORKDIR /app

# Install runtime dependencies only. --no-dev drops the dev group (linting, tests,
# and the Jupyter/IPython stack) from the image; --frozen installs exactly what
# uv.lock pins (it also pulls observability-library from git per pyproject.toml,
# so the deployed image still gets the Loki handler that ships logs when
# LOKI_ENABLED=true). Kept as its own layer so it only re-runs when the manifest
# or lockfile changes, not on every source edit.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

# App source (copied after deps so the dependency layer stays cached across code
# changes). Layout mirrors the previous Dockerfile: alembic/ is merged into
# /app/database alongside database/.
COPY *.py ./
ADD fixtures/ ./fixtures/
ADD agent_routes/ ./agent_routes/
ADD bible_routes/ ./bible_routes/
ADD assessment_routes/ ./assessment_routes/
ADD predict_routes/ ./predict_routes/
ADD security_routes/ ./security_routes/
ADD train_routes/ ./train_routes/
ADD api_v4/ ./api_v4/
ADD schemas/ ./schemas/
ADD database/ ./database
ADD alembic/ ./database
ADD utils/ ./utils

# Put the uv-managed virtualenv first on PATH so `gunicorn` and the app's imports
# resolve to it.
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH=/app:$PYTHONPATH

HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD curl -fsS http://localhost:8000/health || exit 1

# Worker count. Left to $WEB_CONCURRENCY (gunicorn reads it only when --workers
# is absent) so it can be sized to the host without a rebuild. Set it
# explicitly: with the flag gone and the variable unset gunicorn runs a SINGLE
# worker, and a present-but-empty value (WEB_CONCURRENCY=) crashes the
# container at startup on int("").
#
# 8 workers on a 4 vCPU / 8GB host amplified the memory behind the Sep 9 and
# Sep 11 prod outages: each worker carries its own TF-IDF encoder cache. Per
# worker the floor is ~190MB of interpreter and imports (more once sklearn
# imports lazily on the first TF-IDF request), and the cache adds at least
# TFIDF_ENCODER_CACHE_MAX_BYTES (768MB default, see config.py) — "at least"
# because an encoder larger than the whole budget is retained rather than
# evicted, so the real ceiling is per-worker max(budget, largest encoder).
ENV WEB_CONCURRENCY=4

# gunicorn supervises the uvicorn workers (#988). uvicorn's own --workers
# supervisor forks them once and never replaces one that dies, while its parent
# keeps holding the port, so a worker killed by the OOM killer stayed dead and
# the container never looked down enough to restart. gunicorn replaces a dead
# worker within seconds while the others keep serving.
#
# --keep-alive 130: carried over from --timeout-keep-alive 130 under plain
#   uvicorn, set when the App Runner ingress (120s idle) reused connections and
#   uvicorn's 5s default yielded sporadic 502s. nginx today opens a fresh
#   connection per request (no upstream keepalive), so it is inert behind
#   nginx; keep it above nginx's idle time if upstream keepalive is ever added.
# --timeout 120: gunicorn kills a worker whose heartbeat goes silent this long.
#   The uvicorn worker sends that heartbeat from its event loop, not per
#   request, so a slow request does not trip it; only an event loop blocked for
#   roughly 60-120s does, and nginx has given up on the request by then anyway
#   (proxy_read_timeout defaults to 60s). Heavy CPU work already runs off the
#   loop via asyncio.to_thread.
# --max-requests / --max-requests-jitter: recycle each worker now and then to
#   shed slow memory growth in the per-worker TF-IDF caches. High enough that a
#   worker keeps its warm cache for a long time; the jitter stops all workers
#   recycling at once.
# --worker-tmp-dir /dev/shm: the heartbeat is a file touched on every notify;
#   keep it in memory rather than on the container's overlay filesystem.
# --access-logfile -: keep uvicorn's access log on stdout, as before.
CMD ["gunicorn", "app:app", \
     "--worker-class", "uvicorn.workers.UvicornWorker", \
     "--bind", "0.0.0.0:8000", \
     "--keep-alive", "130", \
     "--timeout", "120", \
     "--max-requests", "5000", \
     "--max-requests-jitter", "500", \
     "--worker-tmp-dir", "/dev/shm", \
     "--access-logfile", "-"]
