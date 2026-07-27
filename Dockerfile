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

# Put the uv-managed virtualenv first on PATH so `uvicorn` and the app's imports
# resolve to it.
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH=/app:$PYTHONPATH

HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD curl -fsS http://localhost:8000/health || exit 1

# Keep-alive must exceed the App Runner ingress idle timeout (120s) with margin;
# uvicorn's 5s default races the ingress's connection reuse and yields sporadic 502s.
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "8", "--timeout-keep-alive", "130"]
