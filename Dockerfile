FROM python:3.11

COPY requirements.txt requirements-observability.txt ./
# RUN apt update

# Install the observability superset so the deployed image includes the Loki
# handler (observability-library) that ships logs when LOKI_ENABLED=true.
# requirements-observability.txt `-r`'s the base requirements, so this installs
# everything. Contributors without the extra still build from requirements.txt
# directly; only the deployed image needs the observability dependency.
RUN pip install -r requirements-observability.txt && \
    rm -rf var/lin/apt/lists/*

RUN mkdir /app
COPY *.py /app/
ADD fixtures/ /app/fixtures/
ADD agent_routes/ /app/agent_routes/
ADD bible_routes/ /app/bible_routes/
ADD assessment_routes/ /app/assessment_routes/
ADD predict_routes/ /app/predict_routes/
ADD security_routes/ /app/security_routes/
ADD train_routes/ /app/train_routes/
ADD database/ /app/database
ADD alembic/ /app/database
ADD utils/ /app/utils

WORKDIR /app
ENV PYTHONPATH=/app:$PYTHONPATH

HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD curl -fsS http://localhost:8000/health || exit 1

# Keep-alive must exceed the App Runner ingress idle timeout (120s) with margin;
# uvicorn's 5s default races the ingress's connection reuse and yields sporadic 502s.
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "8", "--timeout-keep-alive", "130"]
