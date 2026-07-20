FROM python:3.11

COPY requirements.txt ./

# requirements.txt includes observability-library, so the deployed image gets
# the Loki handler that ships logs when LOKI_ENABLED=true.
RUN pip install -r requirements.txt

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
