FROM python:3.11

COPY requirements.txt .
# RUN apt update

RUN pip install -r requirements.txt && \
    rm -rf var/lin/apt/lists/*

RUN mkdir /app
COPY *.py /app/
ADD fixtures/ /app/fixtures/
ADD bible_routes/ /app/bible_routes/
ADD assessment_routes/ /app/assessment_routes/
ADD review_routes/ /app/review_routes/
ADD security_routes/ /app/security_routes/
ADD database/ /app/database
ADD alembic/ /app/database

WORKDIR /app
ENV PYTHONPATH=/app:$PYTHONPATH


CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "8"]
