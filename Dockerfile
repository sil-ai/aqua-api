FROM python:3.8.10

COPY requirements.txt .

RUN apt update && \
    pip install -r requirements.txt && \
    rm -rf var/lin/apt/lists/*

RUN mkdir /app
COPY *.py /app/
ADD fixtures/ /app/fixtures/
ADD bible_routes/ /app/bible_routes/
ADD assessment_routes/ /app/assessment_routes/
ADD review_routes/ /app/review_routes/

WORKDIR /app

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
