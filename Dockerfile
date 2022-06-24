FROM python

COPY requirements.txt .

RUN apt update && \
    pip install -r requirements.txt && \
    rm -rf var/lin/apt/lists/*

RUN mkdir /app
COPY app.py /app/app.py
COPY app_test.py /app/app_test.py
COPY queries.py /app/queries.py
COPY key_fetch.py /app/key_fetch.py
COPY vref.txt /app/vref.txt
COPY verse_text.py /app/verse_text.py


WORKDIR /app

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
