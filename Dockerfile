FROM python

COPY requirements.txt .

RUN apt update && \
    pip install -r requirements.txt && \
    rm -rf var/lin/apt/lists/*

RUN mkdir /app
COPY app.py /app/app.py
COPY app_test.py /app/app_test.py

WORKDIR /app
