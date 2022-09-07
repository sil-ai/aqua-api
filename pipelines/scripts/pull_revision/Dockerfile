FROM python:3.8-slim-buster

WORKDIR /app

RUN python3 -m pip install --upgrade pip

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY . .

RUN mkdir out

ENTRYPOINT ["python3", "./pull_rev.py"]
