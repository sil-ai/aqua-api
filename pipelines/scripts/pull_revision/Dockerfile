FROM python:3.8-slim-buster

WORKDIR /app

COPY . .

RUN apt-get update && apt-get -y install jq &&\
    apt-get -y install zip &&\
    python3 -m pip install --upgrade pip &&\
    pip install --root-user-action=ignore -r requirements.txt
