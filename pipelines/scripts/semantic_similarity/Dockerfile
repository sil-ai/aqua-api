FROM python:3.8-slim-buster

WORKDIR /app

COPY . .

RUN python3 -m pip install --upgrade pip &&\
    pip install --root-user-action=ignore -r requirements.txt