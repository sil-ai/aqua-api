FROM python:3.8-slim-buster

WORKDIR /app

COPY requirements.txt requirements.txt

RUN python3 -m pip install --upgrade pip &&\
    pip install --root-user-action=ignore -r requirements.txt

COPY . .

ENTRYPOINT ["python3", "./split_revision.py"]