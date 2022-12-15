FROM python:3.8.10

COPY scripts/top_source_scores/top_source_scores.py vref.txt scripts/top_source_scores/requirements.txt /app/

WORKDIR /app

RUN python3 -m pip install --upgrade pip &&\
    pip install --root-user-action=ignore -r requirements.txt
