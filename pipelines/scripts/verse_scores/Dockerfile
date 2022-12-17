FROM python:3.8.10

COPY scripts/verse_scores/verse_scores.py vref.txt scripts/verse_scores/requirements.txt /app/

WORKDIR /app

RUN python3 -m pip install --upgrade pip &&\
    pip install --root-user-action=ignore -r requirements.txt
