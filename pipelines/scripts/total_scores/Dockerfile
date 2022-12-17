FROM python:3.8.10

COPY scripts/common/get_data.py scripts/total_scores/total_scores.py scripts/total_scores/total_scores_test.py vref.txt scripts/total_scores/requirements.txt /app/
COPY scripts/total_scores/fixtures/ app/fixtures

WORKDIR /app

RUN python3 -m pip install --upgrade pip &&\
    pip install --root-user-action=ignore -r requirements.txt
