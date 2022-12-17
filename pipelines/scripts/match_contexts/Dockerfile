FROM python:3.8.10

COPY scripts/common/get_data.py scripts/common/vref.txt scripts/match_contexts/match_contexts.py scripts/match_contexts/match_contexts_test.py scripts/match_contexts/requirements.txt /app/
COPY scripts/match_contexts/fixtures/ app/fixtures

WORKDIR /app

RUN python3 -m pip install --upgrade pip &&\
    pip install --root-user-action=ignore -r requirements.txt

# RUN pip install "sil-machine[thot]==0.7.4"

# RUN python pachyderm.py
