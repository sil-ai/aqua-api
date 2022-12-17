FROM python:3.8.10

COPY scripts/create_cache/create_cache.py scripts/create_cache/create_cache_test.py scripts/common/get_data.py vref.txt scripts/create_cache/requirements.txt /app/
COPY scripts/create_cache/fixtures/ app/fixtures

WORKDIR /app

RUN python3 -m pip install --upgrade pip &&\
    pip install --root-user-action=ignore -r requirements.txt

# RUN pip install "sil-machine[thot]==0.7.4"

# RUN python pachyderm.py
