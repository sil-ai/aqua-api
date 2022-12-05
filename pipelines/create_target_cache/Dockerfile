FROM python:3.8.10

COPY scripts/word_alignment/create_cache.py scripts/word_alignment/get_data.py scripts/word_alignment/vref.txt scripts/word_alignment/requirements.txt /app/

WORKDIR /app

RUN python3 -m pip install --upgrade pip &&\
    pip install --root-user-action=ignore -r requirements.txt

# RUN pip install "sil-machine[thot]==0.7.4"

# RUN python pachyderm.py
