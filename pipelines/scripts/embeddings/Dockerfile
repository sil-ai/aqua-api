FROM python:3.8.10

COPY scripts/common/get_data.py scripts/common/vref.txt scripts/embeddings/embeddings.py scripts/embeddings/weights/encoder_weights.txt scripts/embeddings/requirements.txt /app/

WORKDIR /app

RUN python3 -m pip install --upgrade pip &&\
    pip install --root-user-action=ignore -r requirements.txt
