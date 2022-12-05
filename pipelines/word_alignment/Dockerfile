FROM python:3.8.10
# FROM pytorch/pytorch:latest
# FROM intel/intel-optimized-pytorch:latest

COPY scripts/word_alignment/align_best.py scripts/word_alignment/align.py scripts/word_alignment/combined.py scripts/word_alignment/get_data.py scripts/word_alignment/match.py scripts/word_alignment/pachyderm.py scripts/word_alignment/vref.txt scripts/word_alignment/requirements.txt /app/

COPY scripts/word_alignment/data/models/encoder_weights.txt /app/data/models/

WORKDIR /app

RUN python3 -m pip install --upgrade pip &&\
    pip install --root-user-action=ignore -r requirements.txt

# RUN pip install "sil-machine[thot]==0.7.4"

# RUN python pachyderm.py
