FROM python:3.8.10
# FROM pytorch/pytorch:latest
# FROM intel/intel-optimized-pytorch:latest

COPY align_best.py align.py combined.py get_data.py match.py pachyderm.py red_flags.py requirements.txt /app/

COPY data/models/encoder_weights.txt /app/data/models/

WORKDIR /app

RUN python3 -m pip install --upgrade pip &&\
    pip install --root-user-action=ignore -r requirements.txt

# RUN python pachyderm.py