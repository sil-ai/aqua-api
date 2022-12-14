FROM python:3.8.10

COPY scripts/common/get_data.py scripts/common/train_fa_model.py scripts/common/vref.txt scripts/translation_scores/translation_scores.py scripts/translation_scores/requirements.txt /app/

WORKDIR /app

RUN python3 -m pip install --upgrade pip &&\
    pip install --root-user-action=ignore -r requirements.txt

# RUN pip install "sil-machine[thot]==0.7.4"

# RUN python pachyderm.py
