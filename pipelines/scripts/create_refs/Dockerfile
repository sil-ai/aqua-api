FROM python:3.8.10

COPY scripts/create_refs/create_refs.py scripts/common/get_data.py vref.txt scripts/create_refs/requirements.txt /app/

WORKDIR /app

RUN python3 -m pip install --upgrade pip &&\
    pip install --root-user-action=ignore -r requirements.txt
