FROM python

WORKDIR /app/fixtures
COPY fixtures .

WORKDIR /app

COPY combined.py .
COPY align.py .
COPY match.py .
COPY combined_test.py .
COPY align_test.py .
COPY match_test.py .
COPY requirements.txt .

RUN python3 -m pip install --upgrade pip &&\
    pip install --root-user-action=ignore -r requirements.txt

RUN pip install "sil-machine[thot]"