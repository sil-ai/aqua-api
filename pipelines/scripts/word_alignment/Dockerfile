FROM python:3.8-slim

COPY ["requirements.txt", "./"]

RUN pip install -r requirements.txt

COPY ["combined.py", "match.py", "align.py", "pachyderm.py", "./"]

CMD python pachyderm.py