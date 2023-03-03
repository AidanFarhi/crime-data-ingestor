FROM python:3.10-slim

WORKDIR /app

ADD requirements.txt .
ADD app.py .

RUN pip install -r requirements.txt

CMD [ "echo", "complete" ]