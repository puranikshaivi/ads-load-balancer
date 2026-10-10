FROM python:3.9-slim

RUN apt-get update

COPY requirements.txt /app/requirements.txt

RUN pip3 install --no-cache-dir -r /app/requirements.txt

COPY client /app/client
COPY server /app/server
COPY experiments /app/experiments
COPY load_balancer /app/load_balancer

WORKDIR /app

CMD ["python3"]