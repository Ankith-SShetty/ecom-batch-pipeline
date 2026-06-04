FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir confluent-kafka pandas psycopg2-binary

COPY producer.py .

CMD ["python", "producer.py"]
