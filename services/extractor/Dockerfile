FROM python:3.11-slim

WORKDIR /app

# Copy the extractor requirements from the correct path in the context
COPY services/extractor/requirements.txt requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

# Copy shared BigQuery helper and extractor code
COPY shared ./shared
COPY services/extractor/main.py ./main.py

ENV PORT=8080

CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 main:app
