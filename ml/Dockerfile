FROM python:3.10-slim

WORKDIR /app

# Keep the image focused on the libraries needed by the prototype ingestion flow.
COPY ml/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY ml /app/ml
COPY data /app/data
