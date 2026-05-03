FROM python:3.13-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py database.py db_connection.py scanner.py ./
COPY pages/ ./pages/

ENV PYTHONUNBUFFERED=1

CMD exec streamlit run app.py \
    --server.port="${PORT:-8080}" \
    --server.address=0.0.0.0 \
    --server.headless=true \
    --server.enableCORS=false \
    --server.enableXsrfProtection=false
