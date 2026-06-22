# Quanfina FastAPI — Cloud Run production image (P572, 20 Haz 2026).
# Build context = REPO KÖKÜ (api/main.py kök modülleri import eder: quanfina_math, db_connection).
# main.py kendi sys.path'ini kurar (_ROOT + _API_DIR) → WORKDIR /app/api, `uvicorn main:app`.
# DB: Cloud SQL connector (--add-cloudsql-instances) + Secret Manager PG_* (scanner pattern).
FROM python:3.13-slim

# psycopg2-binary + yfinance/pandas runtime için minimal (binary wheel'ler kullanır, build-essential gereksiz)
WORKDIR /app

COPY api/requirements.txt ./api/requirements.txt
RUN pip install --no-cache-dir -r api/requirements.txt

# Kök modüller (api/main.py bunları import eder) + api paketi
# P582 (22 Haz 2026): market_calendar.py eklendi — /api/market/calendar/status import
# ediyordu (main.py:1829) ama imajda yoktu -> "No module named market_calendar" -> takvim
# rozeti (MarketStatusBadge) bozuk fallback. Kural #28: gercek modul prod'da olmali.
COPY quanfina_math.py db_connection.py market_calendar.py ./
COPY api/ ./api/

WORKDIR /app/api
ENV PYTHONUNBUFFERED=1
# Cloud Run $PORT verir (default 8080). Tek worker — Cloud Run concurrency yönetir.
CMD exec uvicorn main:app --host 0.0.0.0 --port ${PORT:-8080} --workers 1
