# Quanfina API

FastAPI iskelet — POC ADIM 2.

## Kurulum

```powershell
# api/ klasöründen
python -m venv .venv
.venv\Scripts\pip install -r requirements.txt
```

## Çalıştırma

```powershell
# api/ klasöründen
.venv\Scripts\python.exe -m uvicorn main:app --reload --port 8000
```

Endpoints:
- `GET /api/health` → `{ status, service, timestamp, db_connected }`

.env dosyası proje kökünden (`C:\Projeler\Quanfina\.env`) otomatik okunur.
