"""
Quanfina FastAPI — POC ADIM 2
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

# Project root'u sys.path'e ekle — db_connection + quanfina_math importlanabilsin
_ROOT = Path(__file__).parent.parent
load_dotenv(_ROOT / ".env")          # .env'i db_connection importu ÖNCE yükle
sys.path.insert(0, str(_ROOT))

from db_connection import get_connection  # noqa: E402

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "msg": "%(message)s"}',
)
log = logging.getLogger(__name__)

app = FastAPI(title="Quanfina API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class HealthResponse(BaseModel):
    status: str
    service: str
    timestamp: str
    db_connected: bool


@app.get("/api/health", response_model=HealthResponse)
def health() -> HealthResponse:
    db_connected = False
    try:
        conn = get_connection()
        conn.close()
        db_connected = True
        log.info("DB health check OK")
    except Exception as exc:
        log.warning("DB health check failed: %s", exc)

    return HealthResponse(
        status="ok",
        service="quanfina-api",
        timestamp=datetime.now(timezone.utc).isoformat(),
        db_connected=db_connected,
    )
