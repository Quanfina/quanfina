"""
Quanfina FastAPI — POC ADIM 3
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

from typing import Optional

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


class MinerviniStock(BaseModel):
    symbol: str
    company: str
    sector: str
    price: float
    change_pct: float
    grade: str
    rs_ibd: float
    rs_12m: float
    ma200_slope: float
    high52: float
    pct_from_high: float
    eps_qoq: float
    sales_qoq: float
    volume: int
    market_cap: float
    confirmations: int
    violations: int
    sma50: float
    atr14: float
    pivot_price: Optional[float]
    list_type: str


MOCK_STOCKS: list[MinerviniStock] = [
    # buy (2)
    MinerviniStock(symbol="NVDA", company="NVIDIA Corp", sector="Technology",
        price=875.40, change_pct=2.35, grade="A", rs_ibd=97, rs_12m=95,
        ma200_slope=0.52, high52=974.00, pct_from_high=-10.1,
        eps_qoq=88.5, sales_qoq=122.0, volume=42_000_000, market_cap=2150.0,
        confirmations=8, violations=0, sma50=852.00, atr14=28.50,
        pivot_price=820.00, list_type="buy"),
    MinerviniStock(symbol="AVGO", company="Broadcom Inc", sector="Technology",
        price=1680.20, change_pct=1.87, grade="A", rs_ibd=94, rs_12m=90,
        ma200_slope=0.45, high52=1876.00, pct_from_high=-10.4,
        eps_qoq=61.0, sales_qoq=47.0, volume=6_200_000, market_cap=780.0,
        confirmations=7, violations=0, sma50=1620.00, atr14=52.30,
        pivot_price=1620.00, list_type="buy"),
    # focus (3)
    MinerviniStock(symbol="META", company="Meta Platforms Inc", sector="Communication Services",
        price=525.80, change_pct=0.94, grade="A", rs_ibd=92, rs_12m=87,
        ma200_slope=0.41, high52=589.00, pct_from_high=-10.7,
        eps_qoq=35.0, sales_qoq=27.0, volume=14_500_000, market_cap=1340.0,
        confirmations=7, violations=1, sma50=510.00, atr14=18.40,
        pivot_price=505.00, list_type="focus"),
    MinerviniStock(symbol="MSFT", company="Microsoft Corp", sector="Technology",
        price=415.60, change_pct=0.42, grade="A", rs_ibd=88, rs_12m=82,
        ma200_slope=0.33, high52=468.00, pct_from_high=-11.2,
        eps_qoq=22.0, sales_qoq=17.0, volume=22_000_000, market_cap=3090.0,
        confirmations=6, violations=0, sma50=405.00, atr14=10.80,
        pivot_price=None, list_type="focus"),
    MinerviniStock(symbol="COST", company="Costco Wholesale Corp", sector="Consumer Staples",
        price=890.30, change_pct=0.62, grade="B", rs_ibd=85, rs_12m=79,
        ma200_slope=0.28, high52=1010.00, pct_from_high=-11.8,
        eps_qoq=14.0, sales_qoq=9.0, volume=2_100_000, market_cap=395.0,
        confirmations=6, violations=1, sma50=870.00, atr14=22.10,
        pivot_price=None, list_type="focus"),
    # on_deck (5)
    MinerviniStock(symbol="GOOGL", company="Alphabet Inc Class A", sector="Communication Services",
        price=178.40, change_pct=1.22, grade="A", rs_ibd=83, rs_12m=77,
        ma200_slope=0.30, high52=207.00, pct_from_high=-13.8,
        eps_qoq=28.0, sales_qoq=15.0, volume=25_000_000, market_cap=2190.0,
        confirmations=5, violations=1, sma50=172.00, atr14=5.40,
        pivot_price=None, list_type="on_deck"),
    MinerviniStock(symbol="ASML", company="ASML Holding NV", sector="Technology",
        price=742.10, change_pct=-0.31, grade="A", rs_ibd=81, rs_12m=75,
        ma200_slope=0.22, high52=1110.00, pct_from_high=-33.1,
        eps_qoq=24.0, sales_qoq=14.0, volume=1_500_000, market_cap=292.0,
        confirmations=5, violations=2, sma50=720.00, atr14=26.80,
        pivot_price=None, list_type="on_deck"),
    MinerviniStock(symbol="AMZN", company="Amazon.com Inc", sector="Consumer Discretionary",
        price=196.70, change_pct=0.78, grade="A", rs_ibd=80, rs_12m=74,
        ma200_slope=0.35, high52=232.00, pct_from_high=-15.2,
        eps_qoq=61.0, sales_qoq=11.0, volume=45_000_000, market_cap=2060.0,
        confirmations=5, violations=1, sma50=191.00, atr14=5.90,
        pivot_price=None, list_type="on_deck"),
    MinerviniStock(symbol="ORCL", company="Oracle Corp", sector="Technology",
        price=148.90, change_pct=0.52, grade="B", rs_ibd=76, rs_12m=70,
        ma200_slope=0.20, high52=198.00, pct_from_high=-24.8,
        eps_qoq=18.0, sales_qoq=8.0, volume=10_200_000, market_cap=412.0,
        confirmations=4, violations=2, sma50=144.00, atr14=4.70,
        pivot_price=None, list_type="on_deck"),
    MinerviniStock(symbol="CRM", company="Salesforce Inc", sector="Technology",
        price=296.40, change_pct=-0.14, grade="B", rs_ibd=74, rs_12m=68,
        ma200_slope=0.15, high52=369.00, pct_from_high=-19.7,
        eps_qoq=11.0, sales_qoq=9.0, volume=7_800_000, market_cap=286.0,
        confirmations=4, violations=2, sma50=290.00, atr14=9.20,
        pivot_price=None, list_type="on_deck"),
    # watch (10)
    MinerviniStock(symbol="NFLX", company="Netflix Inc", sector="Communication Services",
        price=672.30, change_pct=1.45, grade="A", rs_ibd=79, rs_12m=73,
        ma200_slope=0.38, high52=758.00, pct_from_high=-11.3,
        eps_qoq=44.0, sales_qoq=15.0, volume=5_400_000, market_cap=288.0,
        confirmations=5, violations=1, sma50=652.00, atr14=21.60,
        pivot_price=None, list_type="watch"),
    MinerviniStock(symbol="AMD", company="Advanced Micro Devices Inc", sector="Technology",
        price=158.70, change_pct=-0.88, grade="B", rs_ibd=73, rs_12m=67,
        ma200_slope=0.08, high52=227.00, pct_from_high=-30.1,
        eps_qoq=31.0, sales_qoq=24.0, volume=55_000_000, market_cap=257.0,
        confirmations=3, violations=3, sma50=155.00, atr14=6.80,
        pivot_price=None, list_type="watch"),
    MinerviniStock(symbol="ADBE", company="Adobe Inc", sector="Technology",
        price=394.50, change_pct=0.32, grade="B", rs_ibd=71, rs_12m=65,
        ma200_slope=0.04, high52=638.00, pct_from_high=-38.2,
        eps_qoq=14.0, sales_qoq=10.0, volume=4_600_000, market_cap=174.0,
        confirmations=3, violations=3, sma50=388.00, atr14=14.30,
        pivot_price=None, list_type="watch"),
    MinerviniStock(symbol="V", company="Visa Inc", sector="Financials",
        price=275.80, change_pct=0.41, grade="B", rs_ibd=75, rs_12m=69,
        ma200_slope=0.17, high52=311.00, pct_from_high=-11.3,
        eps_qoq=12.0, sales_qoq=10.0, volume=9_100_000, market_cap=544.0,
        confirmations=4, violations=1, sma50=268.00, atr14=7.20,
        pivot_price=None, list_type="watch"),
    MinerviniStock(symbol="MA", company="Mastercard Inc", sector="Financials",
        price=477.20, change_pct=0.28, grade="B", rs_ibd=73, rs_12m=67,
        ma200_slope=0.14, high52=542.00, pct_from_high=-12.0,
        eps_qoq=10.0, sales_qoq=12.0, volume=4_200_000, market_cap=440.0,
        confirmations=4, violations=1, sma50=465.00, atr14=12.40,
        pivot_price=None, list_type="watch"),
    MinerviniStock(symbol="JPM", company="JPMorgan Chase & Co", sector="Financials",
        price=215.40, change_pct=0.63, grade="B", rs_ibd=70, rs_12m=64,
        ma200_slope=0.21, high52=280.00, pct_from_high=-23.1,
        eps_qoq=9.0, sales_qoq=7.0, volume=12_000_000, market_cap=618.0,
        confirmations=3, violations=2, sma50=210.00, atr14=6.10,
        pivot_price=None, list_type="watch"),
    MinerviniStock(symbol="HD", company="Home Depot Inc", sector="Consumer Discretionary",
        price=348.90, change_pct=-0.22, grade="C", rs_ibd=62, rs_12m=56,
        ma200_slope=0.06, high52=420.00, pct_from_high=-17.0,
        eps_qoq=3.0, sales_qoq=5.0, volume=4_800_000, market_cap=346.0,
        confirmations=2, violations=3, sma50=344.00, atr14=9.60,
        pivot_price=None, list_type="watch"),
    MinerviniStock(symbol="WMT", company="Walmart Inc", sector="Consumer Staples",
        price=89.60, change_pct=0.11, grade="C", rs_ibd=58, rs_12m=52,
        ma200_slope=0.09, high52=106.00, pct_from_high=-15.5,
        eps_qoq=5.0, sales_qoq=5.0, volume=18_500_000, market_cap=720.0,
        confirmations=2, violations=3, sma50=87.00, atr14=2.20,
        pivot_price=None, list_type="watch"),
    MinerviniStock(symbol="AMAT", company="Applied Materials Inc", sector="Technology",
        price=178.20, change_pct=-1.14, grade="C", rs_ibd=55, rs_12m=49,
        ma200_slope=-0.03, high52=255.00, pct_from_high=-30.1,
        eps_qoq=7.0, sales_qoq=6.0, volume=9_700_000, market_cap=149.0,
        confirmations=1, violations=4, sma50=175.00, atr14=7.80,
        pivot_price=None, list_type="watch"),
    MinerviniStock(symbol="MRVL", company="Marvell Technology Inc", sector="Technology",
        price=62.40, change_pct=-2.18, grade="D", rs_ibd=48, rs_12m=42,
        ma200_slope=-0.12, high52=119.00, pct_from_high=-47.6,
        eps_qoq=38.0, sales_qoq=27.0, volume=22_000_000, market_cap=53.0,
        confirmations=0, violations=5, sma50=65.00, atr14=3.90,
        pivot_price=None, list_type="watch"),
]


@app.get("/api/minervini/stocks", response_model=list[MinerviniStock])
def get_minervini_stocks() -> list[MinerviniStock]:
    return MOCK_STOCKS


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
