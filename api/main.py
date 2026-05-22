"""
Quanfina FastAPI — POC ADIM 8
"""
from __future__ import annotations

import logging
import random
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

# Project root'u sys.path'e ekle — db_connection + quanfina_math importlanabilsin
_ROOT = Path(__file__).parent.parent
_API_DIR = Path(__file__).parent
load_dotenv(_ROOT / ".env")          # .env'i db_connection importu ÖNCE yükle
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_API_DIR))    # db_helpers importu için

from db_connection import get_connection  # noqa: E402
from db_helpers import (  # noqa: E402
    db_health_check,
    watchlist_get_all, watchlist_get_one, watchlist_exists,
    watchlist_insert, watchlist_update, watchlist_delete,
    watchlist_recompute_consensus,
    trades_get_all, trades_get_by_id,
    trades_insert, trades_update, trades_delete,
)

from typing import Literal, Optional

from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy.exc import OperationalError

logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "msg": "%(message)s"}',
)
log = logging.getLogger(__name__)

app = FastAPI(title="Quanfina API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3001"],
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


# ── Sprint 4-bis.1b: Screens (8 ready) ────────────────────────────────────────
# Kaynak: notebook/Notebook_C1_Sprint_QuickStart.md
# Pattern: MOCK fallback (db_connected=false → MOCK), gerçek SQL (db_connected=true)

from api.db_helpers import (
    screen_get_results,
    screen_get_results_dispatch,
    screen_list_available,
    SCREENS_READY_8,
    SCREENS_PARSE_7,
    SCREENS_DIFF_6,
)


class ScreenMeta(BaseModel):
    slug: str
    label: str
    filter_summary: str
    category: str = "ready"  # "ready" | "parse" | "deferred"


class ScreenResultRow(BaseModel):
    symbol: str
    grade: Optional[str] = None
    rs_ibd: Optional[int] = None
    price: Optional[float] = None
    passed: Optional[int] = None
    scan_date: Optional[str] = None
    # KARAR #466 (20 May 2026) — VCP Kalite Skoru: "EXCELLENT" | "PASS" | None
    # tight_low_volume slug'inda anlamli, digerlerinde None (UI rozet gizlenir)
    vcp_quality_score: Optional[str] = None
    # KARAR #465 (20 May 2026) — VCP Ready Score 0-100 (Inside Day + V-Dry + Tight)
    # vcp_ready_high slug + tight_low_volume slug'larda anlamli
    vcp_ready_score: Optional[int] = None
    # KARAR #467 (20 May 2026) — Power Play (HTF) Mark canon
    # power_play_ready slug + tight_low_volume slug'larda anlamli
    power_play_pass: Optional[bool] = None


@app.get("/api/screens", response_model=list[ScreenMeta])
def list_screens() -> list[ScreenMeta]:
    """Mevcut 8 ready screen meta listesi (frontend dropdown icin)."""
    return [ScreenMeta(**m) for m in screen_list_available()]


@app.get("/api/screens/{slug}", response_model=list[ScreenResultRow])
def get_screen_results(slug: str, limit: int = 500) -> list[ScreenResultRow]:
    """
    Sprint 4-bis.1b (ready) + Sprint 4-bis.2 (parse) + Sprint 4-bis.3 (diff)
    + Sprint 4-bis.4 (tight_low_volume — pre-compute) — slug dispatch ile sonuc dondur.

    - SCREENS_READY_9 (9 ekran, saf SQL — tight_low_volume artik buraya dahil)
    - SCREENS_PARSE_7 (7 ekran, confirmations/violations text-parse SQL)
    - SCREENS_DIFF_6 (6 ekran, Self-JOIN onceki scan karsilastirma)

    db_connected=false durumunda MOCK donus (dev ortam).
    db_connected=true -> minervini_scans tablosundan gercek sorgu.

    KARAR #461 (19 May 2026): tight_low_volume artik Master pre-compute
    stratejisiyle SCREENS_READY_9'a tasindi (scanner.py tight_low_vol_pass
    BOOLEAN kolonunu yazar, SQL sade WHERE okur). 501 deferred kaldirildi.
    """
    valid_slugs = (
        set(SCREENS_READY_8.keys()) |  # alias of SCREENS_READY_9 (geriye uyum)
        set(SCREENS_PARSE_7.keys()) |
        set(SCREENS_DIFF_6.keys())
    )
    if slug not in valid_slugs:
        from fastapi import HTTPException
        raise HTTPException(
            status_code=404,
            detail=f"Screen slug bulunamadi: '{slug}'. "
                   f"Gecerli ready: {list(SCREENS_READY_8.keys())}; "
                   f"parse: {list(SCREENS_PARSE_7.keys())}; "
                   f"diff: {list(SCREENS_DIFF_6.keys())}"
        )

    # MOCK fallback (dev ortam, db_connected=false)
    # KARAR #466+#465+#467 — VCP/Power Play slug'larinda kalite+ready+power_play sahte
    if not db_health_check():
        is_quality_slug = slug in (
            "tight_low_volume", "tight_low_vol_excellent",
            "vcp_ready_high", "power_play_ready"
        )
        mock_rows = [
            ScreenResultRow(symbol="NVDA", grade="A", rs_ibd=99,
                            price=145.20, passed=1, scan_date="2026-05-19",
                            vcp_quality_score="EXCELLENT" if is_quality_slug else None,
                            vcp_ready_score=85 if is_quality_slug else None,
                            power_play_pass=True if is_quality_slug else None),
            ScreenResultRow(symbol="AAPL", grade="A", rs_ibd=87,
                            price=212.50, passed=1, scan_date="2026-05-19",
                            vcp_quality_score="PASS" if is_quality_slug else None,
                            vcp_ready_score=62 if is_quality_slug else None,
                            power_play_pass=False if is_quality_slug else None),
            ScreenResultRow(symbol="MSFT", grade="B", rs_ibd=91,
                            price=425.30, passed=1, scan_date="2026-05-19",
                            vcp_quality_score="EXCELLENT" if is_quality_slug else None,
                            vcp_ready_score=78 if is_quality_slug else None,
                            power_play_pass=True if is_quality_slug else None),
            ScreenResultRow(symbol="GOOGL", grade="B", rs_ibd=88,
                            price=178.40, passed=1, scan_date="2026-05-19",
                            vcp_quality_score="PASS" if is_quality_slug else None,
                            vcp_ready_score=55 if is_quality_slug else None,
                            power_play_pass=False if is_quality_slug else None),
            ScreenResultRow(symbol="AMD", grade="C", rs_ibd=85,
                            price=158.20, passed=1, scan_date="2026-05-19",
                            vcp_quality_score=None,
                            vcp_ready_score=42 if is_quality_slug else None,
                            power_play_pass=False if is_quality_slug else None),
        ]
        # Slug bazli filtre
        if slug == "tight_low_vol_excellent":
            mock_rows = [r for r in mock_rows if r.vcp_quality_score == "EXCELLENT"]
        elif slug == "vcp_ready_high":
            mock_rows = [r for r in mock_rows
                         if r.vcp_ready_score is not None and r.vcp_ready_score >= 70]
        elif slug == "power_play_ready":
            mock_rows = [r for r in mock_rows if r.power_play_pass is True]
        return mock_rows[:limit]

    # Gerçek DB sorgusu — ready VEYA parse VEYA diff (dispatch)
    rows = screen_get_results_dispatch(slug, limit=limit)
    return [ScreenResultRow(**{k: r.get(k) for k in
                                ("symbol","grade","rs_ibd","price","passed","scan_date",
                                 "vcp_quality_score","vcp_ready_score","power_play_pass")})
            for r in rows]


@app.get("/api/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(
        status="ok",
        service="quanfina-api",
        timestamp=datetime.now(timezone.utc).isoformat(),
        db_connected=db_health_check(),
    )


# ── Terim Sözlüğü ──────────────────────────────────────────────────────────────

class Term(BaseModel):
    key: str
    short_name: str
    tooltip: str
    definition: str
    source_book: Optional[str] = None
    source_author: Optional[str] = None
    source_year: Optional[int] = None
    quanfina_context: str
    category: str  # technical | fundamental | strategy | risk


MOCK_TERMS: list[Term] = [
    Term(
        key="spy_stage_2",
        short_name="SPY Stage 2",
        tooltip="S&P 500 Stage 2 yükseliş trendi — piyasa bull fazında.",
        definition=(
            "Stan Weinstein'ın 4 aşama modelinde Stage 2, 200 haftalık hareketli "
            "ortalama üzerinde yatay bazdan çıkışı ifade eden yükseliş trendinin "
            "başlangıcı ve ana aşamasıdır. SPY Stage 2 = genel piyasa sağlıklı."
        ),
        source_book="Secrets for Profiting in Bull and Bear Markets",
        source_author="Stan Weinstein",
        source_year=1988,
        quanfina_context="Piyasa Durumu sayfasında SPY/QQQ/IWM için Stage tespiti. Stage 2 = LONG mod önerilir.",
        category="technical",
    ),
    Term(
        key="rs_ibd",
        short_name="RS IBD",
        tooltip="IBD Göreceli Güç Puanı. 99 = en iyi %1, 1 = en kötü %1.",
        definition=(
            "Investor's Business Daily'nin geliştirdiği 1–99 arası göreceli güç "
            "puanı. Son 12 ayda bir hissenin tüm hisselere göre fiyat performansını "
            "ölçer. Minervini Trend Template koşullarından biri: RS > 70."
        ),
        source_book="How to Make Money in Stocks",
        source_author="William J. O'Neil",
        source_year=1988,
        quanfina_context="Minervini tarama grid'inde sütun. Renk: 0-99 arası yeşil gradient.",
        category="technical",
    ),
    Term(
        key="ma200_slope",
        short_name="MA200 Eğim",
        tooltip="200 günlük hareketli ortalama eğimi. Pozitif = uzun vadeli yükseliş.",
        definition=(
            "200 günlük basit hareketli ortalamanın eğimi (günlük değişim). "
            "Pozitif eğim uzun vadeli yükseliş trendini gösterir. "
            "Minervini Trend Template: MA200 eğimi pozitif olmalı."
        ),
        source_book=None,
        source_author=None,
        source_year=None,
        quanfina_context="Minervini grid MA200 EĞİM sütunu. Pozitif = yeşil, negatif = kırmızı.",
        category="technical",
    ),
    Term(
        key="vcp",
        short_name="VCP",
        tooltip="Volatility Contraction Pattern — daralan volatilite bazı, Minervini.",
        definition=(
            "Mark Minervini'nin tanımladığı Volatility Contraction Pattern (VCP). "
            "Fiyat konsolidasyonu sırasında hacim azalırken volatilitenin kademeli "
            "olarak daraldığı formasyon. Kırılım öncesi kurumsal birikimi yansıtır. "
            "Tipik dizi: %25 → %12 → %6 → %2 daralma."
        ),
        source_book="Trade Like a Stock Market Wizard",
        source_author="Mark Minervini",
        source_year=2013,
        quanfina_context="Minervini stratejisinin ana setup paterni. Tarama motorunda VCP kriterleri uygulanır.",
        category="strategy",
    ),
    Term(
        key="pullback",
        short_name="Pullback",
        tooltip="Yükseliş trendinde geçici geri çekilme — Carr'ın 5 long setup'ından biri.",
        definition=(
            "Thomas Carr'ın tanımladığı Pullback setup: Ana yükseliş trendi devam "
            "ederken oluşan geçici geri çekilme. Ana trende geri döneceği öngörüsüyle "
            "momentum girişi yapılır. Destek bölgesi (MA20/MA50) yakınında tetikler."
        ),
        source_book="Trend Trading for a Living",
        source_author="Thomas Carr",
        source_year=2008,
        quanfina_context="Carr sayfasında Long Setup kartı. Tooltip henüz NotebookLM'den detaylandırılacak.",
        category="strategy",
    ),
    Term(
        key="coiled_spring",
        short_name="Coiled Spring",
        tooltip="Sıkışma sonrası kırılım potansiyeli — Carr'ın 5 long setup'ından biri.",
        definition=(
            "Thomas Carr'ın tanımladığı Coiled Spring setup: Dar bant içinde volatilite "
            "sıkışması ve ardından yüksek hacimli kırılım potansiyeli. VCP'ye benzer "
            "ancak Carr metodolojisine göre tanımlanır."
        ),
        source_book="Trend Trading for a Living",
        source_author="Thomas Carr",
        source_year=2008,
        quanfina_context="Carr sayfasında Long Setup kartı.",
        category="strategy",
    ),
    Term(
        key="distribution_days",
        short_name="Distribution Days",
        tooltip="Artan hacimle endeks düşüşü — kurumsal satış sinyali (IBD kavramı).",
        definition=(
            "IBD (Investor's Business Daily) kavramı. S&P 500 veya Nasdaq'ın bir "
            "önceki güne göre %0.2+ düşerken hacmin artması. 4–6 adet distribution "
            "day birikimi piyasa topu ve satış baskısı sinyali verir."
        ),
        source_book="How to Make Money in Stocks",
        source_author="William J. O'Neil",
        source_year=1988,
        quanfina_context="Piyasa Durumu sayfasında sayaç olarak gösterilir. >4 ise sarı uyarı.",
        category="technical",
    ),
    Term(
        key="trend_template",
        short_name="Trend Template",
        tooltip="Minervini'nin 8 koşullu hisse filtresi — tüm koşullar sağlanmalı.",
        definition=(
            "Mark Minervini'nin Trend Template'i 8 koşuldan oluşur: "
            "(1) Fiyat MA150 ve MA200 üstünde, (2) MA150 > MA200, "
            "(3) MA200 en az 1 aydır yükseliş eğiminde, (4) MA50 > MA150 ve MA200, "
            "(5) Fiyat MA50 üstünde, (6) Fiyat 52 hafta düşüğünden %30+ yüksek, "
            "(7) Fiyat 52 hafta yükseklerinin %25 içinde, (8) RS > 70."
        ),
        source_book="Trade Like a Stock Market Wizard",
        source_author="Mark Minervini",
        source_year=2013,
        quanfina_context="Minervini tarama motorunun temel filtresi. Tüm hisseler Trend Template'den geçmeli.",
        category="strategy",
    ),
    Term(
        key="sepa",
        short_name="SEPA",
        tooltip="Tooltip henüz tanımlanmadı. NotebookLM'den detay alınacak.",
        definition="Tooltip henüz tanımlanmadı, NotebookLM'den detay alınacak.",
        source_book=None,
        source_author="Mark Minervini",
        source_year=None,
        quanfina_context="Minervini metodolojisi kapsamında. Detay NotebookLM NB-1'den alınacak.",
        category="strategy",
    ),
    Term(
        key="canslim",
        short_name="CANSLIM",
        tooltip="O'Neil'in 7 koşullu hisse seçim metodolojisi — C-A-N-S-L-I-M kısaltması.",
        definition=(
            "William J. O'Neil'in CANSLIM metodolojisi: "
            "C = Current Earnings (mevcut çeyrek kazanç büyümesi %25+), "
            "A = Annual Earnings (yıllık kazanç büyümesi %25+), "
            "N = New (yeni ürün/hizmet/yönetim veya yeni fiyat zirvesi), "
            "S = Supply/Demand (az free float, kurumsal alım hacmi), "
            "L = Leader (RS > 80, sektör lideri), "
            "I = Institutional Sponsorship (kurumsal sahiplik artışı), "
            "M = Market Direction (genel piyasa yükseliş trendinde)."
        ),
        source_book="How to Make Money in Stocks",
        source_author="William J. O'Neil",
        source_year=1988,
        quanfina_context="Minervini ve IBD metodolojisinin temeli. EPS/Sales kriterleri buradan.",
        category="strategy",
    ),
    Term(
        key="pivot_price",
        short_name="Pivot Fiyatı",
        tooltip="Konsolidasyon bazından kırılım seviyesi — bu fiyat üstünde alım yapılır.",
        definition=(
            "Minervini ve IBD terminolojisinde pivot fiyat: Hissenin konsolidasyon "
            "bazının (örn. VCP, cup-with-handle) en yüksek noktası. Pivot fiyat "
            "üzerinde yüksek hacimle kapanış = kırılım onayı ve alım noktası."
        ),
        source_book="Trade Like a Stock Market Wizard",
        source_author="Mark Minervini",
        source_year=2013,
        quanfina_context="Minervini grid'inde opsiyonel sütun. Null ise henüz baz oluşmamış.",
        category="technical",
    ),
    Term(
        key="high_52w",
        short_name="52 Hafta Yüksek",
        tooltip="Son 52 haftanın en yüksek kapanış fiyatı.",
        definition=(
            "Bir hissenin son 52 haftalık (yaklaşık 1 yıl) en yüksek kapanış fiyatı. "
            "Minervini Trend Template koşulu: fiyat 52 hafta yükseklerinin %25 içinde "
            "olmalı (pct_from_high > -25%)."
        ),
        source_book=None,
        source_author=None,
        source_year=None,
        quanfina_context="Minervini grid 52H MESAFE sütunu: mevcut fiyatın 52 hafta yüksekten uzaklığı.",
        category="technical",
    ),
    Term(
        key="atr",
        short_name="ATR",
        tooltip="Average True Range — volatilite ölçüsü. Stop ve pozisyon büyüklüğünde kullanılır.",
        definition=(
            "Average True Range (ATR). J. Welles Wilder tarafından geliştirildi. "
            "Belirli bir periyottaki (genellikle 14 gün) ortalama fiyat aralığını ölçer. "
            "Yüksek ATR = yüksek volatilite. Stop loss ve pozisyon büyüklüğü "
            "hesaplamalarında temel gösterge."
        ),
        source_book="New Concepts in Technical Trading Systems",
        source_author="J. Welles Wilder",
        source_year=1978,
        quanfina_context="Risk hesaplama modülünde stop mesafesi ve pozisyon büyüklüğü için kullanılır.",
        category="risk",
    ),
    Term(
        key="vix",
        short_name="VIX",
        tooltip="CBOE Volatilite Endeksi. <15 = sakin, >30 = korku bölgesi.",
        definition=(
            "CBOE Volatility Index (VIX). S&P 500 opsiyonlarından hesaplanan, "
            "piyasanın önümüzdeki 30 gün için beklediği volatilite seviyesi. "
            "VIX < 15: düşük volatilite/güven ortamı. "
            "VIX 15–25: normal. VIX > 30: korku bölgesi, satış baskısı yüksek."
        ),
        source_book=None,
        source_author="CBOE",
        source_year=1993,
        quanfina_context="Piyasa Durumu sayfasında gösterge. Yüksek VIX = LONG pozisyon boyutunu küçült.",
        category="technical",
    ),
    Term(
        key="market_breadth",
        short_name="Piyasa Genişliği",
        tooltip="Piyasada yükselen/düşen hisse oranı — piyasa sağlığını ölçer.",
        definition=(
            "Market Breadth (Piyasa Genişliği): Piyasada yükselen hisse sayısının "
            "düşene oranı, 52 hafta yeni yüksekler/düşükler, NYSE A/D Line gibi "
            "göstergeler. Endeks yükselirken breadth zayıfsa = divergence uyarısı."
        ),
        source_book=None,
        source_author=None,
        source_year=None,
        quanfina_context="Piyasa Durumu sayfasında Market Health Score'un bileşenlerinden biri. Tooltip henüz NotebookLM'den detaylandırılacak.",
        category="technical",
    ),
    Term(
        key="minervini_method",
        short_name="Minervini Stratejisi",
        tooltip="Tooltip henüz tanımlanmadı. NotebookLM'den detay alınacak.",
        definition="Tooltip henüz tanımlanmadı, NotebookLM'den detay alınacak.",
        source_book=None,
        source_author="Mark Minervini",
        source_year=None,
        quanfina_context="Watchlist sayfasında Minervini stratejisi satırlarına bağlı terim. Detay NB-1'den alınacak.",
        category="strategy",
    ),
    Term(
        key="carr_method",
        short_name="Carr Stratejisi",
        tooltip="Tooltip henüz tanımlanmadı. NotebookLM'den detay alınacak.",
        definition="Tooltip henüz tanımlanmadı, NotebookLM'den detay alınacak.",
        source_book="Trend Trading for a Living",
        source_author="Thomas Carr",
        source_year=2008,
        quanfina_context="Watchlist sayfasında Carr stratejisi satırlarına bağlı terim. Detay NB-2'den alınacak.",
        category="strategy",
    ),
    Term(
        key="watchlist_status",
        short_name="Watchlist Statüsü",
        tooltip="Watch → On Deck → Focus → Buy — 4 aşamalı hisse öncelik sıralaması.",
        definition=(
            "Quanfina Watchlist sistemi 4 statü: "
            "Watch (izlemede, henüz koşul sağlanmadı), "
            "On Deck (hazır beklemede, yakında setup oluşabilir), "
            "Focus (odak listesi, setup aktif), "
            "Buy (alım bölgesi, pivot kırıldı veya kırılmak üzere). "
            "Hisse olgunlaştıkça statü yükselir."
        ),
        source_book=None,
        source_author=None,
        source_year=None,
        quanfina_context="Watchlist sayfasında her satırın öncelik durumu. Renk: Buy=yeşil, Focus=mavi, On Deck=turuncu, Watch=gri.",
        category="strategy",
    ),
    Term(
        key="consensus",
        short_name="Konsensus",
        tooltip="Kaç farklı stratejide aynı hisse var — yüksek = daha güçlü sinyal.",
        definition=(
            "Quanfina konsensus skoru: Bir hissenin kaç ayrı stratejide "
            "(Minervini, Carr vb.) watchlist'te yer aldığını gösterir. "
            "Konsensus=1: tek stratejide var. "
            "Konsensus=2: hem Minervini hem Carr'da var — çakışan onay. "
            "Yüksek konsensus, hisse hakkında birden fazla bağımsız sinyal oluştuğunu gösterir."
        ),
        source_book=None,
        source_author=None,
        source_year=None,
        quanfina_context="Watchlist sayfasında KONSENSUS kolonu. Default sort: konsensus DESC. Filtre: 1+ / 2+ / 3+.",
        category="strategy",
    ),
    Term(
        key="ma50",
        short_name="MA50",
        tooltip="50 günlük hareketli ortalama — kısa vadeli trend desteği.",
        definition=(
            "50 günlük basit hareketli ortalama (SMA50). Son 50 kapanış fiyatının "
            "aritmetik ortalaması. Minervini Trend Template koşullarından biri: "
            "fiyat MA50 üzerinde olmalı. Destek veya direnç olarak da işlev görür. "
            "MA50 > MA150 > MA200 sırası sağlıklı bir yükseliş trendi gösterir."
        ),
        source_book="Trade Like a Stock Market Wizard",
        source_author="Mark Minervini",
        source_year=2013,
        quanfina_context="Hisse detay sayfasında candlestick grafik üzerine sarı çizgi olarak çizilir.",
        category="technical",
    ),
    Term(
        key="ma200",
        short_name="MA200",
        tooltip="200 günlük hareketli ortalama — uzun vadeli trend çizgisi.",
        definition=(
            "200 günlük basit hareketli ortalama (SMA200). Son 200 kapanış fiyatının "
            "aritmetik ortalaması. Kurumsal yatırımcıların en çok izlediği uzun vadeli "
            "trend göstergesi. Minervini Trend Template: fiyat MA200 üzerinde olmalı ve "
            "MA200 en az 1 aydır pozitif eğimde olmalı. "
            "MA200 altına düşüş genellikle güçlü satış sinyali olarak değerlendirilir."
        ),
        source_book="Trade Like a Stock Market Wizard",
        source_author="Mark Minervini",
        source_year=2013,
        quanfina_context="Hisse detay sayfasında candlestick grafik üzerine mor çizgi olarak çizilir. Eğim hesabı MA200 Eğim sütununda ayrıca gösterilir.",
        category="technical",
    ),
    Term(
        key="candlestick",
        short_name="Candlestick",
        tooltip="Mum grafik — her bar açılış, yüksek, düşük ve kapanış fiyatını gösterir.",
        definition=(
            "Candlestick (mum) grafik: Her zaman dilimi (gün, hafta vb.) için dört fiyat "
            "noktasını gösterir. Gövde: açılış ve kapanış arası. Fitil (wick): yüksek ve "
            "düşük fiyat. Yeşil/boş gövde = kapanış > açılış (yükseldi). "
            "Kırmızı/dolu gövde = kapanış < açılış (düştü). "
            "Japon pirinç tüccarlarından 17. yüzyılda gelen teknik analiz aracı."
        ),
        source_book=None,
        source_author=None,
        source_year=None,
        quanfina_context="Hisse detay sayfasında ana fiyat grafiği candlestick formatta gösterilir. TradingView Lightweight Charts v5 kullanılır.",
        category="technical",
    ),
    Term(
        key="promote_status",
        short_name="Statü Yükseltme",
        tooltip="Watch → On Deck → Focus → Buy — hisse olgunlaştıkça statü yükselir.",
        definition=(
            "Quanfina watchlist statü hiyerarşisi: Watch (izlemede) → On Deck (hazır beklemede) → "
            "Focus (odak listesi, setup aktif) → Buy (alım bölgesi, pivot kırıldı). "
            "Hisse analiz açısından olgunlaştıkça statüsü yükseltilir; zayıflayınca düşürülür."
        ),
        source_book=None,
        source_author=None,
        source_year=None,
        quanfina_context="Watchlist satır eylemleri menüsünde Yükselt/Düşür butonları. Watch→Buy tek yönlü hiyerarşi.",
        category="strategy",
    ),
    Term(
        key="rba",
        short_name="RBA",
        tooltip="Rule-Based Analysis — Minervini'nin kural tabanlı trade değerlendirme sistemi.",
        definition=(
            "Tooltip henüz tanımlanmadı, NotebookLM'den detay alınacak. "
            "Mark Minervini'nin Rule-Based Analysis (RBA) sistemi, her trade'in kurallarını "
            "ve bu kurallara uyumu analiz eder. Giriş, çıkış ve pozisyon yönetiminin "
            "her adımı not edilir."
        ),
        source_book="Trade Like a Stock Market Wizard",
        source_author="Mark Minervini",
        source_year=2013,
        quanfina_context="Trade Journal sayfasında grade ve ders notları RBA metodolojisine dayanır. POC ADIM 10'da detaylandırılacak.",
        category="strategy",
    ),
    Term(
        key="setup_type",
        short_name="Setup Tipi",
        tooltip="Fiyat formasyonunun türü — VCP, Pivot, Pocket Pivot vb.",
        definition=(
            "Trade journal'daki setup tipi, giriş yapılan fiyat formasyonunu tanımlar. "
            "Minervini 6 setup: VCP, Pivot, Pocket Pivot, Power Play (High Tight Flag), "
            "Cup & Handle, Flat Base. Carr 2 setup: Pullback, Coiled Spring. "
            "Aynı setup'ın tekrarlayan başarısı veya başarısızlığı öğrenme sağlar."
        ),
        source_book=None,
        source_author=None,
        source_year=None,
        quanfina_context="Trade Journal'da setup kolonunda görünür. 8 standart tip dropdown'dan seçilir.",
        category="strategy",
    ),
    Term(
        key="exit_reason",
        short_name="Çıkış Sebebi",
        tooltip="Trade neden kapatıldı — stop loss, hedef, trailing stop veya takdiri.",
        definition=(
            "Trade kapatma sebepleri: Stop Loss (önceden belirlenen risk noktası tetiklendi), "
            "Hedef Ulaşıldı (fiyat hedefine ulaşıldı), Trailing Stop (sürüklenen stop tetiklendi), "
            "Takdiri (koşullar değişti, kuralsız çıkış), Süre Çıkışı (beklenen hareket gelmedi). "
            "Çıkış sebebini takip etmek disiplini ölçer."
        ),
        source_book=None,
        source_author=None,
        source_year=None,
        quanfina_context="Trade Journal'da çıkış sebebi kolonunda görünür. Stop loss oranı yüksekse setup kalitesi düşük demektir.",
        category="risk",
    ),
    Term(
        key="pl_pct",
        short_name="P/L %",
        tooltip="Trade kar/zarar yüzdesi — (çıkış - giriş) / giriş × 100.",
        definition=(
            "Trade kar veya zarar yüzdesi: (çıkış fiyatı - giriş fiyatı) / giriş fiyatı × 100. "
            "Pozitif = kârlı trade, negatif = zararlı trade. "
            "Decimal.js ile hesaplanır (kayan nokta hatası yok). "
            "Başarılı Minervini trade'leri genellikle %20-50 P/L hedefler."
        ),
        source_book=None,
        source_author=None,
        source_year=None,
        quanfina_context="Trade Journal P/L % kolonunda gösterilir. Yeşil = kâr, kırmızı = zarar.",
        category="risk",
    ),
    Term(
        key="trade_grade",
        short_name="Trade Grade",
        tooltip="Trade kalite notu: A+ (mükemmel) → F (başarısız) — süreç, sonuç değil.",
        definition=(
            "Mark Minervini'nin trade grading sistemi: A+ (her kurala uyuldu, mükemmel süreç), "
            "A (küçük sapmalar), B (iyi ama hatalar var), C (birçok kural ihlali), "
            "D (kötü süreç), F (tamamen kuralsız). "
            "Not: Grade sonucu değil süreci ölçer. Zararlı A+ trade mümkün (kurallara uyuldu, "
            "piyasa döndü). Kârlı F trade mümkün (şans, kural ihlaliyle)."
        ),
        source_book="Trade Like a Stock Market Wizard",
        source_author="Mark Minervini",
        source_year=2013,
        quanfina_context="Trade Journal'da grade kolonunda renkli badge olarak gösterilir. A+=koyu yeşil, F=kırmızı.",
        category="strategy",
    ),
    Term(
        key="signal",
        short_name="Sinyal",
        tooltip="Konsensus sinyal — birden fazla stratejide aynı hissenin watchlist'e girmesi.",
        definition=(
            "Quanfina sinyal: Bir hissenin en az bir stratejide (Minervini veya Carr) "
            "watchlist'e alınmasıyla oluşan eylem önerisi. Konsensus sinyal ise hissenin "
            "birden fazla stratejide (örn. hem Minervini hem Carr) watchlist'te yer alması. "
            "Konsensus ne kadar yüksekse sinyal gücü o kadar yüksek kabul edilir."
        ),
        source_book=None,
        source_author=None,
        source_year=None,
        quanfina_context="Sinyaller sayfasında SignalCard olarak gösterilir. Konsensus rozeti 2/2 veya 1/2 gibi.",
        category="strategy",
    ),
    Term(
        key="new_today",
        short_name="Yeni Bugün",
        tooltip="Son 24 saat içinde watchlist'e eklenen sinyal.",
        definition=(
            "Sinyaller sayfasında 'Yeni Bugün' filtresi: added_date değeri bugünün tarihi "
            "olan watchlist girişlerine dayalı sinyalleri gösterir. "
            "Sabah rutininde 'dün gece veya bugün sabah ne eklendi?' sorusunu yanıtlar."
        ),
        source_book=None,
        source_author=None,
        source_year=None,
        quanfina_context="Sinyaller sayfasında yeşil 'YENİ BUGÜN' badge olarak görünür. Checkbox ile filtrelenebilir.",
        category="strategy",
    ),
]

_TERMS_BY_KEY: dict[str, Term] = {t.key: t for t in MOCK_TERMS}


@app.get("/api/terms", response_model=list[Term])
def get_terms() -> list[Term]:
    return MOCK_TERMS


@app.get("/api/terms/{key}", response_model=Term)
def get_term(key: str) -> Term:
    term = _TERMS_BY_KEY.get(key)
    if not term:
        raise HTTPException(status_code=404, detail=f"Term '{key}' not found")
    return term


# ── Piyasa Durumu ──────────────────────────────────────────────────────────────

class SectorChange(BaseModel):
    name: str
    change_pct: float


class MarketStatus(BaseModel):
    spy_stage: int
    qqq_stage: int
    iwm_stage: int
    vix: float
    distribution_days: int
    market_health_score: int
    market_health_label: str
    suggested_mode: str
    top_sectors: list[SectorChange]
    bottom_sectors: list[SectorChange]


MOCK_MARKET_STATUS = MarketStatus(
    spy_stage=2,
    qqq_stage=2,
    iwm_stage=1,
    vix=14.2,
    distribution_days=3,
    market_health_score=75,
    market_health_label="YEŞİL",
    suggested_mode="LONG",
    top_sectors=[
        SectorChange(name="Technology", change_pct=2.3),
        SectorChange(name="Energy", change_pct=1.8),
        SectorChange(name="Industrials", change_pct=1.2),
    ],
    bottom_sectors=[
        SectorChange(name="Utilities", change_pct=-1.2),
        SectorChange(name="Health Care", change_pct=-0.4),
    ],
)


@app.get("/api/market/status", response_model=MarketStatus)
def get_market_status() -> MarketStatus:
    return MOCK_MARKET_STATUS


# ─── ABD Borsa Takvim Status (Sprint 4-bis.7, 22 May 2026) ───────────────────
# Sn. Ferit talimat: "veri çekme saati ABD borsa saatleri ABD tatiller veri
# çekme sistemini geliştirelim Türkiye'de yaşadığımı unutma."
# market_calendar.py utility — TR + ET saatleri, tatil tespiti, sonraki açılış.

class MarketCalendarStatus(BaseModel):
    is_open: bool                  # Ana seans (9:30-16:00 ET) açık mı?
    session: str                   # "regular" | "pre_market" | "post_market" | "closed"
    reason: Optional[str] = None   # Kapalıysa sebep (Türkçe)
    is_early_close: bool = False   # Yarı gün mü (13:00 ET kapanış)?
    now_et: str                    # "2026-05-22 09:32 EDT"
    now_tr: str                    # "2026-05-22 16:32 +03"
    next_open_et: str              # "2026-05-26 09:30 EDT"
    next_open_tr: str              # "2026-05-26 16:30 +03"
    last_trading_day: str          # "2026-05-22" (ISO date)


@app.get("/api/market/calendar/status", response_model=MarketCalendarStatus)
def get_market_calendar_status() -> MarketCalendarStatus:
    """
    ABD borsa takvim durumu — hafta sonu/tatil/yarı gün tespiti ile TR + ET saatleri.
    Frontend'de MarketStatusBadge bu endpoint'i poll eder (30 sn refresh).
    """
    try:
        # Repo root'a path ekle (api/ alt klasör, market_calendar.py kök'te)
        import sys
        from pathlib import Path
        repo_root = Path(__file__).parent.parent
        if str(repo_root) not in sys.path:
            sys.path.insert(0, str(repo_root))

        from market_calendar import market_status_now
        s = market_status_now()
        return MarketCalendarStatus(
            is_open=s.is_open,
            session=s.session,
            reason=s.reason,
            is_early_close=s.is_early_close,
            now_et=s.now_et.strftime("%Y-%m-%d %H:%M %Z"),
            now_tr=s.now_tr.strftime("%Y-%m-%d %H:%M %Z"),
            next_open_et=s.next_open_et.strftime("%Y-%m-%d %H:%M %Z"),
            next_open_tr=s.next_open_tr.strftime("%Y-%m-%d %H:%M %Z"),
            last_trading_day=s.last_trading_day.isoformat(),
        )
    except Exception as e:
        from datetime import datetime, timezone
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        return MarketCalendarStatus(
            is_open=False,
            session="closed",
            reason=f"Takvim modülü hata: {e!s}",
            now_et=now_iso,
            now_tr=now_iso,
            next_open_et=now_iso,
            next_open_tr=now_iso,
            last_trading_day=datetime.now(timezone.utc).date().isoformat(),
        )


# ── Watchlist ────────────────────────────────────────────────────────────────

class WatchlistRow(BaseModel):
    symbol: str
    strategy: str                      # "minervini" | "carr"
    status: str                        # "watch" | "on_deck" | "focus" | "buy"
    price: float
    added_date: str                    # ISO date string
    setup_type: Optional[str] = None
    pivot_price: Optional[float] = None
    note: Optional[str] = None
    rs_rating: int                     # 0–99
    consensus_count: int
    consensus_strategies: list[str]


@app.get("/api/watchlist", response_model=list[WatchlistRow])
def get_watchlist() -> list[WatchlistRow]:
    # DB unreachable -> MOCK fallback (Sinyaller pateni — Kural #20 UX)
    # Sn. Ferit: DB down ortamda UI demo dolu görünsün, banner sadece API'da
    if not db_health_check():
        return [
            WatchlistRow(symbol="NVDA",  strategy="minervini", status="buy",     price=145.20,  added_date=f"{date.today().isoformat()} 09:32", setup_type="VCP",                pivot_price=148.00,  rs_rating=99, consensus_count=2, consensus_strategies=["minervini","carr"]),
            WatchlistRow(symbol="NVDA",  strategy="carr",      status="focus",   price=145.20,  added_date="2026-05-19 14:15", setup_type="Pullback",           pivot_price=147.50,  rs_rating=99, consensus_count=2, consensus_strategies=["minervini","carr"]),
            WatchlistRow(symbol="MSFT",  strategy="minervini", status="buy",     price=425.30,  added_date="2026-05-19 11:23", setup_type="Tight Low Vol",      pivot_price=430.00,  rs_rating=91, consensus_count=2, consensus_strategies=["minervini","carr"]),
            WatchlistRow(symbol="MSFT",  strategy="carr",      status="watch",   price=425.30,  added_date="2026-05-17 15:58", setup_type="Coiled Spring",      pivot_price=428.00,  rs_rating=91, consensus_count=2, consensus_strategies=["minervini","carr"]),
            WatchlistRow(symbol="GOOGL", strategy="carr",      status="buy",     price=178.40,  added_date="2026-05-19 13:04", setup_type="Bullish Divergence", pivot_price=180.00,  rs_rating=88, consensus_count=1, consensus_strategies=["carr"]),
            WatchlistRow(symbol="AAPL",  strategy="minervini", status="focus",   price=212.50,  added_date="2026-05-18 10:47", setup_type="Power Play",         pivot_price=215.00,  rs_rating=87, consensus_count=1, consensus_strategies=["minervini"]),
            WatchlistRow(symbol="AMD",   strategy="minervini", status="on_deck", price=158.20,  added_date="2026-05-18 16:42", setup_type="Inside Day",         pivot_price=160.00,  rs_rating=85, consensus_count=1, consensus_strategies=["minervini"]),
            WatchlistRow(symbol="TSLA",  strategy="minervini", status="focus",   price=245.60,  added_date="2026-05-19 09:51", setup_type="Tight Low Vol",      pivot_price=250.00,  rs_rating=82, consensus_count=1, consensus_strategies=["minervini"]),
            WatchlistRow(symbol="META",  strategy="carr",      status="watch",   price=512.80,  added_date="2026-05-16 12:18", setup_type="Pullback",           pivot_price=518.00,  rs_rating=80, consensus_count=1, consensus_strategies=["carr"]),
            WatchlistRow(symbol="AVGO",  strategy="minervini", status="buy",     price=1450.30, added_date="2026-05-19 10:09", setup_type="VCP",                pivot_price=1460.00, rs_rating=78, consensus_count=1, consensus_strategies=["minervini"]),
        ]

    try:
        return [WatchlistRow(**r) for r in watchlist_get_all()]
    except OperationalError as e:
        # Cloud SQL paused / IP whitelist eski / network problemi
        raise HTTPException(
            status_code=503,
            detail="Veritabanına ulaşılamıyor (Cloud SQL). GCP Console → SQL → instance durum/Authorized Networks kontrol et."
        ) from e


# ── Watchlist CRUD helpers ────────────────────────────────────────────────────

_STATUS_HIERARCHY = ["watch", "on_deck", "focus", "buy"]


def _mock_rs(symbol: str) -> int:
    stock = _STOCK_BY_SYM.get(symbol)
    if stock:
        return int(stock.rs_ibd)
    seed = sum(ord(c) for c in symbol)
    return 60 + (seed % 31)


def _mock_price(symbol: str) -> float:
    stock = _STOCK_BY_SYM.get(symbol)
    if stock:
        return stock.price
    existing = [r for r in watchlist_get_all() if r["symbol"] == symbol]
    if existing:
        return float(existing[0]["price"])
    seed = sum(ord(c) * (i + 1) for i, c in enumerate(symbol))
    return round(20.0 + (seed % 500) * 1.5, 2)


def _promote_status(current: str) -> str:
    idx = _STATUS_HIERARCHY.index(current) if current in _STATUS_HIERARCHY else -1
    if idx == -1 or idx == len(_STATUS_HIERARCHY) - 1:
        return current
    return _STATUS_HIERARCHY[idx + 1]


# ── Watchlist mutation models ─────────────────────────────────────────────────

class WatchlistRowCreate(BaseModel):
    symbol: str
    strategy: Literal["minervini", "carr"]
    status: Literal["watch", "on_deck", "focus", "buy"]
    setup_type: Optional[str] = None
    pivot_price: Optional[float] = None
    note: Optional[str] = None


class WatchlistRowUpdate(BaseModel):
    status: Optional[Literal["watch", "on_deck", "focus", "buy"]] = None
    note: Optional[str] = None
    setup_type: Optional[str] = None


# ── Watchlist CRUD endpoints ──────────────────────────────────────────────────

@app.post("/api/watchlist", response_model=WatchlistRow, status_code=201)
def add_watchlist_row(body: WatchlistRowCreate) -> WatchlistRow:
    sym = body.symbol.strip().upper()
    if watchlist_exists(sym, body.strategy):
        raise HTTPException(
            status_code=409, detail=f"{sym}-{body.strategy} zaten watchlist'te"
        )
    row_data = {
        "symbol": sym,
        "strategy": body.strategy,
        "status": body.status,
        "price": _mock_price(sym),
        "added_date": date.today().isoformat(),
        "setup_type": body.setup_type,
        "pivot_price": body.pivot_price,
        "note": body.note,
        "rs_rating": _mock_rs(sym),
        "consensus_count": 1,
        "consensus_strategies": [body.strategy],
    }
    watchlist_insert(row_data)
    watchlist_recompute_consensus()
    return WatchlistRow(**watchlist_get_one(sym, body.strategy))


@app.patch("/api/watchlist/{symbol}/{strategy}", response_model=WatchlistRow)
def update_watchlist_row(symbol: str, strategy: str, body: WatchlistRowUpdate) -> WatchlistRow:
    sym = symbol.upper()
    if not watchlist_exists(sym, strategy):
        raise HTTPException(status_code=404, detail=f"{sym}-{strategy} bulunamadı")
    updates = {k: v for k, v in body.model_dump(include=body.model_fields_set).items()}
    watchlist_update(sym, strategy, updates)
    return WatchlistRow(**watchlist_get_one(sym, strategy))


@app.delete("/api/watchlist/{symbol}/{strategy}", status_code=204)
def delete_watchlist_row(symbol: str, strategy: str) -> Response:
    sym = symbol.upper()
    if not watchlist_delete(sym, strategy):
        raise HTTPException(status_code=404, detail=f"{sym}-{strategy} bulunamadı")
    watchlist_recompute_consensus()
    return Response(status_code=204)


@app.post("/api/watchlist/{symbol}/{strategy}/promote", response_model=WatchlistRow)
def promote_watchlist_row(symbol: str, strategy: str) -> WatchlistRow:
    sym = symbol.upper()
    row = watchlist_get_one(sym, strategy)
    if not row:
        raise HTTPException(status_code=404, detail=f"{sym}-{strategy} bulunamadı")
    new_status = _promote_status(row["status"])
    watchlist_update(sym, strategy, {"status": new_status})
    return WatchlistRow(**watchlist_get_one(sym, strategy))


# ── Hisse Detay ─────────────────────────────────────────────────────────────

_STOCK_META: dict[str, dict] = {
    "NVDA": {"name": "NVIDIA Corp",            "industry": "Semiconductors",          "market_cap": "$2.2T"},
    "AVGO": {"name": "Broadcom Inc",           "industry": "Semiconductors",          "market_cap": "$780B"},
    "META": {"name": "Meta Platforms Inc",     "industry": "Social Media",            "market_cap": "$1.3T"},
    "MSFT": {"name": "Microsoft Corp",         "industry": "Software",                "market_cap": "$3.1T"},
    "COST": {"name": "Costco Wholesale Corp",  "industry": "Retail",                  "market_cap": "$395B"},
    "GOOGL":{"name": "Alphabet Inc Class A",   "industry": "Internet Services",       "market_cap": "$2.2T"},
    "ASML": {"name": "ASML Holding NV",        "industry": "Semiconductor Equipment", "market_cap": "$292B"},
    "AMZN": {"name": "Amazon.com Inc",         "industry": "E-Commerce / Cloud",      "market_cap": "$2.1T"},
    "ORCL": {"name": "Oracle Corp",            "industry": "Enterprise Software",     "market_cap": "$412B"},
    "CRM":  {"name": "Salesforce Inc",         "industry": "CRM Software",            "market_cap": "$286B"},
    "NFLX": {"name": "Netflix Inc",            "industry": "Streaming",               "market_cap": "$288B"},
    "AMD":  {"name": "Advanced Micro Devices", "industry": "Semiconductors",          "market_cap": "$257B"},
    "ADBE": {"name": "Adobe Inc",              "industry": "Creative Software",       "market_cap": "$174B"},
    "V":    {"name": "Visa Inc",               "industry": "Payment Processing",      "market_cap": "$544B"},
    "MA":   {"name": "Mastercard Inc",         "industry": "Payment Processing",      "market_cap": "$440B"},
    "JPM":  {"name": "JPMorgan Chase & Co",    "industry": "Banking",                 "market_cap": "$618B"},
    "HD":   {"name": "Home Depot Inc",         "industry": "Home Improvement",        "market_cap": "$346B"},
    "WMT":  {"name": "Walmart Inc",            "industry": "Retail",                  "market_cap": "$720B"},
    "AMAT": {"name": "Applied Materials Inc",  "industry": "Semiconductor Equipment", "market_cap": "$149B"},
    "MRVL": {"name": "Marvell Technology Inc", "industry": "Semiconductors",          "market_cap": "$53B"},
    "AAPL": {"name": "Apple Inc",              "industry": "Consumer Electronics",    "market_cap": "$2.8T", "sector": "Technology"},
    "TSM":  {"name": "Taiwan Semiconductor",   "industry": "Semiconductors",          "market_cap": "$740B", "sector": "Technology"},
    "LLY":  {"name": "Eli Lilly and Company",  "industry": "Pharmaceuticals",         "market_cap": "$680B", "sector": "Health Care"},
    "UBER": {"name": "Uber Technologies Inc",  "industry": "Ride-Hailing",            "market_cap": "$140B", "sector": "Technology"},
    "PLTR": {"name": "Palantir Technologies",  "industry": "Software",                "market_cap": "$49B",  "sector": "Technology"},
    "COIN": {"name": "Coinbase Global Inc",    "industry": "Crypto Exchange",         "market_cap": "$38B",  "sector": "Financials"},
    "DASH": {"name": "DoorDash Inc",           "industry": "Food Delivery",           "market_cap": "$47B",  "sector": "Consumer Discretionary"},
    "SHOP": {"name": "Shopify Inc",            "industry": "E-Commerce",              "market_cap": "$96B",  "sector": "Technology"},
}

_STOCK_BY_SYM: dict[str, MinerviniStock] = {s.symbol: s for s in MOCK_STOCKS}


class StockInfo(BaseModel):
    symbol: str
    name: str
    sector: str
    industry: str
    market_cap: str
    price: float
    change_pct: float
    rs_rating: int
    active_strategies: list[WatchlistRow]


class OhlcvBar(BaseModel):
    time: str         # ISO date "YYYY-MM-DD"
    open: float
    high: float
    low: float
    close: float
    volume: int


def _generate_ohlcv(symbol: str, end_price: float, n_bars: int = 252) -> list[OhlcvBar]:
    """Deterministic OHLCV generation — seed per symbol, Minervini-appropriate uptrend."""
    rng = random.Random(sum(ord(c) for c in symbol) * 7919)

    # Collect n_bars trading days (Mon–Fri) backwards from yesterday
    trading_days: list[date] = []
    d = date.today() - timedelta(days=1)
    while len(trading_days) < n_bars:
        if d.weekday() < 5:
            trading_days.append(d)
        d -= timedelta(days=1)
    trading_days.reverse()   # chronological order

    # Price series: start ~22% lower, slight uptrend drift
    start_price = end_price * rng.uniform(0.73, 0.82)
    prices = [start_price]
    for _ in range(n_bars - 1):
        prices.append(prices[-1] * (1.0 + rng.gauss(0.0009, 0.014)))

    # Scale to match end_price exactly
    scale = end_price / prices[-1]
    prices = [p * scale for p in prices]

    bars: list[OhlcvBar] = []
    for bar_date, close in zip(trading_days, prices):
        daily_range = rng.uniform(0.006, 0.022)
        high = close * (1.0 + rng.uniform(daily_range * 0.4, daily_range))
        low  = close * (1.0 - rng.uniform(daily_range * 0.4, daily_range))
        open_ = low + rng.random() * (high - low)
        high = max(high, open_, close)
        low  = min(low,  open_, close)
        bars.append(OhlcvBar(
            time=bar_date.isoformat(),
            open=round(open_, 2),
            high=round(high, 2),
            low=round(low, 2),
            close=round(close, 2),
            volume=int(rng.uniform(8_000_000, 55_000_000)),
        ))
    return bars


@app.get("/api/stock/{symbol}/info", response_model=StockInfo)
def get_stock_info(symbol: str) -> StockInfo:
    sym = symbol.upper()
    stock = _STOCK_BY_SYM.get(sym)
    meta  = _STOCK_META.get(sym, {})
    # DB down ortamda hisse detay sayfasi calismali — watchlist bos liste fallback
    try:
        active = [WatchlistRow(**r) for r in watchlist_get_all() if r["symbol"] == sym]
    except OperationalError:
        active = []

    if not stock and not active:
        raise HTTPException(status_code=404, detail=f"Symbol '{sym}' not found")

    if stock:
        return StockInfo(
            symbol=sym,
            name=meta.get("name", stock.company),
            sector=meta.get("sector", stock.sector),
            industry=meta.get("industry", stock.sector),
            market_cap=meta.get("market_cap", f"${stock.market_cap:.0f}B"),
            price=stock.price,
            change_pct=stock.change_pct,
            rs_rating=int(stock.rs_ibd),
            active_strategies=active,
        )
    else:
        row = active[0]
        return StockInfo(
            symbol=sym,
            name=meta.get("name", sym),
            sector=meta.get("sector", "—"),
            industry=meta.get("industry", "—"),
            market_cap=meta.get("market_cap", "—"),
            price=row.price,
            change_pct=0.0,
            rs_rating=row.rs_rating,
            active_strategies=active,
        )


@app.get("/api/stock/{symbol}/ohlcv", response_model=list[OhlcvBar])
def get_stock_ohlcv(symbol: str) -> list[OhlcvBar]:
    sym = symbol.upper()
    stock = _STOCK_BY_SYM.get(sym)
    if stock:
        price = stock.price
    else:
        try:
            wl = [r for r in watchlist_get_all() if r["symbol"] == sym]
        except OperationalError:
            wl = []
        if not wl:
            raise HTTPException(status_code=404, detail=f"Symbol '{sym}' not found")
        price = float(wl[0]["price"])
    return _generate_ohlcv(sym, price)


# ── Setup Types ───────────────────────────────────────────────────────────────

class SetupTypeModel(BaseModel):
    key: str
    label: str
    description: str


SETUP_TYPES: list[SetupTypeModel] = [
    SetupTypeModel(key="vcp",           label="VCP",          description="Volatility Contraction Pattern — Minervini"),
    SetupTypeModel(key="pivot",         label="Pivot",        description="Konsolidasyondan kırılım noktası"),
    SetupTypeModel(key="pocket_pivot",  label="Pocket Pivot", description="Kısmen gizli kırılım — Gill/Morales"),
    SetupTypeModel(key="power_play",    label="Power Play",   description="High Tight Flag — güçlü momentum"),
    SetupTypeModel(key="cup_and_handle",label="Cup & Handle", description="O'Neil klasik cup-and-handle formasyonu"),
    SetupTypeModel(key="flat_base",     label="Flat Base",    description="Dar bantlı konsolidasyon tabanı"),
    SetupTypeModel(key="pullback",      label="Pullback",     description="Yükseliş trendinde geri çekilme — Carr"),
    SetupTypeModel(key="coiled_spring", label="Coiled Spring",description="Sıkışma sonrası kırılım — Carr"),
]


@app.get("/api/setup-types", response_model=list[SetupTypeModel])
def get_setup_types() -> list[SetupTypeModel]:
    return SETUP_TYPES


# ── Trade Journal ─────────────────────────────────────────────────────────────

from decimal import Decimal as _D, ROUND_HALF_UP as _RHU


def _calc_pl(entry_price: float, exit_price: float, shares: int) -> tuple[float, float]:
    entry = _D(str(entry_price))
    exit_ = _D(str(exit_price))
    qty   = _D(str(shares))
    pl_dollar = float(((exit_ - entry) * qty).quantize(_D("0.01"), rounding=_RHU))
    pl_pct    = float(((exit_ - entry) / entry * 100).quantize(_D("0.01"), rounding=_RHU))
    return pl_dollar, pl_pct


class Trade(BaseModel):
    id: int
    symbol: str
    strategy: str
    setup_type: str
    # KARAR #477 (20 May 2026, UX Bölüm 7): Sinyal Kaynağı zorunlu.
    # Disiplin: trade kökeni izlenir, analiz kabiliyeti.
    signal_source: Optional[Literal["strategy", "manual_self", "manual_external"]] = None
    entry_date: str
    entry_price: float
    exit_date: Optional[str] = None
    exit_price: Optional[float] = None
    shares: int
    status: Literal["open", "closed"]
    pl_dollar: Optional[float] = None
    pl_pct: Optional[float] = None
    grade: Optional[str] = None
    exit_reason: Optional[str] = None
    lessons: Optional[str] = None


class TradeCreate(BaseModel):
    symbol: str
    strategy: str
    setup_type: str
    # KARAR #477: ZORUNLU (UX Bölüm 7) — trade kayıt formunda Sinyal Kaynağı default yok.
    # DB'de eski trades NULL kalabilir (geriye uyum), yeni kayıtlar zorunlu doldurur.
    signal_source: Literal["strategy", "manual_self", "manual_external"]
    entry_date: str
    entry_price: float
    shares: int
    status: Literal["open", "closed"] = "open"
    exit_date: Optional[str] = None
    exit_price: Optional[float] = None
    grade: Optional[str] = None
    exit_reason: Optional[str] = None
    lessons: Optional[str] = None


class TradeUpdate(BaseModel):
    exit_date: Optional[str] = None
    exit_price: Optional[float] = None
    status: Optional[Literal["open", "closed"]] = None
    grade: Optional[str] = None
    exit_reason: Optional[str] = None
    lessons: Optional[str] = None
    setup_type: Optional[str] = None


def _make_closed(
    id_: int, symbol: str, strategy: str, setup_type: str,
    entry_date: str, entry_price: float, shares: int,
    exit_date: str, exit_price: float,
    grade: str, exit_reason: str, lessons: Optional[str] = None,
) -> Trade:
    pld, plp = _calc_pl(entry_price, exit_price, shares)
    return Trade(
        id=id_, symbol=symbol, strategy=strategy, setup_type=setup_type,
        entry_date=entry_date, entry_price=entry_price,
        exit_date=exit_date, exit_price=exit_price,
        shares=shares, status="closed",
        pl_dollar=pld, pl_pct=plp,
        grade=grade, exit_reason=exit_reason, lessons=lessons,
    )


@app.get("/api/trades", response_model=list[Trade])
def get_trades() -> list[Trade]:
    # DB unreachable -> MOCK fallback (Sinyaller + Watchlist pateni — Kural #20 UX)
    # 8 trade: 4 açık + 4 kapalı, çeşitli grade/strateji/setup
    if not db_health_check():
        return [
            Trade(id=1, symbol="NVDA",  strategy="minervini", setup_type="VCP",                signal_source="strategy",        entry_date="2026-04-22 09:35", entry_price=132.50,  shares=100, status="closed", exit_date="2026-05-15 15:42", exit_price=145.80, pl_dollar=1330.00,  pl_pct=10.04, grade="A",  exit_reason="target",         lessons="VCP pivot kırılımı 3+ daralma sonrası temiz"),
            Trade(id=2, symbol="MSFT",  strategy="minervini", setup_type="Power Play",         signal_source="strategy",        entry_date="2026-05-02 10:12", entry_price=412.00,  shares=50,  status="open",   exit_date=None,                exit_price=None,    pl_dollar=None,     pl_pct=None,  grade=None, exit_reason=None,              lessons=None),
            Trade(id=3, symbol="AAPL",  strategy="carr",      setup_type="Pullback",           signal_source="manual_self",     entry_date="2026-04-10 14:28", entry_price=198.50,  shares=80,  status="closed", exit_date="2026-04-25 11:05", exit_price=189.20, pl_dollar=-744.00,  pl_pct=-4.69, grade="C",  exit_reason="stop_loss",      lessons="Pullback dip teyit eksikti, erken giriş"),
            Trade(id=4, symbol="GOOGL", strategy="carr",      setup_type="Bullish Divergence", signal_source="manual_external", entry_date="2026-05-10 13:15", entry_price=172.80,  shares=60,  status="open",   exit_date=None,                exit_price=None,    pl_dollar=None,     pl_pct=None,  grade=None, exit_reason=None,              lessons=None),
            Trade(id=5, symbol="AMD",   strategy="minervini", setup_type="Inside Day",         signal_source="strategy",        entry_date="2026-03-28 09:58", entry_price=145.40,  shares=70,  status="closed", exit_date="2026-04-18 14:33", exit_price=162.10, pl_dollar=1169.00,  pl_pct=11.49, grade="A",  exit_reason="target",         lessons="Inside Day patlama hacim teyitli"),
            Trade(id=6, symbol="TSLA",  strategy="minervini", setup_type="Tight Low Vol",      signal_source="strategy",        entry_date="2026-05-12 10:38", entry_price=238.20,  shares=40,  status="open",   exit_date=None,                exit_price=None,    pl_dollar=None,     pl_pct=None,  grade=None, exit_reason=None,              lessons=None),
            Trade(id=7, symbol="META",  strategy="carr",      setup_type="Pullback",           signal_source="manual_self",     entry_date="2026-04-05 11:20", entry_price=485.30,  shares=30,  status="closed", exit_date="2026-04-30 09:47", exit_price=510.20, pl_dollar=747.00,   pl_pct=5.13,  grade="B",  exit_reason="trailing_stop",  lessons="Trailing stop biraz sıkı kalmış, sabırlı kalsaydım daha iyi"),
            Trade(id=8, symbol="AVGO",  strategy="minervini", setup_type="VCP",                signal_source="strategy",        entry_date="2026-05-08 09:42", entry_price=1392.00, shares=10,  status="open",   exit_date=None,                exit_price=None,    pl_dollar=None,     pl_pct=None,  grade=None, exit_reason=None,              lessons=None),
        ]

    try:
        return [Trade(**t) for t in trades_get_all()]
    except OperationalError as e:
        raise HTTPException(
            status_code=503,
            detail="Veritabanına ulaşılamıyor (Cloud SQL). GCP Console → SQL → instance durum/Authorized Networks kontrol et."
        ) from e


@app.post("/api/trades", response_model=Trade, status_code=201)
def add_trade(body: TradeCreate) -> Trade:
    pl_dollar, pl_pct = None, None
    status = body.status
    if status == "closed" and body.exit_price is not None:
        pl_dollar, pl_pct = _calc_pl(body.entry_price, body.exit_price, body.shares)
    trade_data = {
        "symbol": body.symbol.strip().upper(),
        "strategy": body.strategy,
        "setup_type": body.setup_type,
        "entry_date": body.entry_date,
        "entry_price": body.entry_price,
        "exit_date": body.exit_date,
        "exit_price": body.exit_price,
        "shares": body.shares,
        "status": status,
        "pl_dollar": pl_dollar,
        "pl_pct": pl_pct,
        "grade": body.grade,
        "exit_reason": body.exit_reason,
        "lessons": body.lessons,
    }
    new_id = trades_insert(trade_data)
    return Trade(**trades_get_by_id(new_id))


@app.patch("/api/trades/{trade_id}", response_model=Trade)
def update_trade(trade_id: int, body: TradeUpdate) -> Trade:
    current = trades_get_by_id(trade_id)
    if not current:
        raise HTTPException(status_code=404, detail=f"Trade {trade_id} bulunamadı")
    updates = body.model_dump(include=body.model_fields_set)
    # Merge with current values for P/L recompute
    merged = {**current, **updates}
    if merged.get("status") == "closed" and merged.get("exit_price") is not None:
        updates["pl_dollar"], updates["pl_pct"] = _calc_pl(
            merged["entry_price"], merged["exit_price"], merged["shares"]
        )
    else:
        updates["pl_dollar"] = None
        updates["pl_pct"] = None
    trades_update(trade_id, updates)
    return Trade(**trades_get_by_id(trade_id))


@app.delete("/api/trades/{trade_id}", status_code=204)
def delete_trade(trade_id: int) -> Response:
    if not trades_delete(trade_id):
        raise HTTPException(status_code=404, detail=f"Trade {trade_id} bulunamadı")
    return Response(status_code=204)


# ── Signals ───────────────────────────────────────────────────────────────────

_STATUS_RANK: dict[str, int] = {s: i for i, s in enumerate(_STATUS_HIERARCHY)}


class Signal(BaseModel):
    # KARAR #469 (20 May 2026) revize: konsensus mantığı kaldırıldı.
    # Her watchlist satırı = 1 Signal (NVDA-Minervini ayrı, NVDA-Carr ayrı).
    # UX Bölüm 4 madde 5 ("Her satır ayrı trade: kendi stop, kendi hedef, kendi R/R") ile uyumlu.
    # KARAR #473 (20 May 2026 ~07:30): stop_loss + target_price + risk_reward eklendi
    # (UX Bölüm 4 madde 6: "R/R'a göre sıralı"). Backend hesaplar, frontend gösterir.
    symbol: str
    strategy: str
    status: str
    setup_type: Optional[str] = None
    rs_rating: float
    price: float
    stop_loss: Optional[float] = None
    target_price: Optional[float] = None
    risk_reward: Optional[float] = None  # (target - price) / (price - stop)
    added_date: str
    is_new_today: bool


def _calc_rr(price: float, stop: Optional[float], target: Optional[float]) -> Optional[float]:
    """Risk/Reward oranı: hedef / risk. price > stop ve target > price gerek."""
    if stop is None or target is None:
        return None
    risk = price - stop
    reward = target - price
    if risk <= 0 or reward <= 0:
        return None
    return round(reward / risk, 2)


@app.get("/api/signals", response_model=list[Signal])
def get_signals() -> list[Signal]:
    today = date.today().isoformat()

    # DB unreachable -> MOCK fallback (KARAR #469 + /api/screens pateni — Kural #20 UX)
    # Sn. Ferit: "sinyal varmış gibi yap mock ile sinyal sayfasında" (20 May 2026 ~06:00)
    # Sn. Ferit: "tarihlerin yanında saatte olsun" (20 May 2026 ~06:45) — added_date "YYYY-MM-DD HH:MM"
    # Her watchlist satırı 1 sinyal felsefesi korundu — çoklu strateji × çoklu hisse
    if not db_health_check():
        # KARAR #473: stop_loss + target_price + risk_reward (R/R 2:1 ile 3.5:1 arasında gerçekçi dağılım)
        def s(symbol, strategy, status, setup, rs, price, stop, target, added, new=False):
            return Signal(
                symbol=symbol, strategy=strategy, status=status, setup_type=setup,
                rs_rating=rs, price=price, stop_loss=stop, target_price=target,
                risk_reward=_calc_rr(price, stop, target),
                added_date=added, is_new_today=new,
            )
        mock_signals = [
            s("NVDA",  "minervini", "buy",     "VCP",                 99.0, 145.20,  141.50,  157.00,  f"{today} 09:32",     True),
            s("NVDA",  "carr",      "focus",   "Pullback",            99.0, 145.20,  142.00,  155.00,  "2026-05-19 14:15"),
            s("AAPL",  "minervini", "focus",   "Power Play",          87.0, 212.50,  207.00,  228.00,  "2026-05-18 10:47"),
            s("MSFT",  "minervini", "buy",     "Tight Low Vol",       91.0, 425.30,  418.00,  445.00,  "2026-05-19 11:23"),
            s("MSFT",  "carr",      "watch",   "Coiled Spring",       91.0, 425.30,  420.00,  None,    "2026-05-17 15:58"),
            s("GOOGL", "carr",      "buy",     "Bullish Divergence",  88.0, 178.40,  174.50,  189.00,  "2026-05-19 13:04"),
            s("AMD",   "minervini", "on_deck", "Inside Day",          85.0, 158.20,  154.00,  170.00,  "2026-05-18 16:42"),
            s("TSLA",  "minervini", "focus",   "Tight Low Vol",       82.0, 245.60,  240.00,  263.00,  "2026-05-19 09:51"),
            s("META",  "carr",      "watch",   "Pullback",            80.0, 512.80,  None,    None,    "2026-05-16 12:18"),
            s("AVGO",  "minervini", "buy",     "VCP",                 78.0, 1450.30, 1420.00, 1525.00, "2026-05-19 10:09"),
        ]
        # Sıralama: önce R/R (yüksek), R/R yoksa RS — UX Bölüm 4 madde 6
        mock_signals.sort(key=lambda x: (-(x.risk_reward or 0), -x.rs_rating))
        return mock_signals

    # Gerçek DB yolu (db_connected=true)
    try:
        all_rows = [WatchlistRow(**r) for r in watchlist_get_all()]
    except OperationalError as e:
        raise HTTPException(
            status_code=503,
            detail="Veritabanına ulaşılamıyor (Cloud SQL). GCP Console → SQL → instance durum/Authorized Networks kontrol et."
        ) from e

    # KARAR #469: her watchlist satırı = 1 sinyal (NO grouping)
    # is_new_today: added_date prefix (ilk 10 char) bugüne eşit mi
    #   — added_date "YYYY-MM-DD" veya "YYYY-MM-DD HH:MM" formatında olabilir
    # KARAR #473: stop_loss/target_price şu an web_watchlist'te yok (gelecek migration)
    #   → None döndürülür, R/R hesaplanmaz. MOCK fallback'te dolu.
    signals: list[Signal] = []
    for row in all_rows:
        added_prefix = (row.added_date or "")[:10]
        signals.append(Signal(
            symbol=row.symbol,
            strategy=row.strategy,
            status=row.status,
            setup_type=row.setup_type,
            rs_rating=row.rs_rating,
            price=row.price,
            stop_loss=None,
            target_price=None,
            risk_reward=None,
            added_date=row.added_date,
            is_new_today=(added_prefix == today),
        ))

    # Sıralama: RS rating descending (UX Bölüm 4 madde 6 ile uyumlu — sonra R/R sırası)
    signals.sort(key=lambda s: -s.rs_rating)
    return signals
