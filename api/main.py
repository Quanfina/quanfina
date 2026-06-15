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
    mark_signals_get_by_symbol,
    pattern_library_get_all,
    sector_rotation_get_latest,
    breadth_history_from_scans,
    minervini_stocks_get_latest,
    scan_latest_date,
)

# Sprint 4-bis.7 Faz 1 B paket: Mark KARAR #914 + #969 + #970
# Sprint 4-bis.7 Faz 2 başlangıç: Mark KARAR #834 + #855
# Sprint 4-bis.7 Faz 2 genişletme: Mark KARAR #893 + #882 + #864
from quanfina_math import (  # noqa: E402
    compute_dynamic_stop,
    mark_position_sizer,
    mark_six_rule_check,
    detect_eps_acceleration,
    detect_code_33,
    detect_tennis_ball,
    compute_volume_asymmetry,
    detect_leader_fingerprint,
    compute_rba_metrics,
    should_drop_setup,
    compute_pyramid_tier,
    count_distribution_days,
    compute_carr_stage,
    compute_market_breadth,
    compute_breadth_divergence,
    compute_follow_through_day,
    compute_pivot_breakout,
    compute_overhead_supply,
    compute_vcp_pass,
    compute_climax_run,
    compute_brandon_expectancy,
    compute_relative_strength_rating,
    rs_rating_category,
    compute_atr_volatility,
    compute_distribution_day_severity,
    compute_relative_volume,
    compute_breakout_quality,
    compute_cup_with_handle,
    compute_flat_base,
    compute_double_bottom,
    compute_high_tight_flag,
    compute_stage_transition,
    compute_faber_timing,
    compute_mcclellan_oscillator,
    compute_zweig_breadth_thrust,
    MCCLELLAN_SLOW_EMA,
    MARK_PYRAMID_PILOT_PCT_RANGE,
    MARK_PYRAMID_STANDARD_PCT_RANGE,
    MARK_PYRAMID_FULL_PCT_RANGE,
    MARK_STOP_ABSOLUTE_CAP_PCT,
    MARK_EQUITY_RISK_MIN_PCT,
    MARK_EQUITY_RISK_MAX_PCT,
    MARK_POSITION_MAX_PCT,
    MARK_POSITION_OPTIMAL_PCT_RANGE,
    MARK_PORTFOLIO_OPTIMAL_STOCKS,
    MARK_PORTFOLIO_MAX_STOCKS,
    MARK_EPS_MIN_GROWTH_PCT,
    MARK_EPS_SUPERPERFORMANCE_PCT,
    MARK_EPS_BULL_MARKET_PCT,
    MARK_EPS_TURNAROUND_PCT,
    MARK_EPS_90PCT_RULE_THRESHOLD,
    TENNIS_BALL_PULLBACK_MAX_DAYS,
    TENNIS_BALL_RECOVERY_MAX_DAYS,
    VOLUME_ASYMMETRY_HEALTHY_RATIO,
    LEADER_ADVANCE_MIN_PCT,
    LEADER_ADVANCE_MAX_PCT,
    LEADER_PULLBACK_MIN_PCT,
    LEADER_PULLBACK_MAX_PCT,
)

from typing import Literal, Optional

from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
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
    # Paket 368: MOCK -> DB wiring. En son minervini_scans taramasinin gercek
    # Trend Template sonuclarini doner; DB bos/erisilemez ise MOCK fallback (UX
    # kesintisiz, /screens + diger DB endpoint'lerin pateni).
    try:
        rows = minervini_stocks_get_latest(limit=100)
        if rows:
            return [MinerviniStock(**r) for r in rows]
    except Exception:
        pass  # DB erisilemez / sema sorunu -> MOCK fallback
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
    # KARAR #733 alt-paket (Paket 83, 26 May 2026): Pivot Breakout status
    # P81+P82 paten — Tarama'da AL/Zayıf/Yakın/Altı kolon
    pivot_status: Optional[Literal["CONFIRMED", "WEAK", "NEAR_PIVOT", "BELOW_PIVOT"]] = None
    # P423 (31 May 2026 — Kural #28 TEMEL "D" belirsizligi): grade='D' SADECE
    # eps_qoq/sales_qoq verisi yok demekse True (gercekten zayif DEGIL). UI gri
    # "D" + "veri yok" tooltip gosterir; gercek zayif D kirmizi kalir. db_helpers
    # _grade_no_data hesabi (eps/sales bos -> True).
    grade_no_data: Optional[bool] = None


@app.get("/api/screens", response_model=list[ScreenMeta])
def list_screens() -> list[ScreenMeta]:
    """Mevcut 8 ready screen meta listesi (frontend dropdown icin)."""
    return [ScreenMeta(**m) for m in screen_list_available()]


def _screens_mock_results(slug: str, limit: int) -> list[ScreenResultRow]:
    """Screens MOCK fallback satirlari (dev ortam VEYA transient DB hatasi).

    KARAR #466+#465+#467 — VCP/Power Play slug'larinda kalite+ready+power_play sahte.
    Paket 381: hem db_connected=false hem de gercek sorgu OperationalError'inda
    ayni MOCK uretimi kullanilir (DRY — Kural #20 graceful degradation kontrati).
    """
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
    # KARAR #733 alt-paket (Paket 83): pivot_status enrichment
    return [
        r.model_copy(update={
            "pivot_status": _compute_signal_pivot_status(r.symbol, r.price or 100.0),
        })
        for r in mock_rows[:limit]
    ]


# P415 (31 May 2026 — Kural #28 hizalama): Scanner gunde 1 kez calisiyor (Cloud
# Scheduler 22:00 UTC). Her sayfa acilisinda Cloud SQL'e gitmek bos is — veri o
# gun degismiyor. Module-level TTL cache + manuel bust.
#
# TTL 30 dakika: gun-ici veri sabit, scan_date degisirse zaten yeni gun (sabah
# ilk istek cache miss). Refresh icin ?nocache=1 parametresi (Sn. Ferit tarama
# tetikleyince yeni cache).
#
# Bellek: 9 slug x ~50KB = ~500KB (kabul edilebilir, container 512MB).
from time import time as _now
_SCREENS_CACHE: dict[tuple[str, int], tuple[float, list]] = {}
_SCREENS_CACHE_TTL = 30 * 60  # 30 dakika


@app.get("/api/screens/{slug}", response_model=list[ScreenResultRow])
def get_screen_results(slug: str, limit: int = 500, nocache: bool = False) -> list[ScreenResultRow]:
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

    # P415: Cache hit (TTL 30 dk, nocache=1 ile bust). Scanner gun-ici degismez.
    cache_key = (slug, limit)
    if not nocache:
        hit = _SCREENS_CACHE.get(cache_key)
        if hit is not None and (_now() - hit[0]) < _SCREENS_CACHE_TTL:
            return hit[1]

    # MOCK fallback (dev ortam, db_connected=false)
    # KARAR #466+#465+#467 — VCP/Power Play slug'larinda kalite+ready+power_play sahte
    if not db_health_check():
        return _screens_mock_results(slug, limit)

    # Gerçek DB sorgusu — ready VEYA parse VEYA diff (dispatch)
    # Paket 381: db_health_check 30s TTL cache (db_helpers) yaniltici "True"
    # donderebilir (saglik check anindan sorgu anina kadar Cloud SQL baglantisi
    # dusebilir/pool tukenebilir/timeout) -> ham OperationalError 500'e cikiyordu.
    # Graceful degradation: sorgu patlarsa MOCK don (docstring kontrati + Kural #20 UX).
    try:
        rows = screen_get_results_dispatch(slug, limit=limit)
    except ValueError:
        raise  # bilinmeyen slug — 404 mantigi disinda (dispatch ValueError)
    except Exception:
        return _screens_mock_results(slug, limit)
    db_results = [ScreenResultRow(**{k: r.get(k) for k in
                                ("symbol","grade","rs_ibd","price","passed","scan_date",
                                 "vcp_quality_score","vcp_ready_score","power_play_pass",
                                 "grade_no_data")})  # P423: TEMEL "D" veri-yok ayrimi
            for r in rows]
    # KARAR #733 alt-paket (Paket 83): pivot_status enrichment (DB yol)
    # P415 (31 May 2026): Sira-i enrichment 228 satir × ~150ms yfinance =
    # 30s+ timeout. Paralel 20 worker = ~3s (pivot cache ilk sicakta).
    from concurrent.futures import ThreadPoolExecutor
    def _enrich_one(row):
        return row.model_copy(update={
            "pivot_status": _compute_signal_pivot_status(row.symbol, row.price or 100.0),
        })
    with ThreadPoolExecutor(max_workers=20) as pool:
        enriched = list(pool.map(_enrich_one, db_results))
    # P415: Cache yaz (enrichment dahil tam sonuc — pivot_status dahil)
    _SCREENS_CACHE[cache_key] = (_now(), enriched)
    return enriched


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
        # P432 (31 May 2026 — tooltip doğrulama workflow): "200 haftalık" HATA idi
        # → 30 haftalık MA (Weinstein kanon + Quanfina CARR_STAGE_MA_WINDOW=150).
        tooltip="30 haftalık MA üstünde yükseliş trendi — piyasa bull fazında sağlıklı.",
        definition=(
            "Stan Weinstein 4 aşama modelinde Stage 2, fiyatın 30 haftalık hareketli "
            "ortalama üstünde ve MA eğimi pozitif olduğu yükseliş trendinin ana "
            "aşamasıdır. SPY Stage 2 = genel piyasa sağlıklı, LONG mod önerilir. "
            "Mark Minervini ve Thomas Carr da Stage 2'yi aynı metriklere göre tanımlar."
        ),
        source_book="Secrets for Profiting in Bull and Bear Markets",
        source_author="Stan Weinstein",
        source_year=1988,
        quanfina_context="compute_carr_stage() (quanfina_math.py) — CARR_STAGE_MA_WINDOW=150 (30 hafta×5 gün). Stage 2: price > 30W MA + slope pozitif. Piyasa Durumu SPY/QQQ/IWM Stage tespiti → LONG mod.",
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
        # P432: "günlük değişim" HATA → 1 aylık (21 gün) eğim (scanner.py ma200_today - ma200_1m)
        tooltip="200 günlük MA'nın 1 aylık eğimi. Pozitif = uzun vadeli yükseliş.",
        definition=(
            "200 günlük basit hareketli ortalamanın son bir ay içindeki eğimi "
            "(MA200 bugün − MA200 21 gün önce). Pozitif eğim MA200'ün yükselişte "
            "olması = uzun vadeli trend sağlıklı. Minervini Trend Template koşulu: "
            "MA200 eğimi en az 1 aydır pozitif olmalı (Wizard s.79)."
        ),
        source_book="Trade Like a Stock Market Wizard",
        source_author="Mark Minervini",
        source_year=2013,
        quanfina_context="scanner.py (~satır 881): ma200_today − ma200_1m (21 gün). Pozitif slope Trend Template Kural 3'ü geçirir. Minervini grid MA200 EĞİM sütunu (yeşil/kırmızı).",
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
        # P432: eşik/birikim netleştirildi (kod: -0.2% + 20 gün + Mark 4 DD kuralı)
        tooltip="Endeks ≤%-0.2 kapanırken hacim artışı — kurumsal satış günü; 4+ DD yeni alım yasak.",
        definition=(
            "O'Neil tanımı: Endeks (S&P/Nasdaq) önceki güne göre %0.2 veya daha "
            "fazla düşerken hacmin bir önceki günden yüksek olması. Quanfina 20 "
            "günlük pencerede sayar; 3+ DD uyarı, 5+ DD satış baskısı. Mark kuralı: "
            "4 DD = yeni alım yasak, 5+ DD = defansif mod (TLSMW Böl. 5)."
        ),
        source_book="How to Make Money in Stocks",
        source_author="William J. O'Neil",
        source_year=1988,
        quanfina_context="quanfina_math.py count_distribution_days() — eşik -0.2%, 20 gün pencere, hacim>önceki + 4-katman rejim (KARAR #488). Piyasa Durumu sayaç (>4 sarı/kırmızı uyarı).",
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
        # P432: placeholder dolduruldu (kavram öğretimi — Kural #17, "®" kullanılmadı sızma_kontrol)
        tooltip="Specific Entry Point Analysis — Mark Minervini'nin 4 bileşenli hisse seçim framework'ü.",
        definition=(
            "SEPA (Specific Entry Point Analysis), Mark Minervini'nin hisse seçim ve "
            "giriş framework'üdür. 4 bileşeni birleştirir: (1) Teknik setup (VCP/Pivot/"
            "Power Play) + Trend Template, (2) Earnings — EPS Q/Q + Satış Q/Q ivmesi "
            "(%25+), (3) Price Action — RS>70, konsolidasyon sonrası kırılım, (4) "
            "Katalizör/haber. %95+ hisse elenir, 5-20 yüksek olasılıklı aday kalır."
        ),
        source_book="Trade Like a Stock Market Wizard",
        source_author="Mark Minervini",
        source_year=2013,
        quanfina_context="Minervini stratejisinin omurgası: scanner.py Trend Template + EPS/Satış ivme + VCP/Power Play setup tarama + watchlist katmanları (watch/on-deck/focus/buy) SEPA karar hiyerarşisini temsil eder.",
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
        # P432: sayısal eşikler eklendi (kod: 20 gün, %0.5 buffer, 1.5x hacim)
        tooltip="Son 20 günün en yüksek kapanışı; kırılım = %0.5 üstü + 1.5x hacim teyit (TLSMW Böl. 10).",
        definition=(
            "Minervini Pivot Fiyatı: son 20 günün (bugün hariç) en yüksek kapanış "
            "seviyesi. Geçerli kırılım sinyali 3 koşul: (1) fiyat pivot'un %0.5 "
            "üstüne çıkar, (2) hacim 50-gün ortalamasının ≥1.5 katı, (3) hacim teyidi "
            "olmadan kırılım 'false breakout' riski taşır."
        ),
        source_book="Trade Like a Stock Market Wizard",
        source_author="Mark Minervini",
        source_year=2013,
        quanfina_context="quanfina_math.py compute_pivot_breakout() — PIVOT_PRICE_LOOKBACK=20, PIVOT_BUFFER_PCT=0.5, PIVOT_VOLUME_CONFIRM_RATIO=1.5 (KARAR #733). Minervini grid opsiyonel sütun (null = baz yok).",
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
        # P432: placeholder dolduruldu (tooltip doğrulama workflow + kod referansı)
        tooltip="Mark Minervini'nin Trend Template + temel analiz kombinasyonu hisse seçim ve trade sistemi.",
        definition=(
            "Mark Minervini'nin 'Trade Like a Stock Market Wizard' (2013) sistemi: "
            "teknik + temel analizi birleştirir. Trend Template (8 koşul: MA150/200 "
            "üstünde, MA50>MA150>MA200, MA200 pozitif eğim, 52W zirve sınırında, "
            "RS>70) ile başlar, VCP/Cup-Handle konsolidasyonlarını izler, pivot "
            "kırılımında girer, risk-first (Stop + R-Multiple) ile yönetir."
        ),
        source_book="Trade Like a Stock Market Wizard",
        source_author="Mark Minervini",
        source_year=2013,
        quanfina_context="scanner.py Trend Template (Finviz Elite filtreleri) + minervini_scans tablosu (teknik MA200 slope/high52/RS + temel EPS/Satış Q/Q). /minervini grid + /watchlist strateji filtresi.",
        category="strategy",
    ),
    Term(
        key="carr_method",
        short_name="Carr Stratejisi",
        # P432: placeholder dolduruldu (Carr Trend Trading + Weinstein 4-Stage)
        tooltip="Thomas Carr Trend Trading — Stan Weinstein 4-Stage analizi (Basing/Advancing/Topping/Declining).",
        definition=(
            "Thomas Carr'ın 'Trend Trading for a Living' (2008) metodolojisi, Stan "
            "Weinstein 4-aşama modelinin uyarlamasıdır: Stage 1 (Basing/baz), Stage 2 "
            "(Advancing/yükseliş), Stage 3 (Topping/tepe), Stage 4 (Declining/düşüş). "
            "Quanfina'da 150 günlük MA (30 hafta), ±%1 eğim flat eşiği, ±%5 proximity "
            "kriterleri kullanılır."
        ),
        source_book="Trend Trading for a Living",
        source_author="Thomas Carr",
        source_year=2008,
        quanfina_context="quanfina_math.py compute_carr_stage() — MA_WINDOW=150, SLOPE_FLAT_PCT=1.0, PROXIMITY_PCT=5.0. /api/carr/stage/{symbol} + CarrStageCard. /watchlist Carr stratejisi satırları.",
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
        # P432: HATA FİX — "Rule-Based" YANLIŞ → "Result-Based Analysis" (Mark TTLC Böl. 4,
        # uygulamadaki kullanım "Result-Based" ile tutarlı). Placeholder def de dolduruldu.
        tooltip="Result-Based Analysis — kapanmış trade istatistikleriyle setup edge'i ölçer (min 30 trade).",
        definition=(
            "Mark Minervini'nin Result-Based Analysis (RBA) metodu, geçmiş trade "
            "sonuçlarını analiz ederek bir stratejinin istatistiksel edge'ini ölçer. "
            "Adjusted Ratio (avg gain × win% / avg loss × loss%), expectancy ve win "
            "rate gibi metriklerle min 30 trade sonrası setup'ın sürdürülüp "
            "sürdürülmeyeceğine karar verir (TTLC Böl. 4: 'Know the truth')."
        ),
        source_book="Think and Trade Like a Champion",
        source_author="Mark Minervini",
        source_year=2017,
        quanfina_context="quanfina_math.py compute_rba_metrics() (7 metrik, 30 trade eşiği) + should_drop_setup() (adjusted_ratio<1.0 → bırak). İstatistikler + İşlem Günlüğü sayfalarında Mark RBA kartı.",
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


class MarkRegimeInfo(BaseModel):
    """KARAR #488 (Vizyon v20.99) — Mark Market Regime 4-Katman x 2-Eksen.
    Backend pre-compute: distribution_days -> regime + Mark felsefe etiketi."""
    regime: Literal["HEALTHY", "CAUTION", "UNDER_PRESSURE", "BEAR_PRESSURE"]
    label: str            # TR etiket
    allocation: str       # Mark allocation oneri
    new_buy_allowed: bool # Yeni alim izinli mi
    pilot_override: bool  # Lider hisse %1-2 pilot Override


def _compute_mark_regime(distribution_days: int) -> MarkRegimeInfo:
    """KARAR #488 4-katman + O'Neil mekanik (Mark birebir, web/lib paten)."""
    dd = max(0, int(distribution_days))
    if dd <= 2:
        return MarkRegimeInfo(
            regime="HEALTHY", label="Sağlıklı",
            allocation="Tam pozisyon (100%)",
            new_buy_allowed=True, pilot_override=True,
        )
    if dd == 3:
        return MarkRegimeInfo(
            regime="CAUTION", label="Dikkat",
            allocation="Mevcut korunur, yeni alım sıkı kriter",
            new_buy_allowed=True, pilot_override=True,
        )
    if dd == 4:
        # P449 (DD canon derin araştırma): 4 DD = IBD "Uptrend Under Pressure"
        # = %50 exposure + "avoid new buys" AMA "door open" — MUTLAK YASAK DEĞİL.
        # YASAK (Market in Correction) canon 5-6 DD. Eski 4DD→new_buy_allowed=False
        # dd_severity (4DD=CAUTION/yavaşla) ile aynı dd_count'ta ÇELİŞİYORDU (3-katman
        # bug, P446 çelişkisinin kökü). 4DD artık kısıtlı-açık; YASAK 5+'a taşındı.
        # Kaynak (11 Haz uc-kaynak danisma teyit): O'Neil HTMMIS s.45,50 = "indexes
        # reverse -> raise 25%+ cash" (KITAP NITEL; 4/5 DD COUNT'u kitapta YOK) +
        # 4/5 DD esigi = IBD Market Pulse kitap-sonrasi konvansiyon (nasdaq.com 2014:
        # "cut to 50%... leaving the door open"). NOT: %50/%25 exposure yuzdeleri
        # Quanfina ic tasarim — ne O'Neil ne Minervini kitabi DD-count->% verir (Kural #26).
        return MarkRegimeInfo(
            regime="UNDER_PRESSURE", label="Baskı Altında",
            allocation="%50 pozisyon — yeni alım sadece A+ setup + lider pilot (kapı aralık)",
            new_buy_allowed=True, pilot_override=True,
        )
    return MarkRegimeInfo(
        regime="BEAR_PRESSURE", label="Ayı Baskısı",
        allocation="%25 max veya nakit — yeni alım YASAK (Market in Correction, 5+ DD)",
        new_buy_allowed=False, pilot_override=True,
    )


class MarketBreadthInfo(BaseModel):
    """KARAR #733 alt-paket (Paket 52, 25 May 2026): Mark+O'Neil A/D Line canon.
    quanfina_math.compute_market_breadth backend pre-compute (P51 helper).
    P409: is_mock flag — DB scanner verisi varsa False, MOCK fallback ise True."""
    ad_ratio: float                       # Bugun advance/decline orani
    ad_line_cumulative: int               # 20-gun birikimli (advance - decline)
    breadth_health: Literal["STRONG", "NEUTRAL", "WEAK"]
    mark_says: str                        # Mark felsefe yorumu
    is_mock: bool = False                 # P409: True = MOCK fallback, False = gercek scan


class BreadthDivergenceInfo(BaseModel):
    """KARAR #733 alt-paket (Paket 57, 25 May 2026): Mark+O'Neil divergence canon.
    quanfina_math.compute_breadth_divergence backend pre-compute (P56 helper).
    P409: breadth_is_mock — A/D Line MOCK fallback ise True (SPY closes hep gercek)."""
    divergence: Literal[
        "CONFIRMED_UP", "BEARISH_DIVERGENCE", "BULLISH_DIVERGENCE",
        "CONFIRMED_DOWN", "NEUTRAL",
    ]
    index_change_pct: float               # lookback boyunca % degisim (SPY)
    ad_trend_delta: int                   # lookback A/D cumulative delta
    severity: Literal["ok", "info", "warn", "critical"]
    mark_says: str                        # Mark+O'Neil felsefe yorumu
    breadth_is_mock: bool = False         # P409: A/D MOCK ise True (SPY her zaman gercek)


class FollowThroughDayInfo(BaseModel):
    """KARAR #733 alt-paket (Paket 65, 25 May 2026): Mark/O'Neil FTD canon.
    quanfina_math.compute_follow_through_day backend pre-compute (P64 helper)."""
    ftd_detected: bool
    ftd_gain_pct: Optional[float] = None      # FTD günü % degisim (1.7+)
    days_after_low: Optional[int] = None      # Dip'ten kaç gün sonra
    volume_confirmed: bool = False             # Hacim teyit (onceki >=1x)
    previous_low: Optional[float] = None       # Bulunan dip fiyatı
    mark_says: str                             # Mark/O'Neil felsefe yorumu


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
    # KARAR ADAY #731 (24 May 2026): Mark Regime backend pre-compute
    # (KARAR #488 4-Katman x 2-Eksen, frontend DRY)
    mark_regime: Optional[MarkRegimeInfo] = None
    # KARAR #733 alt-paket (Paket 52, 25 May 2026): Market Breadth A/D Line
    # backend pre-compute (P51 compute_market_breadth helper wire)
    market_breadth: Optional[MarketBreadthInfo] = None
    # KARAR #733 alt-paket (Paket 57, 25 May 2026): Index vs A/D divergence
    # backend pre-compute (P56 compute_breadth_divergence helper wire)
    breadth_divergence: Optional[BreadthDivergenceInfo] = None
    # KARAR #733 alt-paket (Paket 65, 25 May 2026): Follow-Through Day backend
    # pre-compute (P64 compute_follow_through_day helper wire — Mark/O'Neil)
    follow_through: Optional[FollowThroughDayInfo] = None
    # P110 (26 May 2026): DD Severity — O'Neil CLEAN/CAUTION/HEAVY/EXTREME
    dd_severity: Optional[str] = None
    dd_allocation_factor: Optional[float] = None
    dd_severity_mark_says: Optional[str] = None


MOCK_MARKET_STATUS = MarketStatus(
    spy_stage=2,
    qqq_stage=2,
    iwm_stage=1,
    vix=14.2,
    distribution_days=3,
    market_health_score=75,
    market_health_label="HEALTHY",  # P382: clean-room (Markets360 Bundle "YESIL/SARI/KIRMIZI" sizma temizligi)
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
    # KARAR ADAY #731: Mark Regime backend pre-compute
    mark_regime=_compute_mark_regime(3),
)


def _mock_index_history(
    ticker: str = "SPY",
    start_price: float = 400.0,
    days: int = 25,
    drift_mean: float = 0.05,
    drift_std: float = 0.7,
) -> tuple[list[float], list[int]]:
    """MOCK SPY/QQQ/IWM closes + volumes - deterministik (tarih + ticker hash seed).

    KARAR #488 + #733 alt-paket (Paket 22 + 24): count_distribution_days +
    compute_carr_stage helper'lari icin gercek bir veri akisi MOCK uretim.
    Production'da yfinance/Cloud SQL real tarihsel veri (ACIK KONU #75).

    Algoritma: hafif drift + arada negatif gunler (DD adaylari).
    Deterministik (tarih + ticker seed) ki ayni gun ayni veri.

    Args:
        ticker: 'SPY' / 'QQQ' / 'IWM' (seed icin)
        start_price: Baslangic fiyati
        days: Pencere uzunlugu (25 = DD; 180 = Carr Stage 30W)
        drift_mean: Gunluk ortalama % degisim
        drift_std: Gunluk std
    """
    seed = int(date.today().toordinal()) + sum(ord(c) for c in ticker)
    rng = random.Random(seed)
    closes: list[float] = [start_price]
    volumes: list[int] = []
    for i in range(days):
        pct = rng.gauss(drift_mean, drift_std)
        closes.append(closes[-1] * (1 + pct / 100.0))
        base_vol = 80_000_000
        if pct < 0:
            vol = int(base_vol * rng.uniform(1.0, 1.4))
        else:
            vol = int(base_vol * rng.uniform(0.8, 1.1))
        volumes.append(vol)
    closes = closes[1:]
    return closes, volumes


# Geriye uyum alias (Paket 22 ismi)
def _mock_spy_closes_volumes(days: int = 25) -> tuple[list[float], list[int]]:
    return _mock_index_history("SPY", 400.0, days)


def _index_closes_volumes(ticker: str, start_price: float, days: int) -> tuple[list[float], list[int]]:
    """Paket 369: SPY/QQQ/IWM icin GERCEK yfinance OHLCV (son `days` bar);
    fail/yetersiz veri ise MOCK fallback (deterministik, UX/test kesintisiz).

    _fetch_ohlcv_real 5dk cache -> ayni request icinde ticker tekrar cagrilirsa 0s
    + determinizm (test_idempotent gecer). Breadth (advance/decline) yfinance'te
    yok -> ayri (MOCK kalir). market_health_score formulu yok -> mode hala MOCK
    (Kural #26: uydurma yok); ama index STAGE + DD artik gercek piyasa verisi.
    """
    bars = _fetch_ohlcv_real(ticker, max(days, 252))
    if bars and len(bars) >= max(60, days):
        closes = [b.close for b in bars][-days:]
        volumes = [b.volume for b in bars][-days:]
        return closes, volumes
    return _mock_index_history(ticker, start_price, days)


def _mock_breadth_history(days: int = 25) -> tuple[list[int], list[int]]:
    """KARAR #733 alt-paket (Paket 52, 25 May 2026): MOCK A/D advance/decline.

    Deterministik tarih seed — ayni gun ayni veri. Production'da
    minervini_scans + sector_rotation tablosundan gunluk sayim
    (AÇIK KONU #75 yfinance pipeline + Cloud SQL JOIN).

    Algoritma: NYSE+NASDAQ ~3500 hisse varsayim. Hafif bias daily.
    Tarih seed dagilimi ile market regime'e gore ortalama A/D ratio
    degisir (carr-stage paten).
    """
    seed = int(date.today().toordinal()) + 17  # SPY/QQQ/IWM seedler 0-200 arasi
    rng = random.Random(seed)
    advances: list[int] = []
    declines: list[int] = []
    total_stocks = 3500
    for _ in range(days):
        # Bias degisken (regime hafif drift)
        bias = rng.uniform(0.40, 0.60)  # 0.4 = zayif, 0.6 = saglikli
        adv = int(total_stocks * bias)
        dec = total_stocks - adv
        # Hafif gurultu
        adv += rng.randint(-100, 100)
        dec += rng.randint(-100, 100)
        advances.append(max(0, adv))
        declines.append(max(0, dec))
    return advances, declines


def _compute_market_health(
    dd_count: int,
    spy_stage: int,
    qqq_stage: int,
    iwm_stage: int,
    ftd_confirmed: bool = False,
) -> tuple[int, str, str]:
    """Paket 382: market_health_score + label MOCK->gercek (AÇIK KONU #58 cozumu).

    Cift Danisma (Kural #20, 30 May 2026): Quanfina Notebook + Quanfina Minervini
    + Görsel #1+#2 birinci-agiz tarama (sat. 685-727 GO/CAUTION/NO-GO Markets360
    Bundle kanit, sat. 2785 ">=%70 esik yabanci platform mu Mark manuel mi" GELISTIRILMESI
    LAZIM #58, sat. 1833-1866 SPY Strategy 25 proprietary kapali algoritma).

    Mark CANON birlesik 0-100 SKOR YOKTUR (kitap teyit). Skor degerleri 25/50/75
    Quanfina IC TASARIM KARARI — UI 0-100 kontrat zorunlulugu (frontend esik
    >=70 yesil, >=40 sari, <40 kirmizi). Mark canon iddiasi yapilmaz; kategori
    onceliklidir (Kural #26 + KALICI ILKE #4: ASLA UYDURMA SAYI/FORMUL).

    Mantik:
    - DD>=4 -> UNDER_PRESSURE override
      Kaynak: Trade Like a Stock Market Wizard Bol. 5 (Mark birebir "Under
      Pressure" kategorik terim, Quanfina Minervini teyit, kitap kanon)
    - Probability Convergence (TLSMW Bol. 10 kitap kanon):
      3 endeks Stan Weinstein Stage 2 hizalanma -> HEALTHY (tum faktor ayni anda)
      2+ endeks Stage 4 -> UNDER_PRESSURE (Mark bear confirm)
      Diger -> NEUTRAL (selective/rotational, alim kismi)

    Clean-room: Markets360 GO/CAUTION/NO-GO mesajlari + "YESIL/SARI/KIRMIZI"
    Bundle CSS sizma riski -> KULLANILMAZ. "Under Pressure" Mark kitap birebir
    terim (sizma yok). HEALTHY/NEUTRAL jenerik finansal terim.

    Args:
        dd_count: count_distribution_days lookback 20 cikti (gercek SPY P369).
        spy_stage / qqq_stage / iwm_stage: compute_carr_stage cikti (1-4).

    Returns:
        (score 0-100 int, label HEALTHY/NEUTRAL/UNDER_PRESSURE,
         suggested_mode LONG/CAUTION/DEFENSIVE).
    """
    # Endeks Stage sayimlari (siralama icin once hesaplanir)
    indices_in_stage2 = sum(
        1 for s in (spy_stage, qqq_stage, iwm_stage) if s == 2
    )
    indices_in_stage4 = sum(
        1 for s in (spy_stage, qqq_stage, iwm_stage) if s == 4
    )

    # 1. YAPISAL BEAR (en derin sinyal): 2+ endeks Stage 4 (declining, 30W MA alti)
    #    -> UNDER_PRESSURE. P424: FTD bu durumu KURTARMAZ — Stage 4 dususu
    #    distribution-day baskisindan daha derin; bear'da basarisiz FTD yaygindir.
    #    Bu kontrol DD-dalindan ONCE gelir ki FTD yumusatmasi yapisal bear'a sizmasin.
    if indices_in_stage4 >= 2:
        return 25, "UNDER_PRESSURE", "DEFENSIVE"

    # 2. Mark/O'Neil kitap kanon: DD>=4 -> "Under Pressure"
    #    P424 (31 May 2026 — FTD simetri, derin tarama F3 bulgusu):
    #    IBD Market Pulse SIMETRIK — DD birikimi zayiflik, Follow-Through Day
    #    yeni uptrend ONAYI (rally 4-7. gun yuksek hacimli kayda deger kazanc).
    #    Quanfina onceden asimetrikti (sadece DD baskisi). Yapisal bear YOKKEN
    #    (yukaridaki kontrol gecildiyse) hacim-onayli FTD, DD baskisini
    #    UNDER_PRESSURE -> NEUTRAL'a yumusatir (dagitim defterde olsa bile
    #    toparlanma onayi). Kaynak: O'Neil "How to Make Money in Stocks" + IBD
    #    Market Pulse (notebook/analizler/Market_Health_Metodoloji_Arastirma.md F3).
    if dd_count >= 4:
        if ftd_confirmed:
            return 50, "NEUTRAL", "CAUTION"
        return 25, "UNDER_PRESSURE", "DEFENSIVE"

    # 3. 3/3 endeks Stage 2 (advancing) -> tam Probability Convergence (TLSMW Bol. 10)
    if indices_in_stage2 == 3:
        return 75, "HEALTHY", "LONG"

    # Karisik (1-2 endeks Stage 2, Stage 4 az) -> selective
    if indices_in_stage2 >= 1 and indices_in_stage4 == 0:
        return 50, "NEUTRAL", "CAUTION"

    # Hicbir endeks Stage 2 yok + Stage 4 1 -> zayif piyasa
    return 25, "UNDER_PRESSURE", "DEFENSIVE"


def _index_stage(ticker: str, start_price: float) -> int:
    """KARAR #733 alt-paket (Paket 24): SPY/QQQ/IWM stage dinamik hesap.

    180 gun GERCEK yfinance (fail->MOCK fallback) + compute_carr_stage helper.
    Stan Weinstein 4-Stage (1=Basing, 2=Advancing, 3=Topping, 4=Declining).
    Helper None donerse fallback Stage 2 (default).
    """
    closes, volumes = _index_closes_volumes(ticker, start_price, 180)  # P369: gercek index
    result = compute_carr_stage(closes, volumes, ma_window=150)
    return result.get("stage") or 2


@app.get("/api/market/status", response_model=MarketStatus)
def get_market_status() -> MarketStatus:
    # KARAR #731 + #488 alt (Paket 22): distribution_days GERCEK SPY -> DD count
    # (P369: yfinance gercek SPY OHLCV, fail->MOCK fallback)
    closes, volumes = _index_closes_volumes("SPY", 400.0, 25)
    dd_result = count_distribution_days(closes, volumes, lookback_days=20)
    dd_count = dd_result["count"]

    # KARAR #733 alt (Paket 24): SPY/QQQ/IWM stage dinamik compute_carr_stage
    # Production'da yfinance/SQL real veri (AÇIK KONU #75).
    spy_stage = _index_stage("SPY", 400.0)
    qqq_stage = _index_stage("QQQ", 380.0)
    iwm_stage = _index_stage("IWM", 200.0)

    # KARAR #733 alt (Paket 52): Market Breadth A/D Line backend pre-compute.
    # P409: minervini_scans tablosundan GERÇEK A/D sayım (Quanfina'nın günlük
    # 700+ sembol Finviz Elite taraması — change_pct kolonu pozitif/negatif
    # sayımı). DB boş veya erişilemez ise MOCK fallback (UX kesintisiz).
    real_breadth = breadth_history_from_scans(days=25)
    if real_breadth:
        advances = [a for a, _ in real_breadth]
        declines = [d for _, d in real_breadth]
        breadth_is_mock = False
    else:
        advances, declines = _mock_breadth_history(days=25)
        breadth_is_mock = True
    breadth = compute_market_breadth(advances, declines, lookback_days=20)
    market_breadth = None
    if breadth.get("breadth_health"):  # None ise skip (edge)
        market_breadth = MarketBreadthInfo(
            ad_ratio=breadth["ad_ratio"],
            ad_line_cumulative=breadth["ad_line_cumulative"],
            breadth_health=breadth["breadth_health"],
            mark_says=breadth["mark_says"],
            is_mock=breadth_is_mock,
        )

    # KARAR #733 alt (Paket 57): Index vs A/D Divergence backend pre-compute
    # (Mark+O'Neil canon — P56 compute_breadth_divergence helper wire)
    # SPY closes 25-gun GERCEK (P369) + A/D Line P409 ile gerçek (MOCK fallback)
    spy_closes, spy_volumes = _index_closes_volumes("SPY", 400.0, 25)
    divergence_result = compute_breadth_divergence(
        spy_closes, advances, declines, lookback_days=10,
    )
    breadth_divergence = None
    if divergence_result.get("divergence"):
        breadth_divergence = BreadthDivergenceInfo(
            divergence=divergence_result["divergence"],
            index_change_pct=divergence_result["index_change_pct"],
            ad_trend_delta=divergence_result["ad_trend_delta"],
            severity=divergence_result["severity"],
            mark_says=divergence_result["mark_says"],
            breadth_is_mock=breadth_is_mock,  # P409: A/D MOCK fallback flag
        )

    # KARAR #733 alt (Paket 65): Follow-Through Day backend pre-compute
    # (Mark/O'Neil canon — P64 compute_follow_through_day helper wire)
    # Aynı SPY closes + volumes 25-gun feed (deterministik)
    ftd_result = compute_follow_through_day(
        spy_closes, spy_volumes, lookback_days=15,
    )
    follow_through = FollowThroughDayInfo(
        ftd_detected=bool(ftd_result.get("ftd_detected", False)),
        ftd_gain_pct=ftd_result.get("ftd_gain_pct"),
        days_after_low=ftd_result.get("days_after_low"),
        volume_confirmed=bool(ftd_result.get("volume_confirmed", False)),
        previous_low=ftd_result.get("previous_low"),
        mark_says=ftd_result.get("mark_says", ""),
    )

    dd_sev = compute_distribution_day_severity(dd_count)

    # P382: market_health_score + label + suggested_mode MOCK->gercek
    # (AÇIK KONU #58 cozumu, Cift Danisma kanit + helper docstring)
    # P424: FTD simetri — hacim-onayli Follow-Through Day DD baskisini yumusatir
    # (ftd_result yukarida zaten hesaplandi — compute_follow_through_day).
    ftd_confirmed = bool(
        ftd_result.get("ftd_detected") and ftd_result.get("volume_confirmed")
    )
    health_score, health_label, suggested_mode = _compute_market_health(
        dd_count, spy_stage, qqq_stage, iwm_stage, ftd_confirmed=ftd_confirmed,
    )

    # P398: VIX MOCK -> gercek yfinance ^VIX (CBOE Volatility Index son kapanış)
    # Sn. Ferit Piyasa Durumu sayfasinda 14.2 hardcoded sahte görüyordu.
    # Fail durumunda MOCK fallback (UX kesintisiz).
    vix_bars = _fetch_ohlcv_real("^VIX", 2)
    vix_value = round(vix_bars[-1].close, 2) if vix_bars else MOCK_MARKET_STATUS.vix

    # P398: top/bottom_sectors MOCK hardcoded -> gercek sector_rotation tablosu.
    # Cloud SQL sector_rotation gunluk 11 SPDR ETF taramasi (mevcut endpoint
    # /api/sector-rotation kullanir). perf_1w sirali — top 3 + bottom 3.
    # DB bos/erisilemez ise MOCK fallback (hardcoded 5 sabit sektor).
    sectors_db = sector_rotation_get_latest()
    if sectors_db:
        sorted_by_perf = sorted(
            sectors_db,
            key=lambda s: (s.get("perf_1w") if isinstance(s, dict) else s.perf_1w) or 0,
            reverse=True,
        )
        def _to_sc(s):
            name = s.get("sector_name") if isinstance(s, dict) else s.sector_name
            perf = (s.get("perf_1w") if isinstance(s, dict) else s.perf_1w) or 0.0
            return SectorChange(name=name, change_pct=round(perf, 2))
        top_sectors_live = [_to_sc(s) for s in sorted_by_perf[:3]]
        bottom_sectors_live = [_to_sc(s) for s in sorted_by_perf[-3:]]
    else:
        top_sectors_live = MOCK_MARKET_STATUS.top_sectors
        bottom_sectors_live = MOCK_MARKET_STATUS.bottom_sectors

    status = MOCK_MARKET_STATUS.model_copy(
        update={
            "distribution_days": dd_count,
            "spy_stage": spy_stage,
            "qqq_stage": qqq_stage,
            "iwm_stage": iwm_stage,
            "vix": vix_value,
            "top_sectors": top_sectors_live,
            "bottom_sectors": bottom_sectors_live,
            "market_health_score": health_score,
            "market_health_label": health_label,
            "suggested_mode": suggested_mode,
            "mark_regime": _compute_mark_regime(dd_count),
            "market_breadth": market_breadth,
            "breadth_divergence": breadth_divergence,
            "follow_through": follow_through,
            "dd_severity": dd_sev["severity"],
            "dd_allocation_factor": dd_sev["allocation_factor"],
            "dd_severity_mark_says": dd_sev["mark_says"],
        }
    )
    return status


# ─── Ek Piyasa Göstergeleri (P425, 31 May 2026 — derin tarama F4/F5/F7) ──────
# Ana Sayfa altında ayrı bölüm. Faber GTAA + McClellan + Zweig Breadth Thrust.
# Kural #28: gerçek veri (SPY OHLCV + minervini_scans breadth). Yetersiz veri ->
# data_sufficient False (MOCK sayı YOK). NAAIM/AAII/Put-Call: harici ücretli feed
# olmadığı için KODLANMADI (MOCK olur). Tooltip açıklamaları frontend'de.
class ExtraIndicatorsInfo(BaseModel):
    faber: dict
    mcclellan: dict
    zweig: dict
    breadth_source: str = "scans"  # "scans" | "backfill" | "none"


# P426 (31 May 2026 — Sn. Ferit "eski verileri cekerek canlandiramaz miyiz"):
# minervini_scans sadece ~15 gun var -> McClellan 39 gun gerekiyor. Cozum: scan
# evreninin GERCEK yfinance fiyat gecmisinden gunluk advance/decline geriye-dolum.
# Kural #28: gercek tarihsel fiyat -> gercek up/down sayim (MOCK degil). Universe =
# son scan sembolleri (cap ile hiz/temsil dengesi). Module cache (6h) — gunluk veri.
_BREADTH_BACKFILL_CACHE: dict = {}
_BREADTH_BACKFILL_TTL = 6 * 3600  # 6 saat
_BREADTH_BACKFILL_CAP = 120  # temsili evren (hiz: ~120 sembol ~18s, cache sonrasi 0s)


def _breadth_backfill(cap: int = _BREADTH_BACKFILL_CAP) -> tuple[list[int], list[int]]:
    """Scan evreninin gercek yfinance fiyat gecmisinden gunluk advance/decline.

    McClellan/Zweig icin 39+ gun breadth uretir (DB scan gecmisi yetersizken).
    cap sembol (son scan, ticker sirali — stabil) x 3-ay Close -> her gun
    close>prev advance, close<prev decline. Module cache 6 saat.

    Returns (advances, declines) kronolojik, veya ([],[]) fail.
    """
    key = ("breadth_backfill", cap)
    hit = _BREADTH_BACKFILL_CACHE.get(key)
    if hit is not None and (_now() - hit[0]) < _BREADTH_BACKFILL_TTL:
        return hit[1]
    try:
        from sqlalchemy import text as _sql_text
        from db_helpers import engine as _engine
        with _engine.connect() as conn:
            rows = conn.execute(_sql_text(
                "SELECT DISTINCT ticker FROM minervini_scans "
                "WHERE scan_date=(SELECT MAX(scan_date) FROM minervini_scans) "
                "ORDER BY ticker LIMIT :cap"
            ), {"cap": cap}).fetchall()
        syms = [r[0] for r in rows]
        if len(syms) < 20:
            return [], []
        import yfinance as yf
        df = yf.download(syms, period="3mo", interval="1d",
                         progress=False, auto_adjust=True)["Close"]
        if df is None or df.empty:
            return [], []
        chg = df.diff()
        advances = [int(x) for x in (chg > 0).sum(axis=1).tolist()][1:]  # ilk gun NaN
        declines = [int(x) for x in (chg < 0).sum(axis=1).tolist()][1:]
        result = (advances, declines)
        _BREADTH_BACKFILL_CACHE[key] = (_now(), result)
        return result
    except Exception:
        return [], []


@app.get("/api/market/extra-indicators", response_model=ExtraIndicatorsInfo)
def get_extra_indicators() -> ExtraIndicatorsInfo:
    """Faber 10-ay SMA + McClellan Oscillator + Zweig Breadth Thrust.

    Kaynak veri: SPY 252-bar yfinance (Faber) + breadth (McClellan/Zweig).
    P426: breadth icin once scan gecmisi (breadth_history_from_scans); 39 gunden
    az ise yfinance backfill (scan evreni gercek fiyat gecmisi). Yetersiz/fail ->
    helper data_sufficient False (MOCK YOK).
    """
    # Faber: SPY 252 günlük kapanış (P369 gerçek yfinance + fallback)
    spy_closes, _spy_vol = _index_closes_volumes("SPY", 400.0, 252)
    faber = compute_faber_timing(spy_closes)

    # McClellan + Zweig: önce scan geçmişi; McClellan 39 gün gerek, scan yetersizse backfill
    real_breadth = breadth_history_from_scans(days=60)
    breadth_source = "none"
    if real_breadth and len(real_breadth) >= MCCLELLAN_SLOW_EMA:
        advances = [a for a, _ in real_breadth]
        declines = [d for _, d in real_breadth]
        breadth_source = "scans"
    else:
        # P426: yfinance backfill (scan evreni gerçek fiyat geçmişi)
        bf_adv, bf_dec = _breadth_backfill()
        if bf_adv and len(bf_adv) >= MCCLELLAN_SLOW_EMA:
            advances, declines = bf_adv, bf_dec
            breadth_source = "backfill"
        elif real_breadth:
            # backfill da olmadi -> en azindan scan geçmişi (Zweig 10 gün için yeter)
            advances = [a for a, _ in real_breadth]
            declines = [d for _, d in real_breadth]
            breadth_source = "scans"
        else:
            advances, declines = [], []

    mcclellan = compute_mcclellan_oscillator(advances, declines)
    zweig = compute_zweig_breadth_thrust(advances, declines)

    return ExtraIndicatorsInfo(
        faber=faber, mcclellan=mcclellan, zweig=zweig, breadth_source=breadth_source,
    )


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


# ── Tarama Veri Tazeliği (P375) ──────────────────────────────────────────────
# Sn. Ferit "14 gün eski veri" acisinin onlenmesi. Scanner Cloud Run'da durursa
# kullanici bayat veriyle trade etmesin diye DataFreshnessBanner uyari gosterir.
SCAN_STALE_THRESHOLD_DAYS = 4  # Cuma->Sali normal max 4 takvim gun; >4 = tarama atlandi


class ScanFreshness(BaseModel):
    latest_scan_date: Optional[str] = None   # "2026-05-29" veya None
    is_stale: bool                            # True ise UI kirmizi banner gosterir
    calendar_days_old: Optional[int] = None
    threshold_days: int = SCAN_STALE_THRESHOLD_DAYS
    message: str


@app.get("/api/scan/freshness", response_model=ScanFreshness)
def get_scan_freshness() -> ScanFreshness:
    """Tarama verisi tazeligi: son minervini_scans tarihi kac gun eski.

    is_stale = calendar_days > 4 (hafta sonu normal Cum->Sali 4 gunu ASMAZ;
    >4 -> islem gunu taramasi atlandi). Basit + saglam (false-positive yok).
    Frontend DataFreshnessBanner sadece is_stale=True iken kirmizi uyari gosterir.
    """
    from datetime import date as _date
    latest = scan_latest_date()
    if latest is None:
        return ScanFreshness(
            latest_scan_date=None, is_stale=True, calendar_days_old=None,
            message="Tarama verisi yok (minervini_scans bos veya DB erisilemez).",
        )
    try:
        scan_d = _date.fromisoformat(str(latest)[:10])
    except ValueError:
        return ScanFreshness(
            latest_scan_date=latest, is_stale=False, calendar_days_old=None,
            message=f"Son tarama: {latest} (tarih parse edilemedi).",
        )
    cal_days = (_date.today() - scan_d).days
    stale = cal_days > SCAN_STALE_THRESHOLD_DAYS
    if stale:
        msg = (f"Tarama BAYAT: son tarama {latest}, {cal_days} gun once "
               f"(esik {SCAN_STALE_THRESHOLD_DAYS} gun). Cloud Run scanner kontrol et.")
    else:
        msg = f"Tarama guncel: {latest} ({cal_days} gun once)."
    return ScanFreshness(
        latest_scan_date=latest, is_stale=stale,
        calendar_days_old=cal_days, message=msg,
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
    # KARAR ADAY #724 (24 May 2026) — Mark Profili rozetleri (Optional)
    # MarkSignalsBlock asagida tanimli (forward ref) — dict olarak tutuluyor
    # JSON serialization sorunsuz, frontend MarkSignals interface ile parse.
    mark_signals: Optional[dict] = None
    # KARAR #733 alt-paket (Paket 82, 26 May 2026): Pivot Breakout status
    # P81 Sinyaller paten — Watchlist'te de AL/Zayıf/Yakın/Altı kolon görünür
    pivot_status: Optional[Literal["CONFIRMED", "WEAK", "NEAR_PIVOT", "BELOW_PIVOT"]] = None
    # P416 (31 May 2026 — Sprint 4-bis Bağıl Hacim): Mark TLSMW Böl. 6
    # "Hacim teyit" canon. Bugün / 50-gün ortalama oranı (1.0 = ortalama,
    # 1.5+ = patlama, 0.7- = sönük). quanfina_math.compute_relative_volume
    # hesabı, yfinance live volumes (5dk cache). UI rozet eşikleri:
    # ≥1.5 yeşil "patlama" / 1.0-1.5 nötr / <0.7 kırmızı "sönük".
    relative_volume: Optional[float] = None


def _compute_live_mark_signals(symbol: str, price: float) -> dict:
    """KARAR #733 alt-paket (Paket 144, 26 May 2026): yfinance gerçek Mark canon.

    Sembol için yfinance OHLCV çek + 4 helper çağrısı (RS Rating + Stage
    Transition + Climax Run + Volume Asymmetry). MOCK _STOCK_MARK_SIGNALS
    ile birleştirir (helper sonuç bulamazsa MOCK kalır).

    Cache: _OHLCV_CACHE 5 dakika TTL (sembol seri çağrı 0s).
    Fail: yfinance fail → MOCK fallback.

    Paket 145 alt-katman: _MARK_SIGNALS_CACHE 5 dk TTL (helper hesap GIL).
    """
    # Paket 145 alt-katman: sembol başına cache (NumPy hesap maliyetli)
    now = _time_module.time()
    cached = _MARK_SIGNALS_CACHE.get(symbol)
    if cached:
        ts, result = cached
        if now - ts < _MARK_SIGNALS_CACHE_TTL_SEC:
            return dict(result)  # Defensive copy

    result: dict = {}
    # 1. DB base (Migration 004-009 sonrasi gercek tarama verisi — scanner doldurur).
    #    minervini_scans son scan_date Mark canon kolonlari. NULL ise bos dict.
    db_signals = mark_signals_get_by_symbol(symbol)
    if db_signals:
        result.update(db_signals)
    else:
        # Gecis donemi fallback: DB henuz Mark kolonlarini doldurmadiysa MOCK.
        # scanner.py sonraki run'da kolonlari doldurunca DB otomatik one gecer,
        # MOCK gereksizlesir (KARAR ADAY #735 — MOCK->DB tam gecis).
        mock = _STOCK_MARK_SIGNALS.get(symbol, {})
        if mock:
            result.update(mock)

    # 2. yfinance OHLCV çek (cache varsa 0s)
    bars = _fetch_ohlcv_real(symbol, 252)
    if not bars or len(bars) < 60:
        return result  # MOCK ile devam, live overlay yok

    closes = [b.close for b in bars]
    volumes = [b.volume for b in bars]

    # 3. RS Rating gerçek (vs SPY benchmark)
    # yfinance 1y = 251 bar (helper default 252 lookback) — 250 ile çağır
    try:
        spy_bars = _fetch_ohlcv_real("SPY", 252)
        if spy_bars and len(spy_bars) >= 250:
            spy_closes = [b.close for b in spy_bars]
            if len(closes) >= 250:
                lookback = min(250, len(closes), len(spy_closes))
                rs = compute_relative_strength_rating(closes, spy_closes, lookback_days=lookback)
                if rs.get("rs_rating") is not None:
                    result["live_rs_rating"] = rs.get("rs_rating")
                    result["live_rs_category"] = rs.get("category")
    except Exception:
        pass

    # 4. Stage Transition gerçek (30W MA + slope)
    try:
        stage = compute_stage_transition(closes, volumes)
        if stage.get("category"):
            result["stage_category"] = stage.get("category")
            result["live_stage_days_above"] = stage.get("days_above_ma")
    except Exception:
        pass

    # 5. Climax Run gerçek (parabolic + exhaustion)
    try:
        opens = [b.open for b in bars]
        climax = compute_climax_run(closes, opens, volumes)
        if climax.get("category"):
            result["climax_category"] = climax.get("category")
    except Exception:
        pass

    # Paket 145 alt-katman: cache yaz (sonraki çağrılar 0s)
    _MARK_SIGNALS_CACHE[symbol] = (now, dict(result))
    return result


def _enrich_with_mark_signals(row: WatchlistRow) -> WatchlistRow:
    """KARAR ADAY #724 + Paket 144 (26 May 2026 yfinance overlay).

    Watchlist satirina Mark Profili rozetlerini ekler:
    - mark_signals (MOCK + yfinance gerçek RS/Stage/Climax)
    - pivot_status (gerçek pivot kırılım)
    """
    updates: dict = {}
    # P144: yfinance live overlay (MOCK base + gerçek 3 alan)
    live_signals = _compute_live_mark_signals(row.symbol, row.price)
    if live_signals:
        updates["mark_signals"] = live_signals
    pivot_status = _compute_signal_pivot_status(row.symbol, row.price)
    if pivot_status:
        updates["pivot_status"] = pivot_status
    # P416 (31 May 2026): Relative Volume (Mark TLSMW Bol. 6 "Hacim teyit").
    # yfinance volumes (5dk cache via _get_ohlcv) -> compute_relative_volume
    # 50-gun ortalama. Yetersiz veri/exception -> None (UI render etmez).
    try:
        bars = _get_ohlcv(row.symbol, row.price)
        if bars and len(bars) >= 52:  # 50 gun + bugun + buffer
            volumes = [b.volume for b in bars]
            rv = compute_relative_volume(volumes)
            if rv.get("rel_vol") is not None:
                updates["relative_volume"] = float(rv["rel_vol"])
    except Exception:
        pass
    if updates:
        return row.model_copy(update=updates)
    return row


def _enrich_watchlist_batch(rows: list[WatchlistRow]) -> list[WatchlistRow]:
    """KARAR #733 alt-paket (Paket 145, 26 May 2026): Paralel watchlist enrich.

    10 sembol seri yfinance fetch = ~11s. ThreadPoolExecutor ile paralel = ~2s.
    Cache hit'lerde 0s. SPY benchmark için bir kez fetch (paralel başlamadan ön-warm).
    """
    if not rows:
        return rows

    # Pre-warm SPY (RS Rating benchmark) — paralelden önce
    try:
        _fetch_ohlcv_real("SPY", 252)
    except Exception:
        pass

    from concurrent.futures import ThreadPoolExecutor

    def _enrich_one(row: WatchlistRow) -> WatchlistRow:
        try:
            return _enrich_with_mark_signals(row)
        except Exception:
            return row

    # 10 sembol için 10 worker yeterli (yfinance internal rate limit'i var)
    with ThreadPoolExecutor(max_workers=10) as pool:
        enriched = list(pool.map(_enrich_one, rows))
    return enriched


@app.get("/api/watchlist", response_model=list[WatchlistRow])
def get_watchlist() -> list[WatchlistRow]:
    # DB unreachable -> MOCK fallback (Sinyaller pateni — Kural #20 UX)
    # Sn. Ferit: DB down ortamda UI demo dolu görünsün, banner sadece API'da
    if not db_health_check():
        rows = [
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
        return _enrich_watchlist_batch(rows)

    try:
        rows = [WatchlistRow(**r) for r in watchlist_get_all()]
        return _enrich_watchlist_batch(rows)
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
    # P387: validation kampanyasi — TradeCreate gt=0 disiplini paralel.
    symbol: str = Field(min_length=1, max_length=10, description="US ticker (1-10 karakter)")
    strategy: Literal["minervini", "carr"]
    status: Literal["watch", "on_deck", "focus", "buy"]
    setup_type: Optional[str] = None
    pivot_price: Optional[float] = Field(default=None, gt=0, description="Pivot fiyati > 0")
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
    # Paket 148 (26 May 2026): paper trading için popüler eklemeler
    "TSLA": {"name": "Tesla Inc",              "industry": "Auto Manufacturers",      "market_cap": "$780B", "sector": "Consumer Discretionary"},
    "GOOG": {"name": "Alphabet Inc Class C",   "industry": "Internet Services",       "market_cap": "$2.2T", "sector": "Technology"},
    "BRK.B":{"name": "Berkshire Hathaway B",   "industry": "Holding Company",         "market_cap": "$925B", "sector": "Financials"},
    "JNJ":  {"name": "Johnson & Johnson",      "industry": "Pharmaceuticals",         "market_cap": "$385B", "sector": "Health Care"},
    "UNH":  {"name": "UnitedHealth Group",     "industry": "Health Insurance",        "market_cap": "$540B", "sector": "Health Care"},
    "XOM":  {"name": "Exxon Mobil Corp",       "industry": "Oil & Gas",               "market_cap": "$450B", "sector": "Energy"},
    "DIS":  {"name": "Walt Disney Co",         "industry": "Entertainment",           "market_cap": "$170B", "sector": "Communication Services"},
    "BAC":  {"name": "Bank of America",        "industry": "Banking",                 "market_cap": "$310B", "sector": "Financials"},
    "PFE":  {"name": "Pfizer Inc",             "industry": "Pharmaceuticals",         "market_cap": "$155B", "sector": "Health Care"},
    "KO":   {"name": "Coca-Cola Co",           "industry": "Beverages",               "market_cap": "$265B", "sector": "Consumer Staples"},
    "PEP":  {"name": "PepsiCo Inc",            "industry": "Beverages",               "market_cap": "$235B", "sector": "Consumer Staples"},
    "MCD":  {"name": "McDonald's Corp",        "industry": "Restaurants",             "market_cap": "$210B", "sector": "Consumer Discretionary"},
    "NKE":  {"name": "Nike Inc",               "industry": "Apparel",                 "market_cap": "$135B", "sector": "Consumer Discretionary"},
    "SBUX": {"name": "Starbucks Corp",         "industry": "Restaurants",             "market_cap": "$110B", "sector": "Consumer Discretionary"},
    "BA":   {"name": "Boeing Co",              "industry": "Aerospace",               "market_cap": "$135B", "sector": "Industrials"},
    "GE":   {"name": "General Electric",       "industry": "Industrial Conglomerate", "market_cap": "$170B", "sector": "Industrials"},
    "CAT":  {"name": "Caterpillar Inc",        "industry": "Heavy Machinery",         "market_cap": "$165B", "sector": "Industrials"},
    "IBM":  {"name": "IBM Corp",               "industry": "IT Services",             "market_cap": "$165B", "sector": "Technology"},
    "INTC": {"name": "Intel Corp",             "industry": "Semiconductors",          "market_cap": "$135B", "sector": "Technology"},
    "QCOM": {"name": "Qualcomm Inc",           "industry": "Semiconductors",          "market_cap": "$180B", "sector": "Technology"},
    "MU":   {"name": "Micron Technology",      "industry": "Semiconductors",          "market_cap": "$135B", "sector": "Technology"},
    "F":    {"name": "Ford Motor Co",          "industry": "Auto Manufacturers",      "market_cap": "$45B",  "sector": "Consumer Discretionary"},
    "GM":   {"name": "General Motors",         "industry": "Auto Manufacturers",      "market_cap": "$52B",  "sector": "Consumer Discretionary"},
    "GS":   {"name": "Goldman Sachs Group",    "industry": "Investment Banking",      "market_cap": "$135B", "sector": "Financials"},
    "WFC":  {"name": "Wells Fargo & Co",       "industry": "Banking",                 "market_cap": "$220B", "sector": "Financials"},
    "C":    {"name": "Citigroup Inc",          "industry": "Banking",                 "market_cap": "$110B", "sector": "Financials"},
    "T":    {"name": "AT&T Inc",               "industry": "Telecom",                 "market_cap": "$150B", "sector": "Communication Services"},
    "VZ":   {"name": "Verizon Communications", "industry": "Telecom",                 "market_cap": "$170B", "sector": "Communication Services"},
    "SPY":  {"name": "SPDR S&P 500 ETF",       "industry": "ETF",                     "market_cap": "$520B", "sector": "ETF"},
    "QQQ":  {"name": "Invesco QQQ Trust",      "industry": "ETF",                     "market_cap": "$280B", "sector": "ETF"},
    # Paket 186 (26 May 2026): Sembol evren genişletme — paper trading
    # autocomplete daha zengin. AI/Semiconductors + Software + Biotech +
    # Defense + Energy + REIT + ETF kategorileri.
    # AI / Yarı İletken eklemeleri
    "ARM":  {"name": "Arm Holdings PLC",       "industry": "Semiconductors",          "market_cap": "$135B", "sector": "Technology"},
    "SMCI": {"name": "Super Micro Computer",   "industry": "Server Hardware",         "market_cap": "$25B",  "sector": "Technology"},
    "AMAT": {"name": "Applied Materials",       "industry": "Semiconductor Equipment", "market_cap": "$152B", "sector": "Technology"},
    "LRCX": {"name": "Lam Research Corp",      "industry": "Semiconductor Equipment", "market_cap": "$110B", "sector": "Technology"},
    "KLAC": {"name": "KLA Corp",               "industry": "Semiconductor Equipment", "market_cap": "$95B",  "sector": "Technology"},
    "ASML": {"name": "ASML Holding",           "industry": "Semiconductor Equipment", "market_cap": "$290B", "sector": "Technology"},
    "TSM":  {"name": "Taiwan Semiconductor ADR","industry": "Semiconductors",         "market_cap": "$735B", "sector": "Technology"},
    # Software / SaaS
    "NOW":  {"name": "ServiceNow Inc",         "industry": "Enterprise Software",     "market_cap": "$165B", "sector": "Technology"},
    "PANW": {"name": "Palo Alto Networks",     "industry": "Cybersecurity",           "market_cap": "$95B",  "sector": "Technology"},
    "CRWD": {"name": "CrowdStrike Holdings",   "industry": "Cybersecurity",           "market_cap": "$78B",  "sector": "Technology"},
    "ZS":   {"name": "Zscaler Inc",            "industry": "Cybersecurity",           "market_cap": "$28B",  "sector": "Technology"},
    "SNOW": {"name": "Snowflake Inc",          "industry": "Cloud Data",              "market_cap": "$58B",  "sector": "Technology"},
    "DDOG": {"name": "Datadog Inc",            "industry": "Monitoring SaaS",         "market_cap": "$42B",  "sector": "Technology"},
    "NET":  {"name": "Cloudflare Inc",         "industry": "Edge Computing",          "market_cap": "$28B",  "sector": "Technology"},
    "MDB":  {"name": "MongoDB Inc",            "industry": "Database SaaS",           "market_cap": "$25B",  "sector": "Technology"},
    # Streaming / Comm
    "ROKU": {"name": "Roku Inc",               "industry": "Streaming Platform",      "market_cap": "$9B",   "sector": "Communication Services"},
    "SPOT": {"name": "Spotify Technology",     "industry": "Music Streaming",         "market_cap": "$60B",  "sector": "Communication Services"},
    # Auto / EV
    "RIVN": {"name": "Rivian Automotive",      "industry": "EV Manufacturer",         "market_cap": "$13B",  "sector": "Consumer Discretionary"},
    "LCID": {"name": "Lucid Group",            "industry": "EV Manufacturer",         "market_cap": "$8B",   "sector": "Consumer Discretionary"},
    # Biotech / Pharma
    "MRNA": {"name": "Moderna Inc",            "industry": "Biotech",                 "market_cap": "$28B",  "sector": "Health Care"},
    "REGN": {"name": "Regeneron Pharmaceuticals","industry": "Biotech",               "market_cap": "$100B", "sector": "Health Care"},
    "VRTX": {"name": "Vertex Pharmaceuticals", "industry": "Biotech",                 "market_cap": "$110B", "sector": "Health Care"},
    "ABBV": {"name": "AbbVie Inc",             "industry": "Pharmaceuticals",         "market_cap": "$315B", "sector": "Health Care"},
    "LLY":  {"name": "Eli Lilly",              "industry": "Pharmaceuticals",         "market_cap": "$680B", "sector": "Health Care"},
    "MRK":  {"name": "Merck & Co",             "industry": "Pharmaceuticals",         "market_cap": "$315B", "sector": "Health Care"},
    # Defense / Industrials
    "LMT":  {"name": "Lockheed Martin",        "industry": "Defense",                 "market_cap": "$115B", "sector": "Industrials"},
    "RTX":  {"name": "RTX Corp",               "industry": "Defense & Aerospace",     "market_cap": "$160B", "sector": "Industrials"},
    "NOC":  {"name": "Northrop Grumman",       "industry": "Defense",                 "market_cap": "$72B",  "sector": "Industrials"},
    "GD":   {"name": "General Dynamics",       "industry": "Defense",                 "market_cap": "$78B",  "sector": "Industrials"},
    # Energy
    "CVX":  {"name": "Chevron Corp",           "industry": "Oil & Gas Integrated",    "market_cap": "$285B", "sector": "Energy"},
    "COP":  {"name": "ConocoPhillips",         "industry": "Oil & Gas E&P",           "market_cap": "$125B", "sector": "Energy"},
    "SLB":  {"name": "Schlumberger NV",        "industry": "Oilfield Services",       "market_cap": "$65B",  "sector": "Energy"},
    # Crypto-adjacent
    "MSTR": {"name": "MicroStrategy Inc",      "industry": "Bitcoin Treasury",        "market_cap": "$28B",  "sector": "Technology"},
    "MARA": {"name": "Marathon Digital",       "industry": "Bitcoin Mining",          "market_cap": "$5B",   "sector": "Technology"},
    # ETFs (sektör + tematik)
    "IWM":  {"name": "iShares Russell 2000 ETF","industry": "ETF",                    "market_cap": "$65B",  "sector": "ETF"},
    "DIA":  {"name": "SPDR Dow Jones ETF",     "industry": "ETF",                     "market_cap": "$35B",  "sector": "ETF"},
    "XLK":  {"name": "Tech Select Sector ETF", "industry": "ETF Tech",                "market_cap": "$72B",  "sector": "ETF"},
    "XLF":  {"name": "Financial Select Sector ETF","industry": "ETF Financials",      "market_cap": "$45B",  "sector": "ETF"},
    "XLE":  {"name": "Energy Select Sector ETF","industry": "ETF Energy",             "market_cap": "$38B",  "sector": "ETF"},
    "XLV":  {"name": "Health Care Select ETF", "industry": "ETF Health",              "market_cap": "$42B",  "sector": "ETF"},
    "XLI":  {"name": "Industrial Select ETF",  "industry": "ETF Industrials",         "market_cap": "$18B",  "sector": "ETF"},
    "XLU":  {"name": "Utilities Select ETF",   "industry": "ETF Utilities",           "market_cap": "$14B",  "sector": "ETF"},
    "XLP":  {"name": "Consumer Staples ETF",   "industry": "ETF Staples",             "market_cap": "$17B",  "sector": "ETF"},
    "XLY":  {"name": "Consumer Discretionary ETF","industry": "ETF Discretionary",    "market_cap": "$22B",  "sector": "ETF"},
    "XLC":  {"name": "Communication Services ETF","industry": "ETF Communication",    "market_cap": "$22B",  "sector": "ETF"},
    "XLB":  {"name": "Materials Select ETF",   "industry": "ETF Materials",           "market_cap": "$5B",   "sector": "ETF"},
    "XLRE": {"name": "Real Estate Select ETF", "industry": "ETF REIT",                "market_cap": "$6B",   "sector": "ETF"},
    # Fintech / Payments
    "PYPL": {"name": "PayPal Holdings",        "industry": "Payments",                "market_cap": "$70B",  "sector": "Financials"},
    "SQ":   {"name": "Block Inc",              "industry": "Payments",                "market_cap": "$45B",  "sector": "Financials"},
    "AXP":  {"name": "American Express",       "industry": "Credit Cards",            "market_cap": "$190B", "sector": "Financials"},
    # Retail / Consumer
    "TGT":  {"name": "Target Corp",            "industry": "Retail Discount",         "market_cap": "$72B",  "sector": "Consumer Discretionary"},
    "LOW":  {"name": "Lowe's Companies",       "industry": "Home Improvement",        "market_cap": "$135B", "sector": "Consumer Discretionary"},
}

_STOCK_BY_SYM: dict[str, MinerviniStock] = {s.symbol: s for s in MOCK_STOCKS}


# =============================================================================
# Paket 148 (26 May 2026): Sembol arama autocomplete
# Sn. Ferit paper trading: AddRowDialog'da hızlı sembol bulma
# =============================================================================

class SymbolSearchResult(BaseModel):
    """Sembol arama sonucu — autocomplete için minimal.

    Paket 211 (27 May 2026): source field — Quanfina evren vs yfinance fallback ayrımı.
    Kullanıcı dropdown'da "universe" = _STOCK_META hit (bilinen), "yfinance" =
    yfinance Ticker.info ile doğrulanmış (evren dışı) bilgisini görür.
    """
    symbol: str
    name: str
    sector: str
    source: Literal["universe", "yfinance"] = "universe"


@app.get("/api/symbols/search", response_model=list[SymbolSearchResult])
def search_symbols(q: str, limit: int = 10) -> list[SymbolSearchResult]:
    """Quanfina hisse evreninde sembol arama (_STOCK_META 56 sembol).

    Sıralama: önce symbol prefix match (NVDA, NVD), sonra company name
    prefix match (Apple → AAPL), sonra contains match. Case-insensitive.

    Production: minervini_scans + symbol_lists tablo birleşimi (gelecek).
    """
    if not q or len(q) < 1:
        return []
    q_upper = q.upper().strip()
    q_lower = q.lower().strip()

    prefix_sym: list[SymbolSearchResult] = []
    prefix_name: list[SymbolSearchResult] = []
    contains: list[SymbolSearchResult] = []
    seen: set[str] = set()

    for sym, meta in _STOCK_META.items():
        if sym in seen:
            continue
        sym_upper = sym.upper()
        name = meta.get("name", sym)
        name_lower = name.lower()
        sector = meta.get("sector") or meta.get("industry", "Unknown")
        # P211: source="universe" — Quanfina bilinen evren
        result = SymbolSearchResult(symbol=sym, name=name, sector=sector, source="universe")
        if sym_upper.startswith(q_upper):
            prefix_sym.append(result)
            seen.add(sym)
        elif name_lower.startswith(q_lower):
            prefix_name.append(result)
            seen.add(sym)
        elif q_upper in sym_upper or q_lower in name_lower:
            contains.append(result)
            seen.add(sym)

    combined = prefix_sym + prefix_name + contains

    # Paket 164 (26 May 2026): yfinance fallback — Quanfina evreni 56 sembol dışı.
    # Kullanıcı tam sembol yazıp boş dönerse yfinance Ticker.info ile doğrula.
    # Paket 167 (26 May 2026): 5 dk TTL cache — aynı sembol için 0s (negative cache dahil).
    if not combined and 2 <= len(q_upper) <= 6 and q_upper.replace(".", "").isalpha() and not _YF_DISABLED:
        now = _time_module.time()
        cached = _YF_SYMBOL_CACHE.get(q_upper)
        if cached:
            ts, payload = cached
            if now - ts < _YF_SYMBOL_CACHE_TTL_SEC:
                if payload:
                    # P211: cached payload zaten source="yfinance" içerir
                    combined.append(SymbolSearchResult(**payload))
                # payload None ise: önceki sorguda yfinance bulamamış — yine boş dön
                return combined[:limit]

        # Cache miss — yfinance fetch
        result_payload: Optional[dict] = None
        try:
            import yfinance as yf
            ticker = yf.Ticker(q_upper)
            info = ticker.info or {}
            name = info.get("longName") or info.get("shortName")
            sector = info.get("sector") or info.get("industry") or "Unknown"
            if name and (info.get("regularMarketPrice") or info.get("previousClose")):
                # P211: source="yfinance" — evren dışı, yfinance ile doğrulandı
                result_payload = {
                    "symbol": q_upper, "name": name, "sector": sector,
                    "source": "yfinance",
                }
                combined.append(SymbolSearchResult(**result_payload))
        except Exception:
            pass  # yfinance fail — sessizce geç
        # Pozitif veya negatif cache yaz (None = "bulunamadı")
        _YF_SYMBOL_CACHE[q_upper] = (now, result_payload)

    return combined[:limit]

# KARAR ADAY #723 (24 May 2026) — Hisse detay Mark Profil rozetleri MOCK feed.
# Cloud SQL Migration 004-007 uygulanana kadar UI'in canli gosterimi icin
# ornek sembollere Mark sinyalleri assign edilir. Production'da scanner.py
# minervini_scans tablo kolonlarindan okunacak (KARAR #470 paten).
_STOCK_MARK_SIGNALS: dict[str, dict] = {
    "NVDA": {
        "vcp_quality_score": "EXCELLENT",
        "vcp_ready_score": 85,
        "power_play_pass": True,
        "tennis_ball_pattern": "TENNIS_BALL",
        "volume_asymmetry_tier": "healthy",
        "carr_stage": 2,  # Advancing - Mark+Carr alim fazi
        # KARAR #733 alt-paket (Paket 95): Climax category MOCK — Mark TLSMW Ch 9
        # CLIMAX_TOP = parabolic + exhaustion gap (Mark "SAT" sinyali).
        "climax_category": "CLIMAX_TOP",
        # KARAR #733 alt-paket (Paket 123, 26 May 2026): Stage Transition MOCK
        # Mark TLSMW Ch 4 — 30W MA bazli Stage 1->2 gecis.
        "stage_category": "STAGE_2_MATURE",
    },
    "MSFT": {
        "vcp_quality_score": "PASS",
        "vcp_ready_score": 72,
        "power_play_pass": False,
        "volume_asymmetry_tier": "healthy",
        "carr_stage": 2,
        "stage_category": "CONFIRMED_STAGE_2",
    },
    "AVGO": {
        "vcp_quality_score": "EXCELLENT",
        "power_play_pass": True,
        "tennis_ball_pattern": "TENNIS_BALL",
        "carr_stage": 2,
        "stage_category": "EARLY_STAGE_2",
    },
    "AMD": {
        "vcp_ready_score": 78,
        "tennis_ball_pattern": "TENNIS_BALL",
        "volume_asymmetry_tier": "healthy",
        "carr_stage": 2,
    },
    "META": {
        "vcp_quality_score": "PASS",
        "volume_asymmetry_tier": "neutral",
        "carr_stage": 3,  # Topping - cikis hazirlik
    },
    "TSLA": {
        "volume_asymmetry_tier": "distribution",  # uyari rozet
        "carr_stage": 4,  # Declining - uzak dur
    },
}


class MarkSignalsBlock(BaseModel):
    """KARAR ADAY #723 — Hisse detay Mark Profil rozetleri.
    Cloud SQL Migration 004-007 uygulanmasi sonrasi populate edilir.
    Tum alanlar Optional — UI undefined ise rozet gostermez (graceful)."""
    vcp_quality_score: Optional[Literal["EXCELLENT", "PASS"]] = None
    vcp_ready_score: Optional[int] = None
    power_play_pass: Optional[bool] = None
    tennis_ball_pattern: Optional[Literal["TENNIS_BALL", "partial", "none"]] = None
    volume_asymmetry_tier: Optional[Literal["healthy", "neutral", "distribution"]] = None
    code_33_pattern: Optional[Literal["CODE_33", "partial", "none"]] = None
    # KARAR ADAY #735 (24 May 2026): Carr Stage rozet (Mark+Carr birleşik DRY)
    carr_stage: Optional[Literal[1, 2, 3, 4]] = None


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
    # KARAR ADAY #723 — Mark Profili rozetleri (Migration 004-007 sonrasi canli)
    mark_signals: Optional[MarkSignalsBlock] = None


class OhlcvBar(BaseModel):
    time: str         # ISO date "YYYY-MM-DD"
    open: float
    high: float
    low: float
    close: float
    volume: int


# KARAR #733 alt-paket (Paket 140, 26 May 2026): yfinance gerçek OHLCV
# entegrasyonu (AÇIK KONU #75 RESOLVED). Paper trading için gerçek piyasa
# verisi şart — MOCK feed yetersiz (NVDA gerçek RS/Pivot/ATR hesabı için).
#
# Cache: in-memory dict + 5 dakika TTL (Mark canon helper sweep × 10 sembol
# = 10 yfinance çağrısı → ardışık sayfa yüklemelerinde 0 çağrı).
# Fallback: yfinance fail → _generate_ohlcv MOCK (UX kesintisiz).
import time as _time_module

_OHLCV_CACHE: dict[str, tuple[float, list]] = {}
_OHLCV_CACHE_TTL_SEC = 300  # 5 dakika

# Paket 145 alt-katman: live Mark signals (RS + Stage + Climax) cache.
# 10 sembol × 3 helper × ~0.7s = ~21s seri Python (NumPy GIL paralel olmaz).
# Cache ile 5 dk içinde tekrar isteklerinde watchlist endpoint <0.5s.
_MARK_SIGNALS_CACHE: dict[str, tuple[float, dict]] = {}
_MARK_SIGNALS_CACHE_TTL_SEC = 300  # 5 dakika

# Paket 167 (26 May 2026): yfinance symbol search fallback cache.
# P164 evren dışı sembol için yfinance Ticker.info çağırır (~1-2s).
# 5 dk TTL cache — aynı sembol için 0s. None = "yfinance bulamadı" (negative cache).
_YF_SYMBOL_CACHE: dict[str, tuple[float, Optional[dict]]] = {}
_YF_SYMBOL_CACHE_TTL_SEC = 300
_YF_DISABLED = False  # yfinance import veya runtime fail -> True (tek seferlik)


def _fetch_ohlcv_real(symbol: str, n_bars: int = 252) -> Optional[list[OhlcvBar]]:
    """yfinance gerçek OHLCV verisi + cache + fail-safe.

    Returns:
        Bar list veya None (fail). None ise caller _generate_ohlcv MOCK
        fallback'e düşer.
    """
    global _YF_DISABLED
    if _YF_DISABLED:
        return None

    sym = symbol.upper()
    now = _time_module.time()

    # Cache hit? (P435: cache TAM 2y saklar, çağrana göre n_bars dilimlenir)
    if sym in _OHLCV_CACHE:
        ts, bars = _OHLCV_CACHE[sym]
        if now - ts < _OHLCV_CACHE_TTL_SEC:
            return bars[-n_bars:] if n_bars and len(bars) > n_bars else bars

    # yfinance fetch — P435: MA200 warmup için 2y çek (n_bars>252 grafik isteği).
    # MA200 = 200 bar warmup gerektirir; 1y (252) çekince grafik son ~52 günde
    # MA200 başlıyordu. 2y fetch + n_bars dilim → grafik (n_bars=460) son 252'de
    # MA200 tam dolu (200 warmup + 252 görünür). Compute helper'lar (n_bars=252)
    # son 252'yi alır — davranış değişmez (recent data aynı).
    try:
        import yfinance as yf
        ticker = yf.Ticker(sym)
        hist = ticker.history(period="2y")
        if len(hist) < 10:
            return None
        bars: list[OhlcvBar] = []
        for date_idx, row in hist.iterrows():
            o = float(row["Open"]); h = float(row["High"]); lo = float(row["Low"])
            c = float(row["Close"]); v = float(row["Volume"])
            # yfinance NaN satirlari atla (settle olmamis gun / split-gap / partial day).
            # float(nan) raise ETMEZ -> close=nan OhlcvBar JSON serialize 500'u
            # (/api/stock/.../ohlcv + downstream compute helper NaN zehirlenmesi).
            # NaN != NaN ozelligi ile tespit (pandas import gerekmez).
            if o != o or h != h or lo != lo or c != c or v != v:
                continue
            bars.append(OhlcvBar(
                time=date_idx.strftime("%Y-%m-%d"),
                open=o, high=h, low=lo, close=c, volume=int(v),
            ))
        _OHLCV_CACHE[sym] = (now, bars)  # P435: tam 2y sakla
        return bars[-n_bars:] if n_bars and len(bars) > n_bars else bars
    except ImportError:
        _YF_DISABLED = True
        return None
    except Exception:
        # Network / rate limit / symbol unknown — MOCK fallback
        return None


def _get_ohlcv(symbol: str, end_price: float, n_bars: int = 252) -> list[OhlcvBar]:
    """yfinance öncelik, fail durumunda MOCK fallback.

    Bu helper tüm endpoint'ler için tek giriş noktası — Mark canon helper'lar
    gerçek piyasa verisi görür (paper trading şartı), kesinti olursa MOCK
    kullanılır (UX kesintisiz).
    """
    real = _fetch_ohlcv_real(symbol, n_bars)
    if real is not None and len(real) >= 10:
        return real
    return _generate_ohlcv(symbol, end_price, n_bars)


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


def _fetch_scan_symbol_data(sym: str) -> dict | None:
    """Tarama tablolarindan (minervini_scans, _fundamental_only, _fundamental_scans)
    sembol fiyat + RS bilgisini cek. Tarama sonucu hisselerde grafik acilmali.

    24 May 2026 — KARAR ADAY #498 (Hisse Detay Generic Fallback):
      Sn. Ferit "hisselere tıklandığında grafik açılmıyor" raporu.
      Sebep: MOCK_STOCKS + watchlist sınırlı, AXTI/SNDK gibi tarama sonucu
      semboller eksikti → 404. Fix: scan tablolarından lookup.
    """
    from sqlalchemy import text as sql_text
    from api.db_helpers import engine
    # En son scan_date'ten symbol cek — 3 tablo birden (öncelik scans > fundamental_only > fundamental_scans)
    for table in ("minervini_scans", "minervini_fundamental_only", "minervini_fundamental_scans"):
        try:
            with engine.connect() as conn:
                result = conn.execute(sql_text(f"""
                    SELECT ticker, price, rs_ibd, company, sector, industry
                    FROM {table}
                    WHERE ticker = :sym
                      AND scan_date = (SELECT MAX(scan_date) FROM {table})
                    LIMIT 1
                """), {"sym": sym})
                row = result.first()
                if row:
                    return {
                        "ticker": row[0],
                        "price": float(row[1]) if row[1] is not None else 100.0,
                        "rs_ibd": int(round(float(row[2]))) if row[2] is not None else 50,
                        "company": row[3] or sym,
                        "sector": row[4] or "—",
                        "industry": row[5] or "—",
                    }
        except (OperationalError, Exception):
            continue
    return None


@app.get("/api/stock/{symbol}/info", response_model=StockInfo)
def get_stock_info(symbol: str) -> StockInfo:
    sym = symbol.upper()
    stock = _STOCK_BY_SYM.get(sym)
    meta  = _STOCK_META.get(sym, {})
    # KARAR ADAY #723 + #735 — Mark Profili rozetleri.
    # Migration 004-009 sonrasi DB-oncelik (minervini_scans), DB bos ise MOCK fallback.
    raw_signals = mark_signals_get_by_symbol(sym) or _STOCK_MARK_SIGNALS.get(sym)
    mark_signals = MarkSignalsBlock(**raw_signals) if raw_signals else None

    # DB down ortamda hisse detay sayfasi calismali — watchlist bos liste fallback
    try:
        active = [WatchlistRow(**r) for r in watchlist_get_all() if r["symbol"] == sym]
    except OperationalError:
        active = []

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
            mark_signals=mark_signals,
        )
    if active:
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
            mark_signals=mark_signals,
        )
    # 24 May 2026 — Tarama sonucu sembollerinde fallback (KARAR ADAY #498)
    # Sn. Ferit raporu: "hisselere tıklandığında grafik açılmıyor"
    scan_data = _fetch_scan_symbol_data(sym)
    if scan_data:
        return StockInfo(
            symbol=sym,
            name=scan_data["company"],
            sector=scan_data["sector"],
            industry=scan_data["industry"],
            market_cap=meta.get("market_cap", "—"),
            price=scan_data["price"],
            change_pct=0.0,
            rs_rating=scan_data["rs_ibd"],
            active_strategies=[],
            mark_signals=mark_signals,
        )
    # Son fallback — Generic MOCK (DB down + scan yok), grafik acilsin
    return StockInfo(
        symbol=sym,
        name=sym,
        sector="—",
        industry="—",
        market_cap="—",
        price=100.0,
        change_pct=0.0,
        rs_rating=50,
        active_strategies=[],
        mark_signals=mark_signals,
    )


# KARAR #733 alt-paket (Paket 77, 25 May 2026): overhead_supply endpoint
class OverheadSupplyInfo(BaseModel):
    category: Optional[Literal["HEAVY", "MODERATE", "NONE"]] = None
    overhead_price: Optional[float] = None
    drop_pct: Optional[float] = None
    proximity_pct: Optional[float] = None
    mark_says: str


@app.get("/api/stock/{symbol}/overhead", response_model=OverheadSupplyInfo)
def get_overhead_supply(symbol: str) -> OverheadSupplyInfo:
    """Mark TLSMW Ch 10 Overhead Supply hesabı (P76 helper wire)."""
    sym = symbol.upper()
    stock = _STOCK_BY_SYM.get(sym)
    if stock:
        price = stock.price
    else:
        try:
            wl = [r for r in watchlist_get_all() if r["symbol"] == sym]
        except OperationalError:
            wl = []
        if wl:
            price = float(wl[0]["price"])
        else:
            scan_data = _fetch_scan_symbol_data(sym)
            price = scan_data["price"] if scan_data else 100.0
    bars = _get_ohlcv(sym, price)
    closes = [b.close for b in bars]
    result = compute_overhead_supply(closes)
    return OverheadSupplyInfo(
        category=result.get("category"),
        overhead_price=result.get("overhead_price"),
        drop_pct=result.get("drop_pct"),
        proximity_pct=result.get("proximity_pct"),
        mark_says=result.get("mark_says", ""),
    )


# KARAR #733 alt-paket (Paket 71, 25 May 2026): pivot_breakout endpoint
# (Mark TLSMW Ch 10 + O'Neil CANSLIM canon — P70 helper backend wire)
class PivotBreakoutInfo(BaseModel):
    status: Optional[Literal["CONFIRMED", "WEAK", "NEAR_PIVOT", "BELOW_PIVOT"]] = None
    pivot_price: Optional[float] = None
    current_price: float
    breakout_pct: Optional[float] = None
    volume_multiplier: Optional[float] = None
    volume_confirmed: bool = False
    mark_says: str


@app.get("/api/stock/{symbol}/pivot", response_model=PivotBreakoutInfo)
def get_pivot_breakout(symbol: str) -> PivotBreakoutInfo:
    """Mark TLSMW Ch 10 pivot kırılım hesabı (P70 helper wire).

    OHLCV: _get_ohlcv (yfinance gerçek + MOCK fallback). yfinance erişimi
    varsa gerçek piyasa verisi, network fail durumunda deterministik MOCK
    (UX/test kesintisiz). AÇIK KONU #75 RESOLVED (P144 yfinance pipeline).
    """
    sym = symbol.upper()
    # OHLCV oluştur (mevcut helper'lar)
    stock = _STOCK_BY_SYM.get(sym)
    if stock:
        price = stock.price
    else:
        try:
            wl = [r for r in watchlist_get_all() if r["symbol"] == sym]
        except OperationalError:
            wl = []
        if wl:
            price = float(wl[0]["price"])
        else:
            scan_data = _fetch_scan_symbol_data(sym)
            price = scan_data["price"] if scan_data else 100.0
    bars = _get_ohlcv(sym, price)
    closes = [b.close for b in bars]
    volumes = [b.volume for b in bars]

    result = compute_pivot_breakout(closes, volumes)
    return PivotBreakoutInfo(
        status=result.get("status"),
        pivot_price=result.get("pivot_price"),
        current_price=result.get("current_price", price),
        breakout_pct=result.get("breakout_pct"),
        volume_multiplier=result.get("volume_multiplier"),
        volume_confirmed=bool(result.get("volume_confirmed", False)),
        mark_says=result.get("mark_says", ""),
    )


# KARAR #733 alt-paket (Paket 87, 26 May 2026): climax_run endpoint
# Mark TLSMW Ch 9 "Selling Short and Climax Runs" canon.
# Parabolic / exhaustion / "last gasp" tepe tespiti.
class ClimaxRunInfo(BaseModel):
    category: Optional[Literal["CLIMAX_TOP", "POTENTIAL_CLIMAX", "HEALTHY_ADVANCE", "NONE"]] = None
    gain_pct: Optional[float] = None
    gap_up_days: Optional[int] = None
    avg_volume_ratio: Optional[float] = None
    mark_says: str


@app.get("/api/stock/{symbol}/climax", response_model=ClimaxRunInfo)
def get_climax_run(symbol: str) -> ClimaxRunInfo:
    """Mark TLSMW Ch 9 Climax Run hesabı (P86 helper wire).

    Parabolic / exhaustion / "last gasp" tepe tespiti — gap-up + hacim teyitli.
    OHLCV: _get_ohlcv (yfinance gerçek + MOCK fallback). AÇIK KONU #75 RESOLVED.
    """
    sym = symbol.upper()
    stock = _STOCK_BY_SYM.get(sym)
    if stock:
        price = stock.price
    else:
        try:
            wl = [r for r in watchlist_get_all() if r["symbol"] == sym]
        except OperationalError:
            wl = []
        if wl:
            price = float(wl[0]["price"])
        else:
            scan_data = _fetch_scan_symbol_data(sym)
            price = scan_data["price"] if scan_data else 100.0
    bars = _get_ohlcv(sym, price)
    closes = [b.close for b in bars]
    opens = [b.open for b in bars]
    volumes = [b.volume for b in bars]

    result = compute_climax_run(closes, opens, volumes)
    return ClimaxRunInfo(
        category=result.get("category"),
        gain_pct=result.get("gain_pct"),
        gap_up_days=result.get("gap_up_days"),
        avg_volume_ratio=result.get("avg_volume_ratio"),
        mark_says=result.get("mark_says", ""),
    )


# KARAR #733 alt-paket (Paket 93, 26 May 2026): RS Rating endpoint
# Mark TLSMW Ch 3-5 / IBD canon Relative Strength Rating (1-99).
class RsRatingInfo(BaseModel):
    rs_rating: Optional[int] = None
    category: Optional[Literal["LEADER", "STRONG", "AVERAGE", "LAGGARD"]] = None
    stock_return_pct: Optional[float] = None
    benchmark_return_pct: Optional[float] = None
    outperform_pct: Optional[float] = None
    mark_says: str
    # P441 (10 Haz 2026): RS kaynak şeffaflığı. "scan" = cross-sectional IBD RS
    # (minervini_scans rs_ibd / MOCK, /info ile birebir tutarlı). "computed" =
    # yerel compute_relative_strength_rating yaklaşığı (taranmamış sembol, evren
    # yok → hardcoded bant; Kural #28 UI'da işaretlenir, Kural #26 follow-up).
    source: Optional[Literal["scan", "computed"]] = None


def _resolve_rs_ibd(sym: str) -> Optional[int]:
    """get_stock_info ile AYNI öncelikten cross-sectional IBD RS (rs_ibd) çöz.
    Amaç: /hisse RS kartı (RsRatingCard/MarkProfileBar) header (info.rs_rating)
    ile BİREBİR tutarlı olsun (P441 — RS 97 scan vs 55 compute tutarsızlığı fix).
    Öncelik get_stock_info ile aynı: MOCK_STOCKS > watchlist > scan DB. Bulunamazsa
    None → yerel compute yaklaşığı fallback (source=computed)."""
    stock = _STOCK_BY_SYM.get(sym)
    if stock is not None:
        return int(round(stock.rs_ibd))
    try:
        wl = [r for r in watchlist_get_all() if r["symbol"] == sym]
    except OperationalError:
        wl = []
    if wl and wl[0].get("rs_rating") is not None:
        return int(round(float(wl[0]["rs_rating"])))
    scan_data = _fetch_scan_symbol_data(sym)
    if scan_data and scan_data.get("rs_ibd") is not None:
        return int(scan_data["rs_ibd"])
    return None


@app.get("/api/stock/{symbol}/rs", response_model=RsRatingInfo)
def get_rs_rating(symbol: str) -> RsRatingInfo:
    """Mark TLSMW Ch 3-5 / IBD canon RS Rating (1-99 cross-sectional persentil).

    P441 (10 Haz 2026 — derin araştırma fix): IBD RS = tüm evrene göre persentil
    rank (mutlak/vs-SPY getiri DEĞİL). Eski /rs endpoint compute_relative_strength_
    rating'in hardcoded bandını (vs-SPY outperform → uydurma 1-99) kullanıyordu →
    NVDA scan 97 (cross-sectional, doğru) vs /rs 55 (uydurma bant) tutarsızlığı.
    Fix: DB-FIRST scan rs_ibd (get_stock_info ile aynı kaynak) → header=kartlar.
    Taranmamış sembol → yerel yaklaşık (source=computed, Kural #28 UI işaret).
    """
    sym = symbol.upper()
    if sym == "SPY":
        # SPY kendi-kendine RS = AVERAGE (50) garantili
        return RsRatingInfo(
            rs_rating=50,
            category="AVERAGE",
            stock_return_pct=0.0,
            benchmark_return_pct=0.0,
            outperform_pct=0.0,
            source="scan",
            mark_says="SPY benchmark — RS Rating tanım gereği AVERAGE (50).",
        )

    # P441: DB-FIRST cross-sectional IBD RS (info.rs_rating ile birebir tutarlı).
    rs_ibd = _resolve_rs_ibd(sym)
    if rs_ibd is not None:
        rs_ibd = max(1, min(99, rs_ibd))
        cat = rs_rating_category(rs_ibd)
        cat_say = {
            "LEADER": f'✓ LEADER RS {rs_ibd} — IBD canon "Top 20%" (evrenin ~%{rs_ibd}\'ini geçti). Mark: alım adayı.',
            "STRONG": f'STRONG RS {rs_ibd} — üst orta dilim. Mark: izleme, breakout için bekle.',
            "AVERAGE": f'AVERAGE RS {rs_ibd} — vasat performans. Mark: leadership eksik, başka aday bul.',
            "LAGGARD": f'⚠️ LAGGARD RS {rs_ibd} — alt dilim. Mark TLSMW Ch 4: leader olmadan kazanç zor.',
        }[cat]
        return RsRatingInfo(rs_rating=rs_ibd, category=cat, source="scan", mark_says=cat_say)

    # Fallback: taranmamış sembol — yerel compute yaklaşığı. compute_relative_
    # strength_rating cross-sectional DEĞİL (vs-SPY hardcoded bant) → source=
    # "computed" ile UI'da işaretlenir (Kural #28). Tam cross-sectional persentil
    # (evren batch percentileofscore) follow-up (Kural #26 GELİŞTİRİLMESİ LAZIM).
    stock_bars = _get_ohlcv(sym, 100.0)
    bench_bars = _get_ohlcv("SPY", 450.0)
    result = compute_relative_strength_rating(
        [b.close for b in stock_bars], [b.close for b in bench_bars]
    )
    return RsRatingInfo(
        rs_rating=result.get("rs_rating"),
        category=result.get("category"),
        stock_return_pct=result.get("stock_return_pct"),
        benchmark_return_pct=result.get("benchmark_return_pct"),
        outperform_pct=result.get("outperform_pct"),
        source="computed",
        mark_says="(Taranmamış — yerel yaklaşık, cross-sectional değil) "
        + result.get("mark_says", ""),
    )


# KARAR #733 alt-paket (Paket 101, 26 May 2026): ATR Volatility endpoint
# Mark TLSMW Ch 11 / Wilder canon ATR — stop placement disiplini.
class AtrVolatilityInfo(BaseModel):
    atr: Optional[float] = None
    atr_pct: Optional[float] = None
    category: Optional[Literal["LOW", "NORMAL", "HIGH", "EXTREME"]] = None
    suggested_stop_tight: Optional[float] = None
    suggested_stop_normal: Optional[float] = None
    suggested_stop_loose: Optional[float] = None
    mark_says: str


@app.get("/api/stock/{symbol}/atr", response_model=AtrVolatilityInfo)
def get_atr_volatility(symbol: str) -> AtrVolatilityInfo:
    """Mark TLSMW Ch 11 / Wilder canon ATR (P100 helper wire).

    OHLCV MOCK feed (AÇIK KONU #75 production yfinance).
    Sonuc: ATR + kategori + 3 stop seviyesi.
    """
    sym = symbol.upper()
    stock = _STOCK_BY_SYM.get(sym)
    if stock:
        price = stock.price
    else:
        try:
            wl = [r for r in watchlist_get_all() if r["symbol"] == sym]
        except OperationalError:
            wl = []
        if wl:
            price = float(wl[0]["price"])
        else:
            scan_data = _fetch_scan_symbol_data(sym)
            price = scan_data["price"] if scan_data else 100.0

    bars = _get_ohlcv(sym, price)
    highs = [b.high for b in bars]
    lows = [b.low for b in bars]
    closes = [b.close for b in bars]

    result = compute_atr_volatility(highs, lows, closes)
    return AtrVolatilityInfo(
        atr=result.get("atr"),
        atr_pct=result.get("atr_pct"),
        category=result.get("category"),
        suggested_stop_tight=result.get("suggested_stop_tight"),
        suggested_stop_normal=result.get("suggested_stop_normal"),
        suggested_stop_loose=result.get("suggested_stop_loose"),
        mark_says=result.get("mark_says", ""),
    )


# P111 (26 May 2026): Relative Volume endpoint
# Mark TLSMW Ch 6 / O'Neil canon — pivot hacim teyit + VCP contraction.
class RelativeVolumeInfo(BaseModel):
    rel_vol: Optional[float] = None
    today_volume: Optional[int] = None
    avg_volume: Optional[int] = None
    category: Optional[Literal["DRYING", "QUIET", "NORMAL", "HIGH", "SURGE"]] = None
    mark_says: str


@app.get("/api/stock/{symbol}/relative-volume", response_model=RelativeVolumeInfo)
def get_relative_volume(symbol: str) -> RelativeVolumeInfo:
    """Mark TLSMW Ch 6 / O'Neil canon Relative Volume (P106 helper wire).

    OHLCV MOCK feed (ACIK KONU #75 production yfinance).
    Sonuc: rel_vol + kategori (DRYING/QUIET/NORMAL/HIGH/SURGE).
    """
    sym = symbol.upper()
    stock = _STOCK_BY_SYM.get(sym)
    if stock:
        price = stock.price
    else:
        try:
            wl = [r for r in watchlist_get_all() if r["symbol"] == sym]
        except OperationalError:
            wl = []
        if wl:
            price = float(wl[0]["price"])
        else:
            scan_data = _fetch_scan_symbol_data(sym)
            price = scan_data["price"] if scan_data else 100.0

    bars = _get_ohlcv(sym, price)
    volumes = [b.volume for b in bars]

    result = compute_relative_volume(volumes)
    return RelativeVolumeInfo(
        rel_vol=result.get("rel_vol"),
        today_volume=result.get("today_volume"),
        avg_volume=result.get("avg_volume"),
        category=result.get("category"),
        mark_says=result.get("mark_says", ""),
    )


# P112 (26 May 2026): Breakout Quality endpoint
# Mark TLSMW Ch 10 — pivot kirilim kalite kompoziti (0-100 puan).
class BreakoutQualityInfo(BaseModel):
    score: int = 0
    category: Optional[Literal["EXCELLENT", "GOOD", "MARGINAL", "POOR"]] = None
    breakdown: dict = {}
    mark_says: str
    # P443 (10 Haz 2026 — Kural #28): hangi breakdown bileşenleri GERÇEK hesaplanıyor.
    # P450 (10 Haz) sonrasi 5/5 GERCEK OHLCV'den (volume + gap_up + breakout_strength
    # + prior_contraction + overhead_clean). UI tam-skoru guvenle gosterir.
    # Esik kaynak durumu (11 Haz uc-kaynak triangule — O'Neil CANSLIM + Minervini x2):
    #   volume 1.5x = O'Neil HTMMIS s.33,164 (kitap birebir);
    #   breakout_strength %5-10 chase cap = O'Neil s.26-27 (kitap);
    #   gap_up %3 = Quanfina heuristic (KITAPTA gap-up % YOK — uydurma degil bilincli);
    #   prior_contraction = Minervini VCP TLSMW s.199.
    real_components: list[str] = []


@app.get("/api/stock/{symbol}/breakout-quality", response_model=BreakoutQualityInfo)
def get_breakout_quality(symbol: str) -> BreakoutQualityInfo:
    """Mark TLSMW Ch 10 pivot kirilim kalite kompoziti (P107 helper wire).

    OHLCV MOCK feed — volume_confirmed + gap_up + breakout_pct deterministik.
    ACIK KONU #75: production'da gercek pivot tarihi gerekir.
    """
    sym = symbol.upper()
    stock = _STOCK_BY_SYM.get(sym)
    if stock:
        price = stock.price
    else:
        try:
            wl = [r for r in watchlist_get_all() if r["symbol"] == sym]
        except OperationalError:
            wl = []
        if wl:
            price = float(wl[0]["price"])
        else:
            scan_data = _fetch_scan_symbol_data(sym)
            price = scan_data["price"] if scan_data else 100.0

    bars = _get_ohlcv(sym, price)
    volumes = [b.volume for b in bars]
    closes = [b.close for b in bars]
    opens = [b.open for b in bars]

    # rel_vol hesapla — volume_confirmed icin (>=1.5x = teyit)
    rv = compute_relative_volume(volumes)
    volume_confirmed = (rv.get("rel_vol") or 0.0) >= 1.5

    # P450 (DD-sonrasi #2 derin arastirma): 4/5 hardcoded bilesen GERCEK OHLCV'den
    # hesaplandi — mevcut CANLI helper'lar (yeni helper YOK). P443 "kismi MOCK"
    # uyarisi kapaniyor. Kaynaklar:
    #   - gap_up: inline (open[-1]-close[-2])/close[-2]*100 >= 3.0 — compute_climax_run
    #     CLIMAX_GAP_UP_PCT=3.0 ile birebir tutarli. NOT (11 Haz 2026 uc-kaynak danisma
    #     COZULDU): O'Neil HTMMIS s.45 + Minervini TLSMW s.219 — buyable gap-up icin
    #     KITAPTA % ESIGI YOK (sadece niteliksel + devasa hacim). %3 = Quanfina bilincli
    #     operasyonel heuristic (Kural #28), uydurma DEGIL. GELISTIRILMESI LAZIM kalkti.
    #   - breakout_pct: compute_pivot_breakout (20-bar pivot, AÇIK KONU #75 calisan cozum)
    #   - overhead_clean: compute_overhead_supply category == NONE
    #   - prior_contraction: compute_vcp_pass (Brandon V-Dry VCP, KARAR #464 gercek hesap)
    gap_up = (
        len(opens) >= 2
        and closes[-2] > 0
        and (opens[-1] - closes[-2]) / closes[-2] * 100.0 >= 3.0
    )
    pb = compute_pivot_breakout(closes, volumes)
    breakout_pct = pb.get("breakout_pct")
    if breakout_pct is None:
        breakout_pct = 0.0  # pivot yok → breakout gucu 0 (kirilim henuz degil)
    oh = compute_overhead_supply(closes)
    overhead_clean = oh.get("category") == "NONE"
    pvh = [
        {"open": b.open, "high": b.high, "low": b.low, "close": b.close, "volume": b.volume}
        for b in bars
    ]
    prior_contraction = compute_vcp_pass(pvh)

    result = compute_breakout_quality(
        volume_confirmed=volume_confirmed,
        gap_up=gap_up,
        breakout_pct=breakout_pct,
        prior_contraction=prior_contraction,
        overhead_clean=overhead_clean,
    )
    # P450: 5/5 bilesen artik gercek OHLCV'den (volume zaten gercekti) — "kismi"
    # uyarisi kalkar. (breakout_strength puanlama ust-kapak '>5% chase' fix ayri
    # paket — Sn. Ferit karari, quanfina_math.py:4584 scoring degisimi.)
    return BreakoutQualityInfo(
        score=result.get("score", 0),
        category=result.get("category"),
        breakdown=result.get("breakdown", {}),
        real_components=[
            "volume", "gap_up", "breakout_strength", "prior_contraction", "overhead_clean",
        ],
        mark_says=result.get("mark_says", ""),
    )


# Paket 456 (11 Haz 2026): Cup-with-Handle endpoint
# O'Neil CAN SLIM "How to Make Money in Stocks" Bol.15 — compute_cup_with_handle wire.
# Uc-kaynak danisma (NotebookLM O'Neil + Minervini x2 + IBD corroborate) ile esikler
# kitap-birebir (Kural #26). VCP'nin yaninda 2. base patern (/hisse + Pattern Library).
class CupHandleInfo(BaseModel):
    detected: bool = False
    quality: Optional[Literal["EXCELLENT", "GOOD", "MARGINAL", "NONE"]] = None
    pivot_price: Optional[float] = None
    prior_uptrend_pct: Optional[float] = None
    cup_depth_pct: Optional[float] = None
    cup_duration_days: Optional[int] = None
    handle_depth_pct: Optional[float] = None
    handle_in_upper_half: bool = False
    shakeout: bool = False
    faults: list[str] = []
    mark_says: str


@app.get("/api/stock/{symbol}/cup-handle", response_model=CupHandleInfo)
def get_cup_handle(symbol: str) -> CupHandleInfo:
    """O'Neil CAN SLIM cup-with-handle base tespiti (HTMMIS Bol.15, P456 wire).

    compute_cup_with_handle — GERCEK OHLCV (highs/lows/closes/volumes). Tum esikler
    kitap-birebir (prior +%30 / kupa %12-33 / kulp <=%15 ust yari + 200MA / shakeout /
    U-sekil / upward-wedging fault). AÇIK KONU #75: production gercek base tarihi ideal.
    """
    sym = symbol.upper()
    stock = _STOCK_BY_SYM.get(sym)
    if stock:
        price = stock.price
    else:
        try:
            wl = [r for r in watchlist_get_all() if r["symbol"] == sym]
        except OperationalError:
            wl = []
        if wl:
            price = float(wl[0]["price"])
        else:
            scan_data = _fetch_scan_symbol_data(sym)
            price = scan_data["price"] if scan_data else 100.0

    bars = _get_ohlcv(sym, price)
    highs = [b.high for b in bars]
    lows = [b.low for b in bars]
    closes = [b.close for b in bars]
    volumes = [b.volume for b in bars]

    r = compute_cup_with_handle(highs, lows, closes, volumes)
    return CupHandleInfo(
        detected=r.get("detected", False),
        quality=r.get("quality"),
        pivot_price=r.get("pivot_price"),
        prior_uptrend_pct=r.get("prior_uptrend_pct"),
        cup_depth_pct=r.get("cup_depth_pct"),
        cup_duration_days=r.get("cup_duration_days"),
        handle_depth_pct=r.get("handle_depth_pct"),
        handle_in_upper_half=r.get("handle_in_upper_half", False),
        shakeout=r.get("shakeout", False),
        faults=r.get("faults", []),
        mark_says=r.get("mark_says", ""),
    )


# Paket 458 (11 Haz 2026): Flat Base endpoint
# O'Neil/IBD Flat Base — compute_flat_base wire (base detector ailesi 2. uye).
# Derin internet arastirma (IBD/MarketSmith/TraderLion) ile esikler kaynak-atifli (Kural #26).
class FlatBaseInfo(BaseModel):
    detected: bool = False
    quality: Optional[Literal["EXCELLENT", "GOOD", "MARGINAL", "NONE"]] = None
    pivot_price: Optional[float] = None
    prior_advance_pct: Optional[float] = None
    base_depth_pct: Optional[float] = None
    base_duration_days: Optional[int] = None
    is_sideways: bool = False
    volume_dryup: Optional[bool] = None
    faults: list[str] = []
    mark_says: str


@app.get("/api/stock/{symbol}/flat-base", response_model=FlatBaseInfo)
def get_flat_base(symbol: str) -> FlatBaseInfo:
    """O'Neil/IBD flat base tespiti (later-stage yatay konsolidasyon, P458 wire).

    compute_flat_base — GERCEK OHLCV (highs/lows/closes/volumes). Esikler IBD-canon
    (min 5 hafta / derinlik <=%15 / onceki advance >=%20 / yatay / pivot = baz zirvesi).
    """
    sym = symbol.upper()
    stock = _STOCK_BY_SYM.get(sym)
    if stock:
        price = stock.price
    else:
        try:
            wl = [r for r in watchlist_get_all() if r["symbol"] == sym]
        except OperationalError:
            wl = []
        if wl:
            price = float(wl[0]["price"])
        else:
            scan_data = _fetch_scan_symbol_data(sym)
            price = scan_data["price"] if scan_data else 100.0

    bars = _get_ohlcv(sym, price)
    highs = [b.high for b in bars]
    lows = [b.low for b in bars]
    closes = [b.close for b in bars]
    volumes = [b.volume for b in bars]

    r = compute_flat_base(highs, lows, closes, volumes)
    return FlatBaseInfo(
        detected=r.get("detected", False),
        quality=r.get("quality"),
        pivot_price=r.get("pivot_price"),
        prior_advance_pct=r.get("prior_advance_pct"),
        base_depth_pct=r.get("base_depth_pct"),
        base_duration_days=r.get("base_duration_days"),
        is_sideways=r.get("is_sideways", False),
        volume_dryup=r.get("volume_dryup"),
        faults=r.get("faults", []),
        mark_says=r.get("mark_says", ""),
    )


# Paket 460 (11 Haz 2026): Double Bottom (W) endpoint
# O'Neil/IBD Double Bottom — compute_double_bottom wire (base detector ailesi 3. uye).
# Derin internet arastirma (MarketSmith/IBD/TraderLion) ile esikler kaynak-atifli (Kural #26).
class DoubleBottomInfo(BaseModel):
    detected: bool = False
    quality: Optional[Literal["EXCELLENT", "GOOD", "MARGINAL", "NONE"]] = None
    pivot_price: Optional[float] = None
    prior_advance_pct: Optional[float] = None
    base_depth_pct: Optional[float] = None
    base_duration_days: Optional[int] = None
    undercut: bool = False
    first_low: Optional[float] = None
    second_low: Optional[float] = None
    middle_peak: Optional[float] = None
    faults: list[str] = []
    mark_says: str


@app.get("/api/stock/{symbol}/double-bottom", response_model=DoubleBottomInfo)
def get_double_bottom(symbol: str) -> DoubleBottomInfo:
    """O'Neil/IBD double bottom (W) tespiti (2. dip undercut + orta tepe pivot, P460 wire).

    compute_double_bottom — GERCEK OHLCV. Esikler kaynak-atifli (min 7 hafta / undercut
    zorunlu / pivot = orta tepe / onceki advance >=%30 / derinlik <=%35, red >%50).
    """
    sym = symbol.upper()
    stock = _STOCK_BY_SYM.get(sym)
    if stock:
        price = stock.price
    else:
        try:
            wl = [r for r in watchlist_get_all() if r["symbol"] == sym]
        except OperationalError:
            wl = []
        if wl:
            price = float(wl[0]["price"])
        else:
            scan_data = _fetch_scan_symbol_data(sym)
            price = scan_data["price"] if scan_data else 100.0

    bars = _get_ohlcv(sym, price)
    highs = [b.high for b in bars]
    lows = [b.low for b in bars]
    closes = [b.close for b in bars]
    volumes = [b.volume for b in bars]

    r = compute_double_bottom(highs, lows, closes, volumes)
    return DoubleBottomInfo(
        detected=r.get("detected", False),
        quality=r.get("quality"),
        pivot_price=r.get("pivot_price"),
        prior_advance_pct=r.get("prior_advance_pct"),
        base_depth_pct=r.get("base_depth_pct"),
        base_duration_days=r.get("base_duration_days"),
        undercut=r.get("undercut", False),
        first_low=r.get("first_low"),
        second_low=r.get("second_low"),
        middle_peak=r.get("middle_peak"),
        faults=r.get("faults", []),
        mark_says=r.get("mark_says", ""),
    )


# Paket 462 (11 Haz 2026): High Tight Flag endpoint (= Minervini Power Play)
# O'Neil HTF — compute_high_tight_flag wire (base detector ailesi 4. uye, EN NADIR/RISKLI).
# Derin internet arastirma (IBD/MarketSmith/Minervini s.255) ile esikler kaynak-atifli (Kural #26).
class HighTightFlagInfo(BaseModel):
    detected: bool = False
    quality: Optional[Literal["EXCELLENT", "GOOD", "MARGINAL", "NONE"]] = None
    pivot_price: Optional[float] = None
    flagpole_pct: Optional[float] = None
    flagpole_weeks: Optional[float] = None
    flag_depth_pct: Optional[float] = None
    flag_duration_days: Optional[int] = None
    volume_dryup: Optional[bool] = None
    faults: list[str] = []
    mark_says: str


@app.get("/api/stock/{symbol}/high-tight-flag", response_model=HighTightFlagInfo)
def get_high_tight_flag(symbol: str) -> HighTightFlagInfo:
    """O'Neil High Tight Flag (= Minervini Power Play) tespiti (flagpole >=%100 + tight flag, P462).

    compute_high_tight_flag — GERCEK OHLCV. Esikler kaynak-atifli (flagpole >=%100 <=8hf /
    flag <=%25 3-6hf / pivot = flag zirvesi). En guclu + EN NADIR + EN RISKLI patern.
    """
    sym = symbol.upper()
    stock = _STOCK_BY_SYM.get(sym)
    if stock:
        price = stock.price
    else:
        try:
            wl = [r for r in watchlist_get_all() if r["symbol"] == sym]
        except OperationalError:
            wl = []
        if wl:
            price = float(wl[0]["price"])
        else:
            scan_data = _fetch_scan_symbol_data(sym)
            price = scan_data["price"] if scan_data else 100.0

    bars = _get_ohlcv(sym, price)
    highs = [b.high for b in bars]
    lows = [b.low for b in bars]
    closes = [b.close for b in bars]
    volumes = [b.volume for b in bars]

    r = compute_high_tight_flag(highs, lows, closes, volumes)
    return HighTightFlagInfo(
        detected=r.get("detected", False),
        quality=r.get("quality"),
        pivot_price=r.get("pivot_price"),
        flagpole_pct=r.get("flagpole_pct"),
        flagpole_weeks=r.get("flagpole_weeks"),
        flag_depth_pct=r.get("flag_depth_pct"),
        flag_duration_days=r.get("flag_duration_days"),
        volume_dryup=r.get("volume_dryup"),
        faults=r.get("faults", []),
        mark_says=r.get("mark_says", ""),
    )


# KARAR #733 alt-paket (Paket 121, 26 May 2026): Stage Transition endpoint
# Mark TLSMW Ch 4 + Weinstein Stage 1->2 gecis tespiti backend wire.
class StageTransitionInfo(BaseModel):
    category: Optional[Literal[
        "NO_TRANSITION", "EARLY_STAGE_2", "CONFIRMED_STAGE_2", "STAGE_2_MATURE"
    ]] = None
    ma_value: Optional[float] = None
    price_above_ma_pct: Optional[float] = None
    slope_pct: Optional[float] = None
    days_above_ma: Optional[int] = None
    volume_trend: Optional[Literal["RISING", "STABLE", "FALLING"]] = None
    mark_says: str


@app.get("/api/stock/{symbol}/stage", response_model=StageTransitionInfo)
def get_stage_transition(symbol: str) -> StageTransitionInfo:
    """Mark TLSMW Ch 4 Stage 1->2 transition (P120 helper wire).

    30W MA (150 day SMA) bazli kirilim erken uyari + hacim teyit.
    OHLCV: _get_ohlcv (yfinance gercek + MOCK fallback). AÇIK KONU #75 RESOLVED.
    """
    sym = symbol.upper()
    stock = _STOCK_BY_SYM.get(sym)
    if stock:
        price = stock.price
    else:
        try:
            wl = [r for r in watchlist_get_all() if r["symbol"] == sym]
        except OperationalError:
            wl = []
        if wl:
            price = float(wl[0]["price"])
        else:
            scan_data = _fetch_scan_symbol_data(sym)
            price = scan_data["price"] if scan_data else 100.0

    bars = _get_ohlcv(sym, price)
    closes = [b.close for b in bars]
    volumes = [b.volume for b in bars]

    result = compute_stage_transition(closes, volumes)
    return StageTransitionInfo(
        category=result.get("category"),
        ma_value=result.get("ma_value"),
        price_above_ma_pct=result.get("price_above_ma_pct"),
        slope_pct=result.get("slope_pct"),
        days_above_ma=result.get("days_above_ma"),
        volume_trend=result.get("volume_trend"),
        mark_says=result.get("mark_says", ""),
    )


@app.get("/api/stock/{symbol}/ohlcv", response_model=list[OhlcvBar])
def get_stock_ohlcv(symbol: str, bars: int = 252) -> list[OhlcvBar]:
    # P435: bars param (default 252 geriye uyum). Grafik MA200 warmup için 460
    # ister (200 warmup + ~252 görünür) → MA200 tüm görünür aralıkta dolu.
    bars = max(60, min(bars, 504))  # 60..504 (2y) sınır
    sym = symbol.upper()
    stock = _STOCK_BY_SYM.get(sym)
    if stock:
        price = stock.price
    else:
        try:
            wl = [r for r in watchlist_get_all() if r["symbol"] == sym]
        except OperationalError:
            wl = []
        if wl:
            price = float(wl[0]["price"])
        else:
            # 24 May 2026 — Tarama sonucu fallback (KARAR ADAY #498)
            scan_data = _fetch_scan_symbol_data(sym)
            price = scan_data["price"] if scan_data else 100.0  # Generic MOCK son fallback
    return _get_ohlcv(sym, price, bars)  # P435: bars (MA200 warmup)


# =============================================================================
# Paket 150 (26 May 2026): Stock Quote endpoint — current price for paper P&L
# Sn. Ferit paper trading: açık trade'lerde canlı kar/zarar gösterimi
# =============================================================================

class StockQuote(BaseModel):
    symbol: str
    price: float           # Son kapanış (yfinance) veya MOCK fallback
    change_dollar: Optional[float] = None  # Bugünkü dolar değişim
    change_pct: Optional[float] = None     # Bugünkü yüzde değişim
    source: Literal["yfinance", "mock"]


@app.get("/api/stock/{symbol}/quote", response_model=StockQuote)
def get_stock_quote(symbol: str) -> StockQuote:
    """Sembolün güncel fiyat + değişim bilgisi (yfinance priority + MOCK fallback).

    Paper trading açık trade canlı P&L hesabı için. Cache: _OHLCV_CACHE 5dk TTL
    paylaşımı (Watchlist/Trade/Signal enrichment ile aynı veri).
    """
    sym = symbol.upper()
    bars = _fetch_ohlcv_real(sym, 2)  # Son 2 gün — bugün + dün (değişim hesabı)
    if bars and len(bars) >= 2:
        last = bars[-1]
        prev = bars[-2]
        change_dollar = round(last.close - prev.close, 4)
        change_pct = round((change_dollar / prev.close) * 100, 2) if prev.close else None
        return StockQuote(
            symbol=sym,
            price=round(last.close, 4),
            change_dollar=change_dollar,
            change_pct=change_pct,
            source="yfinance",
        )
    if bars and len(bars) >= 1:
        return StockQuote(
            symbol=sym,
            price=round(bars[-1].close, 4),
            change_dollar=None,
            change_pct=None,
            source="yfinance",
        )
    # MOCK fallback
    stock = _STOCK_BY_SYM.get(sym)
    if stock:
        return StockQuote(
            symbol=sym,
            price=stock.price,
            change_dollar=None,
            change_pct=stock.change_pct,
            source="mock",
        )
    return StockQuote(symbol=sym, price=100.0, source="mock")


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


# ── Pattern Library (Migration 010 — KARAR ADAY #714) ─────────────────────────

class PatternLibraryEntry(BaseModel):
    """Migration 010 pattern_library satiri — Mark/O'Neil canon pattern parametre.
    KALICI ILKE #4: her pattern kitap referansli (mark_book_ref), hardcoded sayi yok."""
    id: int
    pattern_name: str
    mark_book_ref: Optional[str] = None
    contraction_count_min: Optional[int] = None
    contraction_count_max: Optional[int] = None
    base_weeks_min: Optional[int] = None
    base_weeks_max: Optional[int] = None
    notes: Optional[str] = None


@app.get("/api/patterns", response_model=list[PatternLibraryEntry])
def get_patterns() -> list[PatternLibraryEntry]:
    """Pattern Library — 7 Mark/O'Neil canon pattern (TLSMW Ch 10).
    DB bos/erisilmezse bos liste (graceful — UI pattern listesi gizler)."""
    return pattern_library_get_all()


# ── Sektör Rotasyonu (11 SPDR ETF RS rank) ───────────────────────────────────

class SectorRotationEntry(BaseModel):
    """sector_rotation satırı — Mark/Minervini lider sektör disiplini.
    perf_* yüzde getiri, rs_score relative strength, rs_rank 1=en güçlü."""
    ticker: str
    sector_name: str
    perf_1w: Optional[float] = None
    perf_1m: Optional[float] = None
    perf_3m: Optional[float] = None
    perf_6m: Optional[float] = None
    perf_1y: Optional[float] = None
    rs_score: Optional[float] = None
    rs_rank: Optional[int] = None
    scan_date: Optional[str] = None


@app.get("/api/sector-rotation", response_model=list[SectorRotationEntry])
def get_sector_rotation() -> list[SectorRotationEntry]:
    """Sektör Rotasyonu — son tarama 11 SPDR sektör ETF, RS rank sıralı.
    DB bos/erisilmezse bos liste (graceful)."""
    return sector_rotation_get_latest()


# ── Trade Journal ─────────────────────────────────────────────────────────────

from decimal import Decimal as _D, ROUND_HALF_UP as _RHU


def _calc_pl(entry_price: float, exit_price: float, shares: int) -> tuple[float, float]:
    """Trade P&L hesabı — Decimal precision (float arithmetic tuzağı yok).

    Defensive (P385): entry_price=0 -> (0.0, 0.0) graceful (ZeroDivisionError
    yerine). Caller path: add_trade endpoint Pydantic gt=0 ile entry=0 zaten
    bloklamış olur (422), ama legacy DB satırlarinda (gecmiste validation
    olmadan girilmis) entry=0 olabilir -> defense in depth.
    """
    if entry_price == 0:
        return 0.0, 0.0
    entry = _D(str(entry_price))
    exit_ = _D(str(exit_price))
    qty   = _D(str(shares))
    pl_dollar = float(((exit_ - entry) * qty).quantize(_D("0.01"), rounding=_RHU))
    pl_pct    = float(((exit_ - entry) / entry * 100).quantize(_D("0.01"), rounding=_RHU))
    return pl_dollar, pl_pct


TimeHorizon = Literal["swing", "position", "core"]


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
    # KARAR ADAY #717 (24 May 2026) — Mark TTLC Sec 1 birebir 6 zorunlu plan alanı.
    # Eski trade'ler için NULL (geriye uyum), yeni kayıtlarda zorunlu (TradeCreate).
    # Mark birebir: "Without a written plan, you have only hope"
    plan_entry_trigger: Optional[str] = None
    plan_stop: Optional[float] = None
    plan_target: Optional[float] = None
    plan_size_pct: Optional[float] = None
    plan_exit_strategy: Optional[str] = None
    plan_time_horizon: Optional[TimeHorizon] = None
    # KARAR #733 alt-paket (Paket 41, 24 May 2026): Trade satirina Mark Profili
    # rozetlerini ekler (Watchlist + Signals + Screens + Hisse pateni).
    # Journal sayfasinda stage4Count gerçek hesaplama icin. Production'da
    # minervini_scans tablo join'i ile gelir; simdilik _STOCK_MARK_SIGNALS MOCK.
    mark_signals: Optional[dict] = None
    # KARAR #733 alt-paket (Paket 84, 26 May 2026): Pivot status
    # P81-P83 paten — Journal'da trade'in mevcut pivot durumu (kapanmamış için)
    pivot_status: Optional[Literal["CONFIRMED", "WEAK", "NEAR_PIVOT", "BELOW_PIVOT"]] = None
    # Paket 190 (26 May 2026): Sektör konsantrasyon uyarısı (Mark TTLC s.85).
    # _STOCK_META lookup ile doldurulur (Quanfina evren). Bilinmeyen sembolde None.
    sector: Optional[str] = None


class TradeCreate(BaseModel):
    symbol: str
    strategy: str
    setup_type: str
    # KARAR #477: ZORUNLU (UX Bölüm 7) — trade kayıt formunda Sinyal Kaynağı default yok.
    # DB'de eski trades NULL kalabilir (geriye uyum), yeni kayıtlar zorunlu doldurur.
    signal_source: Literal["strategy", "manual_self", "manual_external"]
    entry_date: str
    # P385: gt=0 validation (entry=0 -> _calc_pl ZeroDivisionError 500 onlenmesi).
    # Paper trading guvenligi: API dogrudan hit edilirse veya UI bug'sa veri girer.
    entry_price: float = Field(gt=0, description="Giris fiyati > 0 zorunlu (P&L hesabi icin)")
    shares: int = Field(gt=0, description="Hisse adedi > 0 zorunlu")
    status: Literal["open", "closed"] = "open"
    exit_date: Optional[str] = None
    # exit_price ge=0 (delisting/total loss senaryosu icin 0 OK, ama negatif YASAK)
    exit_price: Optional[float] = Field(default=None, ge=0)
    grade: Optional[str] = None
    exit_reason: Optional[str] = None
    lessons: Optional[str] = None
    # KARAR ADAY #717 — Mark TTLC Sec 1 disiplini: yeni trade plan'sız girilemez.
    # 6 alan ZORUNLU (default yok), Mark felsefesi: "Always go in with a plan".
    # P385: plan alanlari da gt=0 (long pozisyon icin negatif/sifir fiyat anlamsiz).
    plan_entry_trigger: str
    plan_stop: float = Field(gt=0, description="Stop fiyati > 0 zorunlu (Mark Risk first)")
    plan_target: float = Field(gt=0, description="Hedef fiyati > 0 zorunlu")
    # plan_size_pct: Quanfina iç kontrat — portföy yüzdesi (0 < x ≤ 100).
    plan_size_pct: float = Field(gt=0, le=100, description="Portfoy yuzdesi (0, 100] araliginda")
    plan_exit_strategy: str
    plan_time_horizon: TimeHorizon


class TradeUpdate(BaseModel):
    # P386: TradeUpdate validation gap (P385 paralel — TradeCreate gt=0 disiplini).
    # PATCH endpoint negatif fiyat / yuzde >100 kabul etmemeli.
    exit_date: Optional[str] = None
    # exit_price ge=0 (delisting/total loss 0 OK, negatif YASAK)
    exit_price: Optional[float] = Field(default=None, ge=0)
    status: Optional[Literal["open", "closed"]] = None
    grade: Optional[str] = None
    exit_reason: Optional[str] = None
    lessons: Optional[str] = None
    setup_type: Optional[str] = None
    # KARAR ADAY #717 — Plan alanlari yazıldıktan sonra düzeltilebilir (Optional).
    # P386: Field validation Optional + gt=0 (None = degisiklik yok; gönderilirse > 0).
    plan_entry_trigger: Optional[str] = None
    plan_stop: Optional[float] = Field(default=None, gt=0, description="Stop fiyati > 0")
    plan_target: Optional[float] = Field(default=None, gt=0, description="Hedef fiyati > 0")
    plan_size_pct: Optional[float] = Field(default=None, gt=0, le=100, description="Portfoy yuzdesi (0, 100]")
    plan_exit_strategy: Optional[str] = None
    plan_time_horizon: Optional[TimeHorizon] = None


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


def _enrich_trade_with_mark_signals(trade: Trade) -> Trade:
    """KARAR #733 alt-paket (Paket 41, 24 May 2026) — Trade satirina Mark
    Profili rozetlerini ekler. Watchlist + Signals enrich pateni birebir.

    KARAR #733 alt-paket (Paket 84, 26 May 2026): pivot_status enrichment
    (P81-P83 paten). Açık trade'lerde anlamlı (kapanmış için referans).

    Paket 146 (26 May 2026): yfinance live overlay (RS Rating / Stage / Climax).
    Açık trade'lerde gerçek piyasa durumu — Mark canon paper trading temeli.
    """
    updates: dict = {}
    # P146: yfinance + MOCK birleşik (Watchlist pateni)
    live_signals = _compute_live_mark_signals(trade.symbol, trade.entry_price)
    if live_signals:
        updates["mark_signals"] = live_signals
    pivot_status = _compute_signal_pivot_status(trade.symbol, trade.entry_price)
    if pivot_status:
        updates["pivot_status"] = pivot_status
    # Paket 190: sektör konsantrasyon uyarısı için sector enrichment (_STOCK_META)
    # Fallback: sector field yoksa industry kullan (eski 28 sembol)
    meta = _STOCK_META.get(trade.symbol.upper())
    if meta:
        sec = meta.get("sector") or meta.get("industry")
        if sec:
            updates["sector"] = sec
    if updates:
        return trade.model_copy(update=updates)
    return trade


def _enrich_trade_batch(trades: list[Trade]) -> list[Trade]:
    """Paket 146 (26 May 2026): Trade'leri paralel zenginleştir.

    Watchlist _enrich_watchlist_batch pateni — ThreadPoolExecutor + SPY pre-warm.
    Cache hit'lerle 0s, soğuk başlangıçta n trade × ~0.2s.
    """
    if not trades:
        return trades
    try:
        _fetch_ohlcv_real("SPY", 252)
    except Exception:
        pass
    from concurrent.futures import ThreadPoolExecutor

    def _enrich_one(t: Trade) -> Trade:
        try:
            return _enrich_trade_with_mark_signals(t)
        except Exception:
            return t

    with ThreadPoolExecutor(max_workers=10) as pool:
        enriched = list(pool.map(_enrich_one, trades))
    return enriched


@app.get("/api/trades", response_model=list[Trade])
def get_trades() -> list[Trade]:
    # DB unreachable -> MOCK fallback (Sinyaller + Watchlist pateni — Kural #20 UX)
    # 8 trade: 4 açık + 4 kapalı, çeşitli grade/strateji/setup
    if not db_health_check():
        rows = [
            Trade(id=1, symbol="NVDA",  strategy="minervini", setup_type="VCP",                signal_source="strategy",        entry_date="2026-04-22 09:35", entry_price=132.50,  shares=100, status="closed", exit_date="2026-05-15 15:42", exit_price=145.80, pl_dollar=1330.00,  pl_pct=10.04, grade="A",  exit_reason="target",         lessons="VCP pivot kırılımı 3+ daralma sonrası temiz"),
            Trade(id=2, symbol="MSFT",  strategy="minervini", setup_type="Power Play",         signal_source="strategy",        entry_date="2026-05-02 10:12", entry_price=412.00,  shares=50,  status="open",   exit_date=None,                exit_price=None,    pl_dollar=None,     pl_pct=None,  grade=None, exit_reason=None,              lessons=None),
            Trade(id=3, symbol="AAPL",  strategy="carr",      setup_type="Pullback",           signal_source="manual_self",     entry_date="2026-04-10 14:28", entry_price=198.50,  shares=80,  status="closed", exit_date="2026-04-25 11:05", exit_price=189.20, pl_dollar=-744.00,  pl_pct=-4.69, grade="C",  exit_reason="stop_loss",      lessons="Pullback dip teyit eksikti, erken giriş"),
            Trade(id=4, symbol="GOOGL", strategy="carr",      setup_type="Bullish Divergence", signal_source="manual_external", entry_date="2026-05-10 13:15", entry_price=172.80,  shares=60,  status="open",   exit_date=None,                exit_price=None,    pl_dollar=None,     pl_pct=None,  grade=None, exit_reason=None,              lessons=None),
            Trade(id=5, symbol="AMD",   strategy="minervini", setup_type="Inside Day",         signal_source="strategy",        entry_date="2026-03-28 09:58", entry_price=145.40,  shares=70,  status="closed", exit_date="2026-04-18 14:33", exit_price=162.10, pl_dollar=1169.00,  pl_pct=11.49, grade="A",  exit_reason="target",         lessons="Inside Day patlama hacim teyitli"),
            Trade(id=6, symbol="TSLA",  strategy="minervini", setup_type="Tight Low Vol",      signal_source="strategy",        entry_date="2026-05-12 10:38", entry_price=238.20,  shares=40,  status="open",   exit_date=None,                exit_price=None,    pl_dollar=None,     pl_pct=None,  grade=None, exit_reason=None,              lessons=None),
            Trade(id=7, symbol="META",  strategy="carr",      setup_type="Pullback",           signal_source="manual_self",     entry_date="2026-04-05 11:20", entry_price=485.30,  shares=30,  status="closed", exit_date="2026-04-30 09:47", exit_price=510.20, pl_dollar=747.00,   pl_pct=5.13,  grade="B",  exit_reason="trailing_stop",  lessons="Trailing stop biraz sıkı kalmış, sabırlı kalsaydım daha iyi"),
            Trade(id=8, symbol="AVGO",  strategy="minervini", setup_type="VCP",                signal_source="strategy",        entry_date="2026-05-08 09:42", entry_price=1392.00, shares=10,  status="open",   exit_date=None,                exit_price=None,    pl_dollar=None,     pl_pct=None,  grade=None, exit_reason=None,              lessons=None),
        ]
        # KARAR #733 alt-paket (Paket 41 + Paket 146): Mark enrichment paralel batch
        return _enrich_trade_batch(rows)

    try:
        rows = [Trade(**t) for t in trades_get_all()]
        return _enrich_trade_batch(rows)
    except OperationalError as e:
        raise HTTPException(
            status_code=503,
            detail="Veritabanına ulaşılamıyor (Cloud SQL). GCP Console → SQL → instance durum/Authorized Networks kontrol et."
        ) from e


# KARAR #733 alt-paket (Paket 89, 26 May 2026): Brandon expectancy endpoint
# Mark Brandon Video canon: %20 avg gain / %4 avg loss / %50 win rate hedef
# E = (P_win × avg_gain) - (P_loss × avg_loss) trader expectancy.
class BrandonExpectancyInfo(BaseModel):
    expectancy_pct: float
    category: Optional[Literal["EXCELLENT", "GOOD", "ACCEPTABLE", "POOR"]] = None
    meets_brandon_targets: bool
    mark_says: str
    # Bağlam metrikleri (UI bilgi amaçlı)
    total_closed: int
    winners: int
    losers: int
    avg_gain_pct: float
    avg_loss_pct: float
    win_rate: float


# P417 (31 May 2026 — Kural #28 audit /journal seffaflik):
# /journal sayfa MOCK_TRADES fallback kullanildigini Sn. Ferit'in gormesini sagla.
# Trade response wrapper degisikligi (geriye uyum bozar) yerine ayri meta endpoint:
# UI banner conditional render -> P408 patenli sari rozet.
class TradesInfo(BaseModel):
    source: Literal["db", "mock_fallback"]
    count: int
    is_mock: bool


@app.get("/api/trades/info", response_model=TradesInfo)
def get_trades_info() -> TradesInfo:
    """Trade veri kaynagi meta — DB sağlam mi yoksa MOCK_TRADES fallback mi?

    Kural #28 (Canlı Veri Önce) audit: /journal sayfa basinda UI banner
    conditional render (is_mock=true ise sari "MOCK fallback" uyarisi).
    """
    if not db_health_check():
        return TradesInfo(source="mock_fallback", count=8, is_mock=True)
    try:
        rows = trades_get_all()
        return TradesInfo(source="db", count=len(rows), is_mock=False)
    except Exception:
        # DB query fail -> graceful, MOCK fallback aktif olacak
        return TradesInfo(source="mock_fallback", count=8, is_mock=True)


@app.get("/api/trades/expectancy", response_model=BrandonExpectancyInfo)
def get_trades_expectancy(strategy: Optional[str] = None) -> BrandonExpectancyInfo:
    """Mark Brandon Video expectancy hesabı (P86 helper + P89 wire).

    Query param strategy (opsiyonel, P96):
    - "minervini" -> sadece Minervini trade'leri
    - "carr" -> sadece Carr trade'leri
    - None / "all" -> tüm strateji ortalama

    Tüm kapalı trade'lerin pl_pct'i üzerinden:
    - Avg gain (winners) — pozitif pl_pct ortalaması
    - Avg loss (losers) — negatif pl_pct |mutlak| ortalaması
    - Win rate — winners / total closed
    -> compute_brandon_expectancy(avg_gain, avg_loss, win_rate)

    Hiç kapalı trade yoksa POOR + 0 expectancy (RBA disiplin sıfırı).
    """
    # Trade listesini al (DB veya MOCK fallback)
    trades = get_trades()  # endpoint fonksiyonu reuse
    # KARAR #733 alt-paket (Paket 96): strategy filter
    if strategy and strategy not in ("all", "ALL"):
        strat_lower = strategy.lower()
        trades = [t for t in trades if (t.strategy or "").lower() == strat_lower]
    closed = [t for t in trades if t.status == "closed" and t.pl_pct is not None]

    if not closed:
        return BrandonExpectancyInfo(
            expectancy_pct=0.0,
            category=None,
            meets_brandon_targets=False,
            mark_says="Henüz kapalı trade yok. RBA için en az 1 closed trade gerek.",
            total_closed=0,
            winners=0,
            losers=0,
            avg_gain_pct=0.0,
            avg_loss_pct=0.0,
            win_rate=0.0,
        )

    winners = [t for t in closed if (t.pl_pct or 0) > 0]
    losers = [t for t in closed if (t.pl_pct or 0) <= 0]
    avg_gain = (
        sum(t.pl_pct for t in winners if t.pl_pct is not None) / len(winners)
        if winners else 0.0
    )
    avg_loss = (
        abs(sum(t.pl_pct for t in losers if t.pl_pct is not None) / len(losers))
        if losers else 0.0
    )
    win_rate = len(winners) / len(closed)

    result = compute_brandon_expectancy(avg_gain, avg_loss, win_rate)

    return BrandonExpectancyInfo(
        expectancy_pct=result.get("expectancy_pct", 0.0),
        category=result.get("category"),
        meets_brandon_targets=bool(result.get("meets_brandon_targets", False)),
        mark_says=result.get("mark_says", ""),
        total_closed=len(closed),
        winners=len(winners),
        losers=len(losers),
        avg_gain_pct=round(avg_gain, 2),
        avg_loss_pct=round(avg_loss, 2),
        win_rate=round(win_rate, 3),
    )


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
        # KARAR ADAY #717 — Mark TTLC Sec 1 6 zorunlu plan alani
        "plan_entry_trigger": body.plan_entry_trigger,
        "plan_stop": body.plan_stop,
        "plan_target": body.plan_target,
        "plan_size_pct": body.plan_size_pct,
        "plan_exit_strategy": body.plan_exit_strategy,
        "plan_time_horizon": body.plan_time_horizon,
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


# ── RBA Metrics ───────────────────────────────────────────────────────────────
# KARAR ADAY #722 (24 May 2026) — Mark Result-Based Analysis (RBA) endpoint.
# Mark TTLC Sec 4 birebir: "Know the truth about your trading."
# Sn. Ferit'in kapanan trade'lerinden istatistiksel anlamlı metrikler çıkarır,
# setup'ı bırakma kararı (should_drop_setup) için kullanılabilir.

class RbaMetrics(BaseModel):
    """Mark RBA metrikleri — Notebook B3 Modül 7.1 + NotebookLM Konu 14."""
    num_trades: int
    win_rate: float
    avg_gain_pct: float
    avg_loss_pct: float  # negatif
    largest_gain_pct: float
    largest_loss_pct: float
    adjusted_ratio: float       # (Win% × AvgGain) / (Loss% × |AvgLoss|)
    expectancy_pct: float       # (Win% × AvgGain) - (Loss% × |AvgLoss|)
    is_statistically_significant: bool  # >= 30 trade


class RbaSetupRecommendation(BaseModel):
    severity: Literal["OK", "INFO", "WARNING", "CRITICAL"]
    message: str


class RbaResponse(BaseModel):
    """RBA full response — metrikler + setup öneri + filtre meta."""
    metrics: RbaMetrics
    recommendation: RbaSetupRecommendation
    filter_strategy: Optional[str] = None
    filter_setup_type: Optional[str] = None


def _closed_trades_for_rba(
    strategy: Optional[str] = None,
    setup_type: Optional[str] = None,
) -> list[dict]:
    """Kapanmis trade'leri RBA hesabi icin filtreli getir."""
    try:
        all_trades = trades_get_all()
    except OperationalError:
        # DB unreachable — boş RBA döner (UI placeholder).
        return []

    closed = [
        t for t in all_trades
        if t.get("status") == "closed" and t.get("pl_pct") is not None
    ]
    if strategy:
        closed = [t for t in closed if t.get("strategy") == strategy]
    if setup_type:
        closed = [t for t in closed if t.get("setup_type") == setup_type]

    # compute_rba_metrics 'pnl_pct' anahtarini bekler, DB'den 'pl_pct' geliyor
    # — basit normalization.
    return [{"pnl_pct": float(t["pl_pct"])} for t in closed]


@app.get("/api/rba/metrics", response_model=RbaResponse)
def get_rba_metrics(
    strategy: Optional[str] = None,
    setup_type: Optional[str] = None,
) -> RbaResponse:
    """
    Mark RBA (Result-Based Analysis) metrikleri.

    Filtre: strategy ve/veya setup_type. Boş ise tüm kapanmış trade'ler.

    Mark TTLC Sec 4: "Know the truth about your trading."
    - num_trades >= 30 → istatistiksel anlamlı (Mark kuralı)
    - adjusted_ratio < 1.0 → CRITICAL (setup negatif edge → BIRAK)
    - abs(avg_loss) > avg_gain → WARNING (setup zayıflıyor)
    """
    feed = _closed_trades_for_rba(strategy=strategy, setup_type=setup_type)
    rba = compute_rba_metrics(feed)
    rec = should_drop_setup(rba)
    return RbaResponse(
        metrics=RbaMetrics(
            num_trades=rba.num_trades,
            win_rate=rba.win_rate,
            avg_gain_pct=rba.avg_gain_pct,
            avg_loss_pct=rba.avg_loss_pct,
            largest_gain_pct=rba.largest_gain_pct,
            largest_loss_pct=rba.largest_loss_pct,
            adjusted_ratio=rba.adjusted_ratio,
            expectancy_pct=rba.expectancy_pct,
            is_statistically_significant=rba.is_statistically_significant,
        ),
        recommendation=RbaSetupRecommendation(
            severity=rec.severity,  # type: ignore[arg-type]
            message=rec.message,
        ),
        filter_strategy=strategy,
        filter_setup_type=setup_type,
    )


# ── Pyramid Tier (KARAR #732) ─────────────────────────────────────────────────
# Mark KARAR #487 v20.98 3-Tier (Pilot/Standart/Full) + Mark X "Trades Working"
# guvenlik kilidi. Backend hesap, frontend (web/lib/pyramid-calculator.ts)
# fallback + UI gosterim (MarkPyramidCard).

class PyramidTierRequest(BaseModel):
    # P387: portfolio_value=0 -> ZeroDivisionError (position_pct hesabi). gt=0 zorunlu.
    position_value: float = Field(gt=0, description="Pozisyon dolar tutari > 0")
    portfolio_value: float = Field(gt=0, description="Portfoy dolar tutari > 0 (yuzde hesabi)")
    prev_tier_profitable: bool = False


class PyramidTierResponse(BaseModel):
    tier: Literal["BELOW_PILOT", "PILOT", "STANDARD", "FULL", "OVER_MAX"]
    position_pct: float
    severity: Literal["ok", "info", "warn", "violation"]
    next_tier: Optional[Literal["PILOT", "STANDARD", "FULL"]] = None
    mark_says: str
    # Mark KARAR #487 sabit referanslari (KALICI ILKE #4)
    pilot_range_pct: tuple[float, float] = MARK_PYRAMID_PILOT_PCT_RANGE
    standard_range_pct: tuple[float, float] = MARK_PYRAMID_STANDARD_PCT_RANGE
    full_range_pct: tuple[float, float] = MARK_PYRAMID_FULL_PCT_RANGE


@app.post("/api/pyramid/tier", response_model=PyramidTierResponse)
def evaluate_pyramid_tier(body: PyramidTierRequest) -> PyramidTierResponse:
    """KARAR #732 — Mark Pyramid Tier hesabi.

    Mark KARAR #487 v20.98 birebir 3-Tier + Mark X "Trades not working = no
    size increase" guvenlik kilidi. quanfina_math.compute_pyramid_tier
    fonksiyonu direkt cagrilir (DRY: TypeScript ve Python ayni mantik).
    """
    result = compute_pyramid_tier(
        position_value=body.position_value,
        portfolio_value=body.portfolio_value,
        prev_tier_profitable=body.prev_tier_profitable,
    )
    return PyramidTierResponse(**result)


# ── Carr Stage (KARAR #733) ───────────────────────────────────────────────────
# Tek hisse icin Carr Stage Analizi (Stan Weinstein 4-Stage) detay endpoint'i.

class CarrStageResponse(BaseModel):
    symbol: str
    stage: Optional[Literal[1, 2, 3, 4]] = None
    stage_label: str
    ma_value: Optional[float] = None
    price_vs_ma_pct: Optional[float] = None
    slope_pct_per_year: Optional[float] = None
    mark_says: str
    ma_window: int = 150
    # P413 (31 May 2026 - Kural #28 audit otonom mod):
    # is_mock flag. _mock_index_history (180 gun deterministik) kullanildiginda
    # True; AÇIK KONU #75 yfinance pipeline cozulunce False olur. Paper trading
    # AddTradeDialog Stage 4 alert MOCK veriyle calismasin diye UI'a sinyal.
    is_mock: bool = False


@app.get("/api/carr/stage/{symbol}", response_model=CarrStageResponse)
def get_carr_stage(symbol: str) -> CarrStageResponse:
    """KARAR #733 — Tek hisse Carr Stage Analizi detay.

    Sn. Ferit hisse ticker'i verir, 180 gun MOCK SPY-tipi history uretilir
    + compute_carr_stage cagrisi + Stan Weinstein 4-Stage detay yanit.

    Production'da yfinance/SQL real veri (ACIK KONU #75 yfinance pipeline).
    """
    sym = symbol.upper()
    # MOCK SPY pattern (ticker hash seed deterministik)
    start_price = 100.0 + (sum(ord(c) for c in sym) % 400)
    closes, volumes = _mock_index_history(sym, start_price, days=180)
    result = compute_carr_stage(closes, volumes, ma_window=150)
    return CarrStageResponse(
        symbol=sym,
        stage=result.get("stage"),
        stage_label=result.get("stage_label", "Belirsiz"),
        ma_value=result.get("ma_value"),
        price_vs_ma_pct=result.get("price_vs_ma_pct"),
        slope_pct_per_year=result.get("slope_pct_per_year"),
        mark_says=result.get("mark_says", ""),
        ma_window=150,
        is_mock=True,  # P413: _mock_index_history deterministik (ACIK KONU #75 cozulunce False)
    )


# ── Signals ───────────────────────────────────────────────────────────────────

_STATUS_RANK: dict[str, int] = {s: i for i, s in enumerate(_STATUS_HIERARCHY)}


class Signal(BaseModel):
    # KARAR #469 (20 May 2026) revize: konsensus mantığı kaldırıldı.
    # Her watchlist satırı = 1 Signal (NVDA-Minervini ayrı, NVDA-Carr ayrı).
    # UX Bölüm 4 madde 5 ("Her satır ayrı trade: kendi stop, kendi hedef, kendi R/R") ile uyumlu.
    # KARAR #473 (20 May 2026 ~07:30): stop_loss + target_price + risk_reward eklendi
    # (UX Bölüm 4 madde 6: "R/R'a göre sıralı"). Backend hesaplar, frontend gösterir.
    # KARAR #726 (24 May 2026): Mark Profili rozetleri (DRY MarkBadgeStrip 4. sayfa).
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
    # KARAR ADAY #726 — Sinyaller Mark Profili (MOCK feed + Migration 004-007 sonra canli)
    mark_signals: Optional[dict] = None
    # KARAR #733 alt-paket (Paket 81, 26 May 2026): Pivot breakout status
    # P70+P71 helper enrichment — sinyal listesinde AL/Zayıf/Yakın/Altı görünür
    pivot_status: Optional[Literal["CONFIRMED", "WEAK", "NEAR_PIVOT", "BELOW_PIVOT"]] = None
    # P417 (31 May 2026): Bağıl Hacim (Watchlist P416 paten) — Mark TLSMW Bol. 6
    # "Hacim teyit" canon. Bugün / 50-gün ortalama oranı. Watchlist'teki gibi
    # paper trading karar verirken pivot kırılım hacim teyit görünürlüğü.
    relative_volume: Optional[float] = None
    # P472 (15 Haz 2026): Kural #28 — stop/hedef/R-R MOCK-turevi mi (yfinance fail -> sentetik
    # volatilite). True ise UI Stop/Hedef/R-R'i amber isaretler (Fiyat quote_source paterni).
    # None = stop yok veya bilinmiyor; False = gercek yfinance bazli.
    stop_basis_is_mock: Optional[bool] = None


_PIVOT_STATUS_CACHE: dict[str, tuple[float, Optional[str]]] = {}
_PIVOT_STATUS_CACHE_TTL_SEC = 300


def _compute_signal_pivot_status(symbol: str, price: float) -> Optional[str]:
    """KARAR #733 alt-paket (Paket 81): Sinyal satırı için pivot status.

    OHLCV: _get_ohlcv (yfinance gerçek + MOCK fallback). yfinance taze
    olduğunda gerçek piyasa verisinden compute_pivot_breakout (Mark TLSMW
    Ch 10 canon) hesabı; network fail durumunda deterministik MOCK
    (sembol+tarih seed, UX kesintisiz). Paket 145 alt-katman: 5 dk cache
    (yfinance fetch maliyetli). Helper Exception sızdırmaz -> None
    döndürür (caller pivot_status alanını UI'da render etmez).

    Eski docstring "MOCK OHLCV üretip" yanıltıcıydı (Paket 384 düzeltme,
    30 May 2026) — gerçek wire P144 yfinance pipeline ile zaten canlıydı.
    """
    # Cache hit
    now = _time_module.time()
    cached = _PIVOT_STATUS_CACHE.get(symbol)
    if cached:
        ts, status = cached
        if now - ts < _PIVOT_STATUS_CACHE_TTL_SEC:
            return status
    try:
        bars = _get_ohlcv(symbol, price)
        closes = [b.close for b in bars]
        volumes = [b.volume for b in bars]
        result = compute_pivot_breakout(closes, volumes)
        status = result.get("status")
    except Exception:
        status = None
    _PIVOT_STATUS_CACHE[symbol] = (now, status)
    return status


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
        # KARAR #726: mark_signals MOCK lookup (_STOCK_MARK_SIGNALS dict)
        # Paket 147 (26 May 2026): _compute_live_mark_signals yfinance overlay (Watchlist pateni)
        def s(symbol, strategy, status, setup, rs, price, stop, target, added, new=False):
            # P417: relative_volume MOCK fallback — yfinance varsa gerçek hesap
            rv_val: Optional[float] = None
            try:
                bars = _get_ohlcv(symbol, price)
                if bars and len(bars) >= 52:
                    rv_dict = compute_relative_volume([b.volume for b in bars])
                    if rv_dict.get("rel_vol") is not None:
                        rv_val = float(rv_dict["rel_vol"])
            except Exception:
                pass
            return Signal(
                symbol=symbol, strategy=strategy, status=status, setup_type=setup,
                rs_rating=rs, price=price, stop_loss=stop, target_price=target,
                risk_reward=_calc_rr(price, stop, target),
                added_date=added, is_new_today=new,
                mark_signals=_compute_live_mark_signals(symbol, price),
                pivot_status=_compute_signal_pivot_status(symbol, price),
                relative_volume=rv_val,
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
    # P417 (31 May 2026): Paralel enrichment (P415 patenli) — 31 watchlist sıralı
    # yfinance fetch ~5s yerine ThreadPoolExecutor 20 worker ~1s. Pivot status
    # cache (5dk) zaten ısınmıştır watchlist isteğinden — burada hızlı.
    # Relative volume eklendi (P416 watchlist paterni).
    from concurrent.futures import ThreadPoolExecutor

    def _build_signal(row: WatchlistRow) -> Signal:
        added_prefix = (row.added_date or "")[:10]
        # P472 (15 Haz 2026): Kural #28 — bars kaynagini (gercek yfinance vs MOCK) izle ki
        # stop/hedef MOCK-turevi ise UI'da Fiyat gibi amber isaretlenebilsin. _get_ohlcv'nin
        # ic mantigini birebir tekrarliyoruz (real>=10 -> gercek, yoksa MOCK) ki kaynak flag'i
        # yakalanabilsin (_get_ohlcv imzasi degismez -> diger endpoint'ler etkilenmez).
        bars = []
        bars_is_mock: Optional[bool] = None
        try:
            _real = _fetch_ohlcv_real(row.symbol)
            if _real is not None and len(_real) >= 10:
                bars = _real
                bars_is_mock = False
            else:
                bars = _generate_ohlcv(row.symbol, row.price) or []
                bars_is_mock = True
        except Exception:
            bars = []
            bars_is_mock = None
        # Relative volume — yfinance + compute_relative_volume (Mark TLSMW Bol. 6)
        rv_val: Optional[float] = None
        try:
            if bars and len(bars) >= 52:
                rv_dict = compute_relative_volume([b.volume for b in bars])
                if rv_dict.get("rel_vol") is not None:
                    rv_val = float(rv_dict["rel_vol"])
        except Exception:
            pass
        # P466 (12 Haz 2026): Stop·Hedef·R/R canli hesap. web_watchlist'te kolon YOK ->
        # eskiden None idi (KARAR #473 acigi, /signals "—"). Mark Risk-first canon
        # (RiskSummaryCard P439 ile ayni): ATR stop (suggested_stop_normal=Entry-2.5xATR)
        # + 3R hedef (Mark R-multiple framework; sabit fiyat hedefi DEGIL — Kural #26
        # seffaf, hisseye gore degisir) + R/R. bars zaten cekildi, ek fetch yok.
        stop_val: Optional[float] = None
        target_val: Optional[float] = None
        rr_val: Optional[float] = None
        # P472: stop/hedef MOCK-turevi mi — sadece stop hesaplandiginda anlamli (Kural #28).
        stop_basis_is_mock: Optional[bool] = None
        try:
            if bars and len(bars) >= 30:
                entry = bars[-1].close
                atr_info = compute_atr_volatility(
                    [b.high for b in bars], [b.low for b in bars], [b.close for b in bars]
                )
                sn = atr_info.get("suggested_stop_normal")
                if sn is not None and entry > sn > 0:
                    stop_val = round(sn, 2)
                    target_val = round(entry + 3.0 * (entry - sn), 2)  # 3R (Mark ideal R-multiple)
                    rr_val = _calc_rr(entry, stop_val, target_val)
                    stop_basis_is_mock = bars_is_mock
        except Exception:
            pass
        return Signal(
            symbol=row.symbol,
            strategy=row.strategy,
            status=row.status,
            setup_type=row.setup_type,
            rs_rating=row.rs_rating,
            price=row.price,
            stop_loss=stop_val,
            target_price=target_val,
            risk_reward=rr_val,
            added_date=row.added_date,
            is_new_today=(added_prefix == today),
            mark_signals=_compute_live_mark_signals(row.symbol, row.price),
            pivot_status=_compute_signal_pivot_status(row.symbol, row.price),
            relative_volume=rv_val,
            stop_basis_is_mock=stop_basis_is_mock,
        )

    with ThreadPoolExecutor(max_workers=20) as pool:
        signals = list(pool.map(_build_signal, all_rows))

    # Sıralama: RS rating descending (UX Bölüm 4 madde 6 ile uyumlu — sonra R/R sırası)
    signals.sort(key=lambda s: -s.rs_rating)
    return signals


# =============================================================================
# Sprint 4-bis.7 — Faz 1 B paket: Mark Risk Advisor Endpoint
# Vizyon v22.00 tescili (24 May 2026)
# KARAR ADAY #914 + #969 + #970 — backend exposure
# =============================================================================

class RiskAdvisorRequest(BaseModel):
    """Mark Risk Advisor input — Trade form'dan gelir."""
    # P387: validation kampanyasi — finansal hesaplar gt=0 zorunlu, sayim ge=0.
    portfolio_value: float = Field(gt=0, description="Portfoy dolar tutari > 0")
    target_risk_pct: float = Field(default=2.0, gt=0, le=100, description="Risk %2 default (TTLC s.143)")
    max_stop_pct: float = Field(default=7.0, gt=0, le=100, description="Stop %7 default (TLSMW Ch 12)")
    total_positions: int = Field(default=0, ge=0, description="Acik trade sayisi >= 0")
    is_best_name: bool = False
    # RBA optional — kullanici gecmis trade istatistik bilgisi
    avg_gain_pct: Optional[float] = None
    avg_loss_pct: Optional[float] = None
    num_trades: Optional[int] = Field(default=None, ge=0)


class RiskAdvisorRule(BaseModel):
    rule_no: int
    rule: str
    passed: bool
    value: Optional[float] = None
    message: str
    mark_says: str
    critical: bool


class RiskAdvisorResponse(BaseModel):
    # Position sizing (KARAR #969)
    position_dollars: float
    position_pct: float
    risk_dollars: float
    risk_pct: float
    tier: str  # 'pilot_buy' | 'optimal' | 'aggressive'
    sizing_warnings: list[str]
    sizing_says: str
    # Dynamic stop (KARAR #914)
    recommended_stop_pct: float
    stop_method: str  # 'rba_based' | 'fallback'
    stop_absolute_cap_applied: bool
    stop_says: str
    # 6-Rule check (KARAR #970)
    six_rule_all_pass: bool
    six_rule_pass_count: int
    six_rule_critical_violations: list[int]
    six_rules: list[RiskAdvisorRule]
    # Mark KESIN sabitler — UI'da göstermek için
    mark_constants: dict


@app.post("/api/risk/advisor", response_model=RiskAdvisorResponse)
def risk_advisor(req: RiskAdvisorRequest) -> RiskAdvisorResponse:
    """Mark Risk Advisor — Trade form için canlı pozisyon size + 6-rule danışmanlık.

    Faz 1 B paket UI tamamlayıcı endpoint.
    Detay: notebook/Sprint_4_bis_7_Mark_HASSAS_Tarama.md
    """
    # 1) Position sizing (KARAR #969)
    sizing = mark_position_sizer(
        portfolio_value=req.portfolio_value,
        target_risk_pct=req.target_risk_pct,
        max_stop_pct=req.max_stop_pct,
    )

    # 2) Dynamic stop (KARAR #914) — RBA varsa kullan
    rba_obj = None
    if (req.avg_gain_pct is not None
            and req.avg_loss_pct is not None
            and req.num_trades is not None
            and req.num_trades >= 1):
        from quanfina_math import RBAMetrics
        rba_obj = RBAMetrics(
            num_trades=req.num_trades,
            win_rate=0.5,  # placeholder; advisor stop için sadece avg_gain kullanılır
            avg_gain_pct=req.avg_gain_pct,
            avg_loss_pct=req.avg_loss_pct,
            largest_gain_pct=req.avg_gain_pct * 2,
            largest_loss_pct=req.avg_loss_pct * 2,
            adjusted_ratio=0.0,
            expectancy_pct=0.0,
            is_statistically_significant=req.num_trades >= 30,
        )

    stop_advice = compute_dynamic_stop(rba_obj, fallback_pct=req.max_stop_pct)

    # 3) 6-Rule check (KARAR #970)
    six_rule = mark_six_rule_check(
        risk_pct=req.target_risk_pct,
        stop_pct=req.max_stop_pct,
        avg_loss_pct=req.avg_loss_pct,
        position_pct=sizing.get('position_pct', 0.0),
        is_best_name=req.is_best_name,
        total_positions=req.total_positions,
    )

    rule_list = [
        RiskAdvisorRule(
            rule_no=r['rule_no'],
            rule=r['rule'],
            passed=r['pass'],
            value=r['value'] if isinstance(r['value'], (int, float)) else None,
            message=r['message'],
            mark_says=r['mark_says'],
            critical=r['critical'],
        )
        for r in six_rule['rules']
    ]

    return RiskAdvisorResponse(
        position_dollars=sizing.get('position_dollars', 0.0),
        position_pct=sizing.get('position_pct', 0.0),
        risk_dollars=sizing.get('risk_dollars', 0.0),
        risk_pct=sizing.get('risk_pct', req.target_risk_pct),
        tier=sizing.get('tier', 'pilot_buy'),
        sizing_warnings=sizing.get('warnings', []),
        sizing_says=sizing.get('mark_says', ''),
        recommended_stop_pct=stop_advice['recommended_stop_pct'],
        stop_method=stop_advice['method'],
        stop_absolute_cap_applied=stop_advice['absolute_cap_applied'],
        stop_says=stop_advice['mark_says'],
        six_rule_all_pass=six_rule['all_pass'],
        six_rule_pass_count=six_rule['pass_count'],
        six_rule_critical_violations=six_rule['critical_violations'],
        six_rules=rule_list,
        mark_constants={
            'stop_absolute_cap_pct': MARK_STOP_ABSOLUTE_CAP_PCT,
            'equity_risk_min_pct': MARK_EQUITY_RISK_MIN_PCT,
            'equity_risk_max_pct': MARK_EQUITY_RISK_MAX_PCT,
            'position_max_pct': MARK_POSITION_MAX_PCT,
            'position_optimal_range': list(MARK_POSITION_OPTIMAL_PCT_RANGE),
            'portfolio_optimal_stocks': list(MARK_PORTFOLIO_OPTIMAL_STOCKS),
            'portfolio_max_stocks': MARK_PORTFOLIO_MAX_STOCKS,
        },
    )


# =============================================================================
# Sprint 4-bis.7 — Faz 2 backend: EPS Acceleration + Code 33 endpoint'leri
# Vizyon v22.00 tescili (KARAR ADAY #834 + #855)
# Detay: notebook/Sprint_4_bis_7_Mark_HASSAS_Tarama.md
# =============================================================================

class EpsAccelerationRequest(BaseModel):
    """Mark EPS Acceleration Detector input.

    eps_growth_yoy_last_4q: Son 4 çeyrek YoY EPS büyüme oranı %
        [q-3, q-2, q-1, current] sırasıyla (en eski → en yeni)
        Mark TLSMW s.131 örnek: [-5.0, 10.0, 28.0, 56.0]
    """
    eps_growth_yoy_last_4q: list[float]


class EpsAccelerationResponse(BaseModel):
    accelerating: bool
    magnitude_pct_pts: float
    mark_90pct_rule: bool
    phase: str  # 'accelerating' | 'decelerating' | 'flat' | 'invalid'
    tier: str   # 'below_minimum' | 'minimum' | 'superperformance' | 'bull_market' | 'turnaround'
    mark_says: str
    quarters_count: int
    mark_constants: dict


@app.post("/api/risk/eps-acceleration", response_model=EpsAccelerationResponse)
def risk_eps_acceleration(req: EpsAccelerationRequest) -> EpsAccelerationResponse:
    """KARAR ADAY #834 — Mark EPS Acceleration Detector endpoint.

    Mark TLSMW s.131:
        "More than 90 percent of the biggest stock market winners showed
        some form of earnings acceleration before or during their huge price moves."
    """
    result = detect_eps_acceleration(req.eps_growth_yoy_last_4q)
    return EpsAccelerationResponse(
        accelerating=result['accelerating'],
        magnitude_pct_pts=result['magnitude_pct_pts'],
        mark_90pct_rule=result['mark_90pct_rule'],
        phase=result['phase'],
        tier=result['tier'],
        mark_says=result['mark_says'],
        quarters_count=result['quarters_count'],
        mark_constants={
            'eps_min_growth_pct': MARK_EPS_MIN_GROWTH_PCT,
            'eps_superperformance_pct': MARK_EPS_SUPERPERFORMANCE_PCT,
            'eps_bull_market_pct': MARK_EPS_BULL_MARKET_PCT,
            'eps_turnaround_pct': MARK_EPS_TURNAROUND_PCT,
            'eps_90pct_rule_threshold': MARK_EPS_90PCT_RULE_THRESHOLD,
        },
    )


class Code33Request(BaseModel):
    """Mark Code 33 Detector input.

    3 ardışık array, her biri 4-çeyrek YoY büyüme oranı %.
    Mark TLSMW s.173 — superperformance condition.
    """
    eps_growth_yoy_last_4q: list[float]
    sales_growth_yoy_last_4q: list[float]
    net_margin_last_4q: list[float]


class Code33Response(BaseModel):
    pattern: str  # 'CODE_33' | 'partial' | 'none'
    eps_accel: bool
    sales_accel: bool
    margin_expanding: bool
    pass_count: int  # 0-3
    tier: str  # 'elite' | 'partial_2' | 'partial_1' | 'none'
    mark_says: str


@app.post("/api/risk/code-33", response_model=Code33Response)
def risk_code_33(req: Code33Request) -> Code33Response:
    """KARAR ADAY #855 — Mark Code 33 Detector endpoint.

    Mark TLSMW s.173:
        "Code 33 situation: three quarters of acceleration in earnings, sales,
        AND profit margins. That's a potent recipe."

    Mark Monster Beverage (MNST) 2003-2005 classic Code 33 reference.
    """
    result = detect_code_33(
        eps_growth_yoy_last_4q=req.eps_growth_yoy_last_4q,
        sales_growth_yoy_last_4q=req.sales_growth_yoy_last_4q,
        net_margin_last_4q=req.net_margin_last_4q,
    )
    return Code33Response(
        pattern=result['pattern'],
        eps_accel=result['eps_accel'],
        sales_accel=result['sales_accel'],
        margin_expanding=result['margin_expanding'],
        pass_count=result['pass_count'],
        tier=result['tier'],
        mark_says=result['mark_says'],
    )


# =============================================================================
# Sprint 4-bis.7 — Faz 2 genişletme endpoint'leri (KARAR #893 + #882 + #864)
# Vizyon v22.03 tescili
# =============================================================================

class DailyBar(BaseModel):
    close: float
    high: Optional[float] = None
    low: Optional[float] = None
    volume: int = 0


class TennisBallRequest(BaseModel):
    # P387: breakout_date_idx ge=0 (negatif index helper'da edge case)
    breakout_date_idx: int = Field(ge=0, description="Breakout gun indeksi >= 0")
    daily_history: list[DailyBar]


class TennisBallResponse(BaseModel):
    pattern: str  # TENNIS_BALL / EGG / STILL_RUNNING / INVALID
    pullback_days: Optional[int] = None
    recovery_days: Optional[int] = None
    recovered: bool
    pullback_depth_pct: Optional[float] = None
    mark_says: str


@app.post("/api/risk/tennis-ball", response_model=TennisBallResponse)
def risk_tennis_ball(req: TennisBallRequest) -> TennisBallResponse:
    """KARAR ADAY #893 — Mark Tennis Ball Detector endpoint (TLSMW s.253)."""
    history_dicts = [
        {
            'close': b.close,
            'high': b.high if b.high is not None else b.close * 1.01,
            'low': b.low if b.low is not None else b.close * 0.99,
            'volume': b.volume,
        }
        for b in req.daily_history
    ]
    result = detect_tennis_ball(req.breakout_date_idx, history_dicts)
    return TennisBallResponse(
        pattern=result['pattern'],
        pullback_days=result['pullback_days'],
        recovery_days=result['recovery_days'],
        recovered=result['recovered'],
        pullback_depth_pct=result['pullback_depth_pct'],
        mark_says=result['mark_says'],
    )


class VolumeAsymmetryRequest(BaseModel):
    daily_history: list[DailyBar]
    # P387: lookback gt=0 (negatif/sifir lookback hesabi anlamsiz)
    lookback_days: int = Field(default=20, gt=0, description="Lookback gun sayisi > 0")


class VolumeAsymmetryResponse(BaseModel):
    asymmetry_ratio: float
    up_days_count: int
    down_days_count: int
    up_volume_avg: float
    down_volume_avg: float
    tier: str
    mark_says: str


@app.post("/api/risk/volume-asymmetry", response_model=VolumeAsymmetryResponse)
def risk_volume_asymmetry(req: VolumeAsymmetryRequest) -> VolumeAsymmetryResponse:
    """KARAR ADAY #882 — Mark Volume Asymmetry endpoint (TLSMW s.234)."""
    history_dicts = [{'close': b.close, 'volume': b.volume} for b in req.daily_history]
    result = compute_volume_asymmetry(history_dicts, lookback_days=req.lookback_days)
    return VolumeAsymmetryResponse(
        asymmetry_ratio=result['asymmetry_ratio'],
        up_days_count=result['up_days_count'],
        down_days_count=result['down_days_count'],
        up_volume_avg=result['up_volume_avg'],
        down_volume_avg=result['down_volume_avg'],
        tier=result['tier'],
        mark_says=result['mark_says'],
    )


class LeaderFingerprintRequest(BaseModel):
    advance_segments: list[float]   # pozitif %
    pullback_segments: list[float]  # pozitif %


class LeaderFingerprintResponse(BaseModel):
    pattern: str  # LEADER_FINGERPRINT / LEADER_PARTIAL / NOT_LEADER / INVALID
    advances_in_range: int
    pullbacks_in_range: int
    total_advances: int
    total_pullbacks: int
    advance_pct_in_range: float
    pullback_pct_in_range: float
    tier: str
    mark_says: str


@app.post("/api/risk/leader-fingerprint", response_model=LeaderFingerprintResponse)
def risk_leader_fingerprint(req: LeaderFingerprintRequest) -> LeaderFingerprintResponse:
    """KARAR ADAY #864 — Mark Leader Behavior Fingerprint endpoint (TLSMW s.184)."""
    result = detect_leader_fingerprint(req.advance_segments, req.pullback_segments)
    return LeaderFingerprintResponse(
        pattern=result['pattern'],
        advances_in_range=result['advances_in_range'],
        pullbacks_in_range=result['pullbacks_in_range'],
        total_advances=result['total_advances'],
        total_pullbacks=result['total_pullbacks'],
        advance_pct_in_range=result['advance_pct_in_range'],
        pullback_pct_in_range=result['pullback_pct_in_range'],
        tier=result['tier'],
        mark_says=result['mark_says'],
    )
