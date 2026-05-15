"""
Quanfina FastAPI — POC ADIM 5
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

from fastapi import FastAPI, HTTPException
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


# Build mock data — consensus computed automatically
from collections import defaultdict as _dd

_RAW: list[tuple] = [
    # symbol, strategy, status, price, added_date, setup_type, pivot_price, note, rs_rating
    # ── Minervini (20 satır) ──────────────────────────────────────
    ("NVDA", "minervini", "buy",     875.40, "2026-05-13", "VCP",   820.00, None,          97),
    ("AVGO", "minervini", "buy",    1680.20, "2026-05-12", None,   1620.00, None,          94),
    ("META", "minervini", "focus",   525.80, "2026-05-10", None,    505.00, None,          92),
    ("MSFT", "minervini", "focus",   415.60, "2026-05-10", None,      None, None,          88),
    ("COST", "minervini", "focus",   890.30, "2026-05-09", None,      None, None,          85),
    ("GOOGL","minervini", "on_deck", 178.40, "2026-05-08", None,      None, None,          83),
    ("ASML", "minervini", "on_deck", 742.10, "2026-05-08", None,      None, None,          81),
    ("AMZN", "minervini", "on_deck", 196.70, "2026-05-07", None,      None, None,          80),
    ("ORCL", "minervini", "on_deck", 148.90, "2026-05-05", None,      None, None,          76),
    ("CRM",  "minervini", "on_deck", 296.40, "2026-05-05", None,      None, None,          74),
    ("NFLX", "minervini", "watch",   672.30, "2026-05-03", None,      None, None,          79),
    ("AMD",  "minervini", "watch",   158.70, "2026-05-03", None,      None, None,          73),
    ("ADBE", "minervini", "watch",   394.50, "2026-05-02", None,      None, None,          71),
    ("V",    "minervini", "watch",   275.80, "2026-05-01", None,      None, None,          75),
    ("MA",   "minervini", "watch",   477.20, "2026-05-01", None,      None, None,          73),
    ("JPM",  "minervini", "watch",   215.40, "2026-04-28", None,      None, None,          70),
    ("PLTR", "minervini", "watch",    22.80, "2026-04-25", None,      None, None,          65),
    ("COIN", "minervini", "watch",   152.40, "2026-04-24", None,      None, None,          68),
    ("DASH", "minervini", "watch",   118.60, "2026-04-22", None,      None, None,          63),
    ("SHOP", "minervini", "watch",    74.20, "2026-04-18", None,      None, None,          61),
    # ── Carr (10 satır) ──────────────────────────────────────────
    ("NVDA", "carr",      "focus",   875.40, "2026-05-13", "Pullback",           820.00, "MA50 destek", 97),
    ("META", "carr",      "focus",   525.80, "2026-05-10", "Pullback",           505.00, None,          92),
    ("GOOGL","carr",      "on_deck", 178.40, "2026-05-08", "Coiled Spring",        None, None,          83),
    ("AMZN", "carr",      "on_deck", 196.70, "2026-05-07", "Coiled Spring",        None, None,          80),
    ("MSFT", "carr",      "on_deck", 415.60, "2026-05-10", "Bullish Divergence",   None, None,          88),
    ("AAPL", "carr",      "watch",   182.30, "2026-05-03", "Bullish Divergence",   None, None,          72),
    ("TSM",  "carr",      "watch",   142.80, "2026-05-02", "Blue Sky Breakout",    None, None,          78),
    ("AMD",  "carr",      "watch",   158.70, "2026-05-03", "Blue Sky Breakout",    None, None,          73),
    ("LLY",  "carr",      "watch",   724.50, "2026-04-28", "Bullish Base Breakout",None, None,          82),
    ("UBER", "carr",      "watch",    68.40, "2026-04-25", "Bullish Base Breakout",None, None,          70),
]

_sym_strategies: dict[str, list[str]] = _dd(list)
for _r in _RAW:
    _sym_strategies[_r[0]].append(_r[1])

MOCK_WATCHLIST: list[WatchlistRow] = [
    WatchlistRow(
        symbol=r[0], strategy=r[1], status=r[2], price=r[3],
        added_date=r[4], setup_type=r[5], pivot_price=r[6], note=r[7],
        rs_rating=r[8],
        consensus_count=len(_sym_strategies[r[0]]),
        consensus_strategies=_sym_strategies[r[0]],
    )
    for r in _RAW
]


@app.get("/api/watchlist", response_model=list[WatchlistRow])
def get_watchlist() -> list[WatchlistRow]:
    return MOCK_WATCHLIST
