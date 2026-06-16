"""
KARAR #733 alt-paket (Paket 173): /api/stock/{symbol}/quote pytest.

Paper trading canlı fiyat altyapısı (P150):
- yfinance success path (gerçek 2-bar fetch + change calc)
- MOCK fallback (_STOCK_BY_SYM Quanfina evreni)
- Unknown symbol → 100.0 generic mock
- _OHLCV_CACHE 5dk TTL paylaşımı (Watchlist enrich ile aynı)

Network bağımsız: _OHLCV_CACHE manuel seed ile yfinance call bypass.
"""
import sys
import time
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent
API_DIR = PROJECT_ROOT / "api"
for d in (str(PROJECT_ROOT), str(API_DIR)):
    if d not in sys.path:
        sys.path.insert(0, d)

try:
    from fastapi.testclient import TestClient
    import main as api_main
    from main import OhlcvBar
except ImportError:
    pytest.skip("fastapi TestClient veya api/main.py yok", allow_module_level=True)


@pytest.fixture(scope="module")
def client():
    return TestClient(api_main.app)


@pytest.fixture(autouse=True)
def clear_caches():
    """Her test ayrı cache state'i."""
    api_main._OHLCV_CACHE.clear()
    yield
    api_main._OHLCV_CACHE.clear()


def _seed_cache(symbol: str, bars: list[OhlcvBar]):
    """OHLCV cache'i manuel doldur — yfinance call bypass."""
    api_main._OHLCV_CACHE[symbol.upper()] = (time.time(), bars)


def _make_bars(prices: list[float]) -> list[OhlcvBar]:
    """Test için minimal OHLCV bar listesi."""
    return [
        OhlcvBar(
            time=f"2026-05-{20+i:02d}",
            open=p, high=p * 1.01, low=p * 0.99, close=p, volume=1_000_000,
        )
        for i, p in enumerate(prices)
    ]


# =====================================================================
# Endpoint shape + happy path
# =====================================================================

class TestQuoteEndpointShape:
    def test_response_fields(self, client):
        """Response: symbol, price, change_dollar, change_pct, source."""
        _seed_cache("NVDA", _make_bars([215.0, 217.5]))
        r = client.get("/api/stock/NVDA/quote")
        assert r.status_code == 200
        data = r.json()
        for field in ("symbol", "price", "change_dollar", "change_pct", "source"):
            assert field in data

    def test_symbol_uppercase_normalization(self, client):
        """nvda → NVDA (case-insensitive lookup)."""
        _seed_cache("NVDA", _make_bars([215.0, 217.5]))
        r = client.get("/api/stock/nvda/quote")
        assert r.status_code == 200
        assert r.json()["symbol"] == "NVDA"


# =====================================================================
# yfinance success path — 2 bar fetch + change calc
# =====================================================================

class TestYfinanceSuccessPath:
    def test_price_from_last_bar(self, client):
        """price = bars[-1].close."""
        _seed_cache("NVDA", _make_bars([200.0, 220.0]))
        r = client.get("/api/stock/NVDA/quote")
        data = r.json()
        assert abs(data["price"] - 220.0) < 0.01

    def test_change_dollar_calc(self, client):
        """change_dollar = last.close - prev.close."""
        _seed_cache("MSFT", _make_bars([400.0, 425.0]))
        r = client.get("/api/stock/MSFT/quote")
        data = r.json()
        assert abs(data["change_dollar"] - 25.0) < 0.01

    def test_change_pct_calc(self, client):
        """change_pct = (change_dollar / prev) × 100."""
        _seed_cache("AAPL", _make_bars([200.0, 210.0]))
        r = client.get("/api/stock/AAPL/quote")
        data = r.json()
        assert abs(data["change_pct"] - 5.0) < 0.01

    def test_source_yfinance_when_bars_present(self, client):
        """OHLCV bars varsa source='yfinance'."""
        _seed_cache("TSLA", _make_bars([400.0, 410.0]))
        r = client.get("/api/stock/TSLA/quote")
        assert r.json()["source"] == "yfinance"

    def test_single_bar_no_change(self, client):
        """1 bar varsa: price OK, change_dollar/pct None."""
        _seed_cache("SOLO", _make_bars([100.0]))
        r = client.get("/api/stock/SOLO/quote")
        data = r.json()
        assert abs(data["price"] - 100.0) < 0.01
        assert data["change_dollar"] is None
        assert data["change_pct"] is None


# =====================================================================
# MOCK fallback — _STOCK_BY_SYM + generic 100.0
# =====================================================================

class TestMockFallback:
    @pytest.fixture
    def no_yfinance(self, monkeypatch):
        """#18 (denetim): _fetch_ohlcv_real → None (yfinance erişilemez simülasyonu).
        MOCK fallback yolu DETERMINISTIK test edilir — eski 'yfinance VEYA mock ikisi
        de geçerli' zayıf assertion + canlı network çağrısı yerine kesin kontrol."""
        monkeypatch.setattr(api_main, "_fetch_ohlcv_real", lambda *a, **k: None)

    def test_known_symbol_stock_by_sym(self, client, no_yfinance):
        """yfinance yok + _STOCK_BY_SYM'de var (NVDA) → source='mock' + _STOCK_BY_SYM fiyatı."""
        r = client.get("/api/stock/NVDA/quote")
        assert r.status_code == 200
        data = r.json()
        assert data["symbol"] == "NVDA"
        assert data["source"] == "mock"
        assert data["price"] > 0  # _STOCK_BY_SYM NVDA MOCK fiyatı (deterministik)

    def test_unknown_symbol_generic_mock(self, client, no_yfinance):
        """yfinance yok + _STOCK_BY_SYM'de yok → generic 100.0 + source='mock'."""
        r = client.get("/api/stock/ZZZZTEST/quote")
        assert r.status_code == 200
        data = r.json()
        assert data["source"] == "mock"
        assert data["price"] == 100.0  # get_stock_quote generic fallback


# =====================================================================
# Edge cases
# =====================================================================

class TestEdgeCases:
    def test_zero_prev_close_no_pct(self, client):
        """prev_close = 0 → change_pct None (division by zero koruması)."""
        _seed_cache("EDGE1", _make_bars([0.0, 100.0]))
        r = client.get("/api/stock/EDGE1/quote")
        data = r.json()
        assert data["change_pct"] is None

    def test_round_to_4_decimal(self, client):
        """Price round(4) — yfinance precision'ı korumalı."""
        _seed_cache("PRECISE", _make_bars([100.0, 100.123456789]))
        r = client.get("/api/stock/PRECISE/quote")
        data = r.json()
        # 100.1235 (round 4)
        assert abs(data["price"] - 100.1235) < 0.0001

    def test_negative_change(self, client):
        """Düşen fiyat: change_dollar < 0."""
        _seed_cache("FALL", _make_bars([220.0, 210.0]))
        r = client.get("/api/stock/FALL/quote")
        data = r.json()
        assert data["change_dollar"] < 0
        assert data["change_pct"] < 0
