"""
P479 (#1003, Kural #28): /api/stock/{symbol}/info canli fiyat once + price_source seffaflik.

Bug: get_stock_info MOCK_STOCKS'u ILK siraya koyuyordu -> NVDA icin stale
MOCK 875.40 donuyordu (canli ~209). Frontend zaten /quote ile ezer (P431/P437),
ama ham /info API kontrati yaniltiyordu (test/gelecek tuketici). Fix: get_stock_info
mevcut get_stock_quote'u (DRY tek kaynak) reuse eder — source=="yfinance" ise canli
fiyat MOCK/DB'yi ezer + price_source="yfinance"; degilse fallback + price_source=
"mock|watchlist|scan".

Deterministik: get_stock_quote monkeypatch (network yok). DRY -> _live_price
duplikasyonu kaldirildi, get_stock_quote tek canli-fiyat kaynagi.
"""
import pytest
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent
API_DIR = PROJECT_ROOT / "api"
for d in (str(PROJECT_ROOT), str(API_DIR)):
    if d not in sys.path:
        sys.path.insert(0, d)

try:
    from fastapi.testclient import TestClient
    import main as api_main
    from main import StockQuote
except ImportError:
    pytest.skip("fastapi TestClient veya api/main.py yok", allow_module_level=True)


@pytest.fixture
def client():
    return TestClient(api_main.app)


def _quote(price, change_pct, source):
    return StockQuote(symbol="X", price=price, change_pct=change_pct, source=source)


class TestPriceSourceField:
    def test_field_present(self, client):
        """price_source alani her zaman response'ta (Optional ama dolu)."""
        r = client.get("/api/stock/NVDA/info")
        assert r.status_code == 200
        assert "price_source" in r.json()


class TestLivePriceOverridesMock:
    """Kural #28: canli fiyat (get_stock_quote source=yfinance) stale MOCK'u EZER."""

    def test_live_overrides_nvda_mock(self, client, monkeypatch):
        # NVDA MOCK_STOCKS price=875.40; canli 209.28 ezmelidir
        monkeypatch.setattr(api_main, "get_stock_quote",
                            lambda s: _quote(209.28, 1.85, "yfinance"))
        d = client.get("/api/stock/NVDA/info").json()
        assert d["price"] == 209.28, "canli fiyat MOCK 875.40'i ezmeli"
        assert d["change_pct"] == 1.85
        assert d["price_source"] == "yfinance"

    def test_live_overrides_unknown_symbol(self, client, monkeypatch):
        # Bilinmeyen sembol -> generic fallback (100.0); canli ezmeli
        monkeypatch.setattr(api_main, "get_stock_quote",
                            lambda s: _quote(42.5, -0.7, "yfinance"))
        d = client.get("/api/stock/ZZZZ/info").json()
        assert d["price"] == 42.5
        assert d["price_source"] == "yfinance"


class TestMockFallbackFlagged:
    """yfinance fail (get_stock_quote source=mock) -> fallback + price_source isaretli."""

    def test_nvda_fallback_is_mock(self, client, monkeypatch):
        monkeypatch.setattr(api_main, "get_stock_quote",
                            lambda s: _quote(875.40, 2.35, "mock"))
        d = client.get("/api/stock/NVDA/info").json()
        # source!=yfinance -> NVDA MOCK_STOCKS branch -> price_source="mock"
        assert d["price_source"] == "mock"
        assert d["price"] > 0  # fallback degeri (MOCK 875.40)

    def test_unknown_fallback_is_mock(self, client, monkeypatch):
        monkeypatch.setattr(api_main, "get_stock_quote",
                            lambda s: _quote(100.0, 0.0, "mock"))
        d = client.get("/api/stock/ZZZZ/info").json()
        assert d["price_source"] == "mock"
        assert d["price"] == 100.0  # generic fallback


class TestDryReuse:
    """DRY: _live_price duplikasyonu kaldirildi, get_stock_quote tek kaynak."""

    def test_no_duplicate_live_price_helper(self):
        assert not hasattr(api_main, "_live_price"), \
            "_live_price duplikasyonu kaldirilmali (get_stock_quote tek canli-fiyat kaynagi)"
