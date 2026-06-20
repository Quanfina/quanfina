"""
P563 (20 Haz 2026): get_stock_info gerçek market_cap — hardcoded stale string kaldırıldı.

minervini_scans.market_cap (milyon USD, gerçek günlük Finviz) → formatlı ($X.XXT/$XXXB/$XXM).
Hardcoded _STOCK_META "$2.2T" (NVDA gerçek $5.05T) stale fallback'e indi (Kural #28). NaN/None
guard (uydurma YOK — Kural #26).
"""
import sys
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
    _HAS_API = True
except ImportError:
    _HAS_API = False


@pytest.mark.skipif(not _HAS_API, reason="fastapi yok")
class TestFormatMarketCap:
    def test_trillion(self):
        assert api_main._format_market_cap(5_049_087) == "$5.05T"  # NVDA gerçek
        assert api_main._format_market_cap(4_367_579) == "$4.37T"  # AAPL gerçek

    def test_billion_large(self):
        assert api_main._format_market_cap(322_211) == "$322B"  # SNDK

    def test_billion_small_one_decimal(self):
        assert api_main._format_market_cap(5_475) == "$5.5B"  # AXTI (<$10B → 1 ondalık)

    def test_million(self):
        assert api_main._format_market_cap(850) == "$850M"
        assert api_main._format_market_cap(22.84) == "$23M"

    def test_nan_none_nonpositive_guard(self):
        # MOCK/uydurma YOK (Kural #26/#28): geçersiz → None (caller stale fallback)
        assert api_main._format_market_cap(None) is None
        assert api_main._format_market_cap(float("nan")) is None
        assert api_main._format_market_cap(0) is None
        assert api_main._format_market_cap(-100) is None


@pytest.mark.skipif(not _HAS_API, reason="fastapi yok")
class TestStockInfoMarketCap:
    @pytest.fixture(scope="class")
    def client(self):
        return TestClient(api_main.app)

    def test_scan_symbol_uses_real_market_cap(self, client, monkeypatch):
        # Taranmış sembol → gerçek scan market_cap (hardcoded meta DEĞİL)
        monkeypatch.setattr(api_main, "_fetch_scan_symbol_data", lambda s: {
            "ticker": "ZZZ", "price": 100.0, "rs_ibd": 80, "company": "Test Corp",
            "sector": "Technology", "industry": "Software", "market_cap": 1_500_000,  # $1.5T
        })
        # MOCK_STOCKS + watchlist'te olmasın → scan branch
        monkeypatch.setattr(api_main, "_STOCK_BY_SYM", {})
        monkeypatch.setattr(api_main, "watchlist_get_all", lambda: [])
        r = client.get("/api/stock/ZZZ/info")
        assert r.status_code == 200
        assert r.json()["market_cap"] == "$1.50T"

    def test_nan_market_cap_falls_back_not_crash(self, client, monkeypatch):
        # Gerçek market_cap NaN → None → meta hardcoded fallback (— veya stale), 500 DEĞİL
        monkeypatch.setattr(api_main, "_fetch_scan_symbol_data", lambda s: {
            "ticker": "ZZZ", "price": 100.0, "rs_ibd": 80, "company": "Test Corp",
            "sector": "Technology", "industry": "Software", "market_cap": float("nan"),
        })
        monkeypatch.setattr(api_main, "_STOCK_BY_SYM", {})
        monkeypatch.setattr(api_main, "watchlist_get_all", lambda: [])
        r = client.get("/api/stock/ZZZ/info")
        assert r.status_code == 200
        # NaN → "$" formatı YOK (— veya hardcoded meta string)
        assert "T" not in r.json()["market_cap"] or r.json()["market_cap"] == "—"
