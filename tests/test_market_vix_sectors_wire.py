"""
P398: VIX + top/bottom_sectors MOCK -> gercek wire regresyon testleri.

Sn. Ferit Piyasa Durumu sayfasinda 14.2 hardcoded VIX + 5 sabit sektor goruyordu.
P398 fix: yfinance ^VIX (son kapanis) + sector_rotation tablosu (gercek 11 SPDR
ETF taramasi) wire. MOCK fallback (yfinance fail veya DB bos) korundu.

Cloud SQL flaky'den bagimsiz — _fetch_ohlcv_real ve sector_rotation_get_latest
monkeypatch ile mock'lanir. Helper kontratlari uctan uca test edilir.
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
except ImportError:
    pytest.skip("fastapi yok", allow_module_level=True)


@pytest.fixture(scope="module")
def client():
    return TestClient(api_main.app)


class TestVixWire:
    """P398: VIX yfinance ^VIX gercek + MOCK fallback (14.2)."""

    def test_vix_from_yfinance_when_available(self, client, monkeypatch):
        # _fetch_ohlcv_real("^VIX", 2) -> 2 bar, son kapanis vix degeri
        class _Bar:
            def __init__(self, close): self.close = close
        monkeypatch.setattr(
            api_main, "_fetch_ohlcv_real",
            lambda symbol, n: [_Bar(20.0), _Bar(22.55)] if symbol == "^VIX" else None,
        )
        r = client.get("/api/market/status")
        assert r.status_code == 200
        assert r.json()["vix"] == 22.55  # round(22.55, 2)

    def test_vix_fallback_when_yfinance_fail(self, client, monkeypatch):
        # _fetch_ohlcv_real -> None (network fail / rate limit)
        monkeypatch.setattr(api_main, "_fetch_ohlcv_real", lambda symbol, n: None)
        r = client.get("/api/market/status")
        # MOCK fallback (14.2 default — yfinance fail UX kesintisiz)
        assert r.json()["vix"] == 14.2

    def test_vix_rounded_2_decimal(self, client, monkeypatch):
        class _Bar:
            def __init__(self, close): self.close = close
        monkeypatch.setattr(
            api_main, "_fetch_ohlcv_real",
            lambda symbol, n: [_Bar(15.0), _Bar(18.987654)] if symbol == "^VIX" else None,
        )
        r = client.get("/api/market/status")
        assert r.json()["vix"] == 18.99  # round 2 decimal


class TestSectorsWire:
    """P398: sector_rotation tablosu gercek 11 SPDR ETF + MOCK fallback."""

    def test_top_bottom_from_db_when_available(self, client, monkeypatch):
        # 5 sektor mock — perf_1w siralanmali (DESC top)
        mock_sectors = [
            {"ticker": "XLK", "sector_name": "Technology",  "perf_1w": 6.95},
            {"ticker": "XLE", "sector_name": "Energy",      "perf_1w": 3.20},
            {"ticker": "XLI", "sector_name": "Industrials", "perf_1w": 1.80},
            {"ticker": "XLV", "sector_name": "Health Care", "perf_1w": -0.45},
            {"ticker": "XLU", "sector_name": "Utilities",   "perf_1w": -1.29},
        ]
        monkeypatch.setattr(api_main, "sector_rotation_get_latest", lambda: mock_sectors)
        r = client.get("/api/market/status")
        d = r.json()
        # Top 3 (perf_1w DESC)
        assert d["top_sectors"][0]["name"] == "Technology"
        assert d["top_sectors"][0]["change_pct"] == 6.95
        assert len(d["top_sectors"]) == 3
        # Bottom 3 (en dusuk negatif)
        assert d["bottom_sectors"][-1]["name"] == "Utilities"
        assert d["bottom_sectors"][-1]["change_pct"] == -1.29
        assert len(d["bottom_sectors"]) == 3

    def test_sectors_fallback_when_db_empty(self, client, monkeypatch):
        monkeypatch.setattr(api_main, "sector_rotation_get_latest", lambda: [])
        r = client.get("/api/market/status")
        d = r.json()
        # MOCK fallback (3 + 2 hardcoded)
        assert len(d["top_sectors"]) >= 1
        assert len(d["bottom_sectors"]) >= 1
        # MOCK_MARKET_STATUS default sektor isimleri
        top_names = {s["name"] for s in d["top_sectors"]}
        assert "Technology" in top_names or "Energy" in top_names

    def test_sectors_handle_none_perf(self, client, monkeypatch):
        # perf_1w None olabilir (yeni sektor, yetersiz veri) — crash etmesin
        mock_sectors = [
            {"ticker": "XLK", "sector_name": "Technology",  "perf_1w": 5.0},
            {"ticker": "XLE", "sector_name": "Energy",      "perf_1w": None},  # None graceful
            {"ticker": "XLU", "sector_name": "Utilities",   "perf_1w": -2.0},
        ]
        monkeypatch.setattr(api_main, "sector_rotation_get_latest", lambda: mock_sectors)
        r = client.get("/api/market/status")
        assert r.status_code == 200
        # None -> 0 default ile sirala
        d = r.json()
        assert d["top_sectors"][0]["name"] == "Technology"  # 5.0 en yuksek


class TestRegressionLegacy:
    """P398 sonrasi eski MOCK degerleri default fallback olarak hala mevcut."""

    def test_mock_market_status_intact(self):
        # MOCK_MARKET_STATUS hardcoded degerler korundu (fallback kaynagi)
        assert api_main.MOCK_MARKET_STATUS.vix == 14.2
        assert len(api_main.MOCK_MARKET_STATUS.top_sectors) >= 1
        assert len(api_main.MOCK_MARKET_STATUS.bottom_sectors) >= 1
