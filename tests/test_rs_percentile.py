"""
P561 (20 Haz 2026): RS Rating cross-sectional persentil — taranmamış sembol fallback.

IBD canon RS = hissenin getirisinin TÜM evrene göre persentil rank'ı (vs-SPY/mutlak DEĞİL).
Eski hardcoded bant yerine gerçek evren-persentili (minervini_scans.perf_year). MOCK GUARD:
sadece gerçek yfinance verisi ile hesaplanır (Kural #28); yoksa eski yaklaşık (işaretli).
"""
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent
API_DIR = PROJECT_ROOT / "api"
for d in (str(PROJECT_ROOT), str(API_DIR)):
    if d not in sys.path:
        sys.path.insert(0, d)

from quanfina_math import compute_rs_percentile  # noqa: E402


class TestComputeRsPercentile:
    UNIV = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]  # 11 değer

    def test_top_performer_high_rs(self):
        # 95 → 10/11 altında → ~90
        assert compute_rs_percentile(95, self.UNIV) == 90

    def test_median_mid_rs(self):
        # 50 → 5 altında (0,10,20,30,40) → 5/11*99 = 45
        assert compute_rs_percentile(50, self.UNIV) == 45

    def test_above_all_universe_99(self):
        assert compute_rs_percentile(1000, self.UNIV) == 99

    def test_below_all_universe_clamped_1(self):
        # -5 → 0 altında → 0 → clamp 1 (asla 0)
        assert compute_rs_percentile(-5, self.UNIV) == 1

    def test_none_return_none(self):
        assert compute_rs_percentile(None, self.UNIV) is None

    def test_empty_universe_none(self):
        # MOCK YOK (Kural #28): evren yok → None (uydurma rakam değil)
        assert compute_rs_percentile(50, []) is None

    def test_result_always_1_99(self):
        for r in (-100, 0, 12.5, 250, 5000):
            v = compute_rs_percentile(r, self.UNIV)
            assert v is None or (1 <= v <= 99)


# --- Endpoint (TestClient) ---
try:
    from fastapi.testclient import TestClient
    import main as api_main
    _HAS_API = True
except ImportError:
    _HAS_API = False


@pytest.mark.skipif(not _HAS_API, reason="fastapi yok")
class TestRsEndpointXSec:
    @pytest.fixture(scope="class")
    def client(self):
        return TestClient(api_main.app)

    def _bars(self, first: float, last: float, n: int = 200):
        # closes[0]=first, closes[-1]=last → 1y getiri (last/first-1)
        mid = [SimpleNamespace(close=last)] * (n - 1)
        return [SimpleNamespace(close=first), *mid]

    def test_untracked_real_data_cross_sectional(self, client, monkeypatch):
        monkeypatch.setattr(api_main, "_resolve_rs_ibd", lambda s: None)  # taranmamış
        monkeypatch.setattr(api_main, "_fetch_ohlcv_real", lambda s, n=252: self._bars(100, 150))  # +%50
        monkeypatch.setattr(api_main, "universe_perf_year_values", lambda: [10, 20, 30, 60, 70])
        # stock %50 → 3 altında (10,20,30) → 3/5*99 = 59
        r = client.get("/api/stock/ZZZX/rs")
        assert r.status_code == 200
        d = r.json()
        assert d["rs_rating"] == 59
        assert d["source"] == "computed"
        assert "Cross-sectional" in d["mark_says"]
        assert d["stock_return_pct"] == 50.0

    def test_mock_guard_no_real_data_falls_back(self, client, monkeypatch):
        # yfinance fail (real None) → cross-sectional HESAPLAMA YOK (sahte üretme — Kural #28)
        monkeypatch.setattr(api_main, "_resolve_rs_ibd", lambda s: None)
        monkeypatch.setattr(api_main, "_fetch_ohlcv_real", lambda s, n=252: None)
        monkeypatch.setattr(api_main, "universe_perf_year_values", lambda: [10, 20, 30])
        r = client.get("/api/stock/ZZZY/rs")
        assert r.status_code == 200
        # eski yaklaşık fallback (cross-sectional mesajı YOK)
        assert "Cross-sectional" not in r.json()["mark_says"]

    def test_empty_universe_falls_back(self, client, monkeypatch):
        monkeypatch.setattr(api_main, "_resolve_rs_ibd", lambda s: None)
        monkeypatch.setattr(api_main, "_fetch_ohlcv_real", lambda s, n=252: self._bars(100, 150))
        monkeypatch.setattr(api_main, "universe_perf_year_values", lambda: [])  # evren yok
        r = client.get("/api/stock/ZZZW/rs")
        assert r.status_code == 200
        assert "Cross-sectional" not in r.json()["mark_says"]

    def test_scanned_symbol_uses_scan_not_xsec(self, client, monkeypatch):
        # Taranmış sembol → scan rs_ibd (cross-sectional fallback'e GİRMEZ)
        monkeypatch.setattr(api_main, "_resolve_rs_ibd", lambda s: 88)
        r = client.get("/api/stock/NVDA/rs")
        d = r.json()
        assert d["rs_rating"] == 88
        assert d["source"] == "scan"
