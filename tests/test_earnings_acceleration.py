"""
P569 (20 Haz 2026): Earnings Acceleration — CANSLIM 'C' derin fundamental (#75 pipeline).

Minervini CANSLIM-C: çeyreklik EPS+satış YoY büyümesi >= %25 + HIZLANIYOR. yfinance quarterly
income statement (yapısal, güvenilir). Veri yetersiz/None → available=False (Kural #28 — uydurma yok).
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

from quanfina_math import compute_earnings_acceleration  # noqa: E402


class TestComputeEarningsAcceleration:
    def test_strong_growth_5q_both_pass(self):
        # NVDA-benzeri (en yeni önce). YoY = q[0] vs q[4].
        rev = [81615, 68127, 57006, 46743, 44062]   # YoY=(81615-44062)/44062≈%85
        ni = [58321, 42960, 31910, 26422, 18775]     # YoY≈%211
        r = compute_earnings_acceleration(rev, ni)
        assert r["available"] is True
        assert r["revenue_yoy_pct"] == pytest.approx(85.2, abs=0.5)
        assert r["earnings_yoy_pct"] == pytest.approx(210.6, abs=0.5)
        assert r["both_pass"] is True
        # 5 çeyrek → önceki YoY yok → hızlanma hesaplanamaz (False)
        assert r["revenue_accelerating"] is False

    def test_acceleration_6q(self):
        # 6 çeyrek: YoY[0] > YoY[1] → hızlanıyor
        rev = [160, 120, 110, 105, 100, 80]  # YoY0=(160-100)/100=60% ; YoY1=(120-80)/80=50%
        ni = [60, 40, 30, 25, 30, 25]        # YoY0=(60-30)/30=100% ; YoY1=(40-25)/25=60%
        r = compute_earnings_acceleration(rev, ni)
        assert r["available"] is True
        assert r["revenue_accelerating"] is True   # 60 > 50
        assert r["earnings_accelerating"] is True   # 100 > 60
        assert "HIZLANIYOR" in r["mark_says"]

    def test_insufficient_quarters_unavailable(self):
        r = compute_earnings_acceleration([100, 90, 80, 70], [10, 9, 8, 7])  # 4 çeyrek
        assert r["available"] is False
        assert r["quarters_used"] == 4

    def test_negative_base_earnings_nm(self):
        # Net kâr 1 yıl önce zarar (negatif baz) → earnings YoY n/m (None), revenue hesaplanır
        rev = [120, 110, 100, 95, 90]
        ni = [10, 5, -2, -5, -8]   # ni[4]=-8 (negatif baz) → earnings YoY None
        r = compute_earnings_acceleration(rev, ni)
        assert r["available"] is True
        assert r["revenue_yoy_pct"] is not None
        assert r["earnings_yoy_pct"] is None   # negatif baz → n/m (uydurma yok)

    def test_none_in_series_counts_only_valid(self):
        # rev'de 4 geçerli değer (None hariç) → <5 → available False (uydurma yok)
        rev = [120, 110, 100, 95, None]
        ni = [10, 9, 8, 7, 6]
        r = compute_earnings_acceleration(rev, ni)
        assert r["available"] is False
        assert r["quarters_used"] == 4

    def test_none_base_yoy_skipped(self):
        # 5 geçerli ama baz (q[4]) None → revenue YoY None, available True
        rev = [120, 110, 100, 95, None, 88]   # 5 geçerli (88 dahil), q[4]=None → YoY None
        ni = [10, 9, 8, 7, 6, 5]
        r = compute_earnings_acceleration(rev, ni)
        assert r["available"] is True
        assert r["revenue_yoy_pct"] is None  # q[4] None → YoY hesaplanamaz (n/m)

    def test_empty_unavailable(self):
        r = compute_earnings_acceleration([], [])
        assert r["available"] is False


# --- Endpoint ---
try:
    from fastapi.testclient import TestClient
    import main as api_main
    _HAS_API = True
except ImportError:
    _HAS_API = False


@pytest.mark.skipif(not _HAS_API, reason="fastapi yok")
class TestEarningsGrowthEndpoint:
    @pytest.fixture(scope="class")
    def client(self):
        return TestClient(api_main.app)

    def test_real_data_endpoint(self, client, monkeypatch):
        monkeypatch.setattr(api_main, "_fetch_quarterly_financials",
                            lambda s: ([81615, 68127, 57006, 46743, 44062],
                                       [58321, 42960, 31910, 26422, 18775]))
        r = client.get("/api/stock/NVDA/earnings-growth")
        assert r.status_code == 200
        d = r.json()
        assert d["available"] is True
        assert d["both_pass"] is True
        assert d["revenue_yoy_pct"] is not None

    def test_yfinance_fail_unavailable_not_mock(self, client, monkeypatch):
        # yfinance erişilemez → ([],[]) → available False (uydurma YOK — Kural #28)
        monkeypatch.setattr(api_main, "_fetch_quarterly_financials", lambda s: ([], []))
        r = client.get("/api/stock/ZZZZ/earnings-growth")
        assert r.status_code == 200
        d = r.json()
        assert d["available"] is False
        assert d["revenue_yoy_pct"] is None
