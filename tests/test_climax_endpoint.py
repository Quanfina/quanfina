"""
KARAR #733 alt-paket (Paket 87): /api/stock/{symbol}/climax endpoint pytest.
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
except ImportError:
    pytest.skip("fastapi TestClient yok", allow_module_level=True)


@pytest.fixture(scope="module")
def client():
    return TestClient(api_main.app)


class TestClimaxEndpointShape:
    def test_status_code(self, client):
        r = client.get("/api/stock/NVDA/climax")
        assert r.status_code == 200

    def test_required_fields(self, client):
        data = client.get("/api/stock/NVDA/climax").json()
        for f in ("category", "gain_pct", "gap_up_days", "avg_volume_ratio", "mark_says"):
            assert f in data

    def test_category_valid(self, client):
        data = client.get("/api/stock/NVDA/climax").json()
        valid = {"CLIMAX_TOP", "POTENTIAL_CLIMAX", "HEALTHY_ADVANCE", "NONE", None}
        assert data["category"] in valid

    def test_mark_says_non_empty(self, client):
        data = client.get("/api/stock/NVDA/climax").json()
        assert len(data["mark_says"]) > 5

    def test_symbol_uppercase(self, client):
        data = client.get("/api/stock/nvda/climax").json()
        assert "category" in data

    def test_different_symbols(self, client):
        nvda = client.get("/api/stock/NVDA/climax").json()
        tsla = client.get("/api/stock/TSLA/climax").json()
        # OHLCV deterministic per symbol seed — farklı sonuçlar
        assert "mark_says" in nvda and "mark_says" in tsla


class TestClimaxEndpointValues:
    def test_gap_up_days_int_or_none(self, client):
        data = client.get("/api/stock/NVDA/climax").json()
        assert data["gap_up_days"] is None or isinstance(data["gap_up_days"], int)

    def test_gain_pct_float_or_none(self, client):
        data = client.get("/api/stock/AAPL/climax").json()
        assert data["gain_pct"] is None or isinstance(data["gain_pct"], (int, float))

    def test_volume_ratio_float_or_none(self, client):
        data = client.get("/api/stock/TSLA/climax").json()
        ratio = data["avg_volume_ratio"]
        assert ratio is None or isinstance(ratio, (int, float))
