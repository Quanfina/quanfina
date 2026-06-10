"""
KARAR #733 alt-paket (Paket 93): /api/stock/{symbol}/rs endpoint pytest.
Mark/IBD RS Rating endpoint canon.
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


class TestRsEndpointShape:
    def test_status_code(self, client):
        r = client.get("/api/stock/NVDA/rs")
        assert r.status_code == 200

    def test_required_fields(self, client):
        data = client.get("/api/stock/NVDA/rs").json()
        for f in ("rs_rating", "category", "stock_return_pct",
                  "benchmark_return_pct", "outperform_pct", "mark_says"):
            assert f in data

    def test_category_valid(self, client):
        data = client.get("/api/stock/NVDA/rs").json()
        valid = {"LEADER", "STRONG", "AVERAGE", "LAGGARD", None}
        assert data["category"] in valid

    def test_mark_says_non_empty(self, client):
        data = client.get("/api/stock/NVDA/rs").json()
        assert len(data["mark_says"]) > 5


class TestRsEndpointValues:
    def test_rs_rating_in_range(self, client):
        data = client.get("/api/stock/AAPL/rs").json()
        assert data["rs_rating"] is None or 1 <= data["rs_rating"] <= 99

    def test_spy_self_average(self, client):
        """SPY kendi-kendine RS = AVERAGE (50)."""
        data = client.get("/api/stock/SPY/rs").json()
        assert data["rs_rating"] == 50
        assert data["category"] == "AVERAGE"

    def test_different_symbols(self, client):
        nvda = client.get("/api/stock/NVDA/rs").json()
        tsla = client.get("/api/stock/TSLA/rs").json()
        # Deterministic OHLCV per symbol — sonuçlar farklı olabilir
        assert "mark_says" in nvda
        assert "mark_says" in tsla

    def test_uppercase_normalize(self, client):
        data = client.get("/api/stock/nvda/rs").json()
        assert "category" in data


class TestRsConsistency:
    """P441 (10 Haz 2026): /hisse RS tutarsizligi fix. Eski: StockHeader
    info.rs_rating=97 (cross-sectional IBD) vs /rs=55 (compute hardcoded bant).
    Fix: /rs DB-FIRST → ayni rs_ibd kaynagi → header (info) = kart (/rs)."""

    def test_info_rs_matches_rs_endpoint(self, client):
        info = client.get("/api/stock/NVDA/info").json()
        rs = client.get("/api/stock/NVDA/rs").json()
        # Header (info.rs_rating) ile kart (/rs rs_rating) BIREBIR ayni olmali
        assert rs["rs_rating"] == info["rs_rating"]
        assert rs["source"] == "scan"

    def test_category_matches_threshold(self, client):
        """rs_rating_category eşiği (LEADER>=80 / STRONG>=70 / AVERAGE>=50 / LAGGARD)."""
        rs = client.get("/api/stock/NVDA/rs").json()
        v = rs["rs_rating"]
        cat = rs["category"]
        if v >= 80:
            assert cat == "LEADER"
        elif v >= 70:
            assert cat == "STRONG"
        elif v >= 50:
            assert cat == "AVERAGE"
        else:
            assert cat == "LAGGARD"

    def test_source_field_present(self, client):
        """P441: source alani ('scan'|'computed') her yanitta var (Kural #28)."""
        rs = client.get("/api/stock/NVDA/rs").json()
        assert rs["source"] in ("scan", "computed")
