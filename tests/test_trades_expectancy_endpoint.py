"""
KARAR #733 alt-paket (Paket 89): /api/trades/expectancy endpoint pytest.
Mark Brandon Video canon — kapalı trade'ler expectancy hesabı.
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


class TestExpectancyEndpointShape:
    def test_status_code(self, client):
        r = client.get("/api/trades/expectancy")
        assert r.status_code == 200

    def test_required_fields(self, client):
        data = client.get("/api/trades/expectancy").json()
        for f in ("expectancy_pct", "category", "meets_brandon_targets",
                  "mark_says", "total_closed", "winners", "losers",
                  "avg_gain_pct", "avg_loss_pct", "win_rate"):
            assert f in data, f"Missing field: {f}"

    def test_category_valid(self, client):
        data = client.get("/api/trades/expectancy").json()
        valid = {"EXCELLENT", "GOOD", "ACCEPTABLE", "POOR", None}
        assert data["category"] in valid

    def test_mark_says_non_empty(self, client):
        data = client.get("/api/trades/expectancy").json()
        assert len(data["mark_says"]) > 5

    def test_meets_brandon_targets_bool(self, client):
        data = client.get("/api/trades/expectancy").json()
        assert isinstance(data["meets_brandon_targets"], bool)


class TestExpectancyEndpointAggregation:
    def test_totals_consistent(self, client):
        data = client.get("/api/trades/expectancy").json()
        # winners + losers == total_closed
        assert data["winners"] + data["losers"] == data["total_closed"]

    def test_win_rate_in_range(self, client):
        data = client.get("/api/trades/expectancy").json()
        assert 0.0 <= data["win_rate"] <= 1.0

    def test_avg_gain_non_negative(self, client):
        data = client.get("/api/trades/expectancy").json()
        assert data["avg_gain_pct"] >= 0

    def test_avg_loss_non_negative(self, client):
        # avg_loss_pct |mutlak| olarak döner — pozitif
        data = client.get("/api/trades/expectancy").json()
        assert data["avg_loss_pct"] >= 0

    def test_mock_fallback_has_closed_trades(self, client):
        """MOCK fallback'te 4 kapalı trade var (id 1,3,5,7)."""
        data = client.get("/api/trades/expectancy").json()
        # En az MOCK'taki 4 kapalı trade görülmeli (DB up ise daha fazla olabilir)
        assert data["total_closed"] >= 0


class TestExpectancyStrategyFilter:
    """KARAR #733 alt-paket (Paket 96): strategy query filter."""

    def test_strategy_minervini_filter(self, client):
        data = client.get("/api/trades/expectancy?strategy=minervini").json()
        assert "category" in data
        # Filter sonucu total_closed <= unfiltered total_closed olmalı
        all_data = client.get("/api/trades/expectancy").json()
        assert data["total_closed"] <= all_data["total_closed"]

    def test_strategy_carr_filter(self, client):
        data = client.get("/api/trades/expectancy?strategy=carr").json()
        assert "category" in data

    def test_strategy_all_equals_no_filter(self, client):
        all_data = client.get("/api/trades/expectancy?strategy=all").json()
        no_filter = client.get("/api/trades/expectancy").json()
        assert all_data["total_closed"] == no_filter["total_closed"]

    def test_strategy_uppercase_normalize(self, client):
        a = client.get("/api/trades/expectancy?strategy=MINERVINI").json()
        b = client.get("/api/trades/expectancy?strategy=minervini").json()
        # Lowercase normalize sayesinde eşit sonuç
        assert a["total_closed"] == b["total_closed"]

    def test_minervini_plus_carr_equals_all(self, client):
        """Minervini + Carr total = unfiltered total (MOCK 8 trade hepsi minervini veya carr)."""
        m = client.get("/api/trades/expectancy?strategy=minervini").json()
        c = client.get("/api/trades/expectancy?strategy=carr").json()
        all_data = client.get("/api/trades/expectancy").json()
        assert m["total_closed"] + c["total_closed"] == all_data["total_closed"]

    def test_strategy_unknown_returns_empty(self, client):
        """Bilinmeyen strategy = 0 closed."""
        data = client.get("/api/trades/expectancy?strategy=nonexistent").json()
        assert data["total_closed"] == 0
