"""
KARAR #733 alt-paket (Paket 71): /api/stock/{symbol}/pivot endpoint pytest.
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


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    """#31 (denetim): _fetch_ohlcv_real → None → endpoint _get_ohlcv deterministik
    _generate_ohlcv MOCK kullanır. Eski hali her test CANLI yfinance çağırıyordu
    (yavaş + flaky). Network-free + deterministik."""
    monkeypatch.setattr(api_main, "_fetch_ohlcv_real", lambda *a, **k: None)


class TestPivotEndpointShape:
    def test_status_code(self, client):
        r = client.get("/api/stock/NVDA/pivot")
        assert r.status_code == 200

    def test_required_fields(self, client):
        data = client.get("/api/stock/NVDA/pivot").json()
        for f in ("status", "pivot_price", "current_price", "breakout_pct",
                  "volume_multiplier", "volume_confirmed", "mark_says"):
            assert f in data

    def test_status_valid(self, client):
        data = client.get("/api/stock/NVDA/pivot").json()
        valid = {"CONFIRMED", "WEAK", "NEAR_PIVOT", "BELOW_PIVOT", None}
        assert data["status"] in valid

    def test_volume_confirmed_bool(self, client):
        data = client.get("/api/stock/NVDA/pivot").json()
        assert isinstance(data["volume_confirmed"], bool)

    def test_mark_says_non_empty(self, client):
        data = client.get("/api/stock/NVDA/pivot").json()
        assert len(data["mark_says"]) > 5

    def test_symbol_uppercase(self, client):
        data = client.get("/api/stock/nvda/pivot").json()
        # Endpoint sym.upper() yapıyor, NVDA için OHLCV oluştur
        assert "current_price" in data

    def test_different_symbols(self, client):
        nvda = client.get("/api/stock/NVDA/pivot").json()
        tsla = client.get("/api/stock/TSLA/pivot").json()
        # Farklı fiyatlar olmalı
        assert nvda["current_price"] != tsla["current_price"]
