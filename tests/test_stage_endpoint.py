"""/api/stock/{symbol}/stage endpoint pytest (P121)."""
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


def test_status_200(client):
    r = client.get("/api/stock/NVDA/stage")
    assert r.status_code == 200


def test_required_fields(client):
    data = client.get("/api/stock/NVDA/stage").json()
    for f in ("category", "ma_value", "price_above_ma_pct", "slope_pct",
              "days_above_ma", "volume_trend", "mark_says"):
        assert f in data


def test_category_valid(client):
    data = client.get("/api/stock/NVDA/stage").json()
    valid = {"NO_TRANSITION", "EARLY_STAGE_2", "CONFIRMED_STAGE_2",
             "STAGE_2_MATURE", None}
    assert data["category"] in valid


def test_volume_trend_valid(client):
    data = client.get("/api/stock/AAPL/stage").json()
    valid = {"RISING", "STABLE", "FALLING", None}
    assert data["volume_trend"] in valid


def test_mark_says_non_empty(client):
    data = client.get("/api/stock/AAPL/stage").json()
    assert len(data["mark_says"]) > 5


def test_uppercase_normalize(client):
    data = client.get("/api/stock/nvda/stage").json()
    assert "category" in data
