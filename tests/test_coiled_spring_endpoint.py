"""
P510 (18 Haz 2026): /api/stock/{symbol}/coiled-spring endpoint pytest.

FastAPI TestClient + smoke + shape + Carr canon tutarlilik (TIER-2 CANDIDATE).
quanfina_math.compute_coiled_spring (P509, test_coiled_spring.py 11/11) backend wire.
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
    pytest.skip("fastapi TestClient veya api/main.py yok", allow_module_level=True)


@pytest.fixture(scope="module")
def client():
    return TestClient(api_main.app)


def _get(client, symbol):
    return client.get(f"/api/stock/{symbol}/coiled-spring")


class TestCoiledSpringEndpoint:
    FIELDS = (
        "detected", "direction", "quality", "signal_close", "entry", "stop",
        "target", "risk_pct", "rr", "sma20", "sma50", "eyeball_checks",
        "mark_says", "is_mock",
    )

    def test_response_shape(self, client):
        r = _get(client, "AAPL")
        assert r.status_code == 200
        data = r.json()
        for f in self.FIELDS:
            assert f in data, f"eksik alan: {f}"

    def test_direction_long_only(self, client):
        data = _get(client, "NVDA").json()
        assert data["direction"] in ("LONG", None)

    def test_candidate_quality_and_eyeball(self, client):
        """detected -> quality='CANDIDATE' (TIER-2, GOOD degil) + eyeball_checks dolu + exits."""
        data = _get(client, "AAPL").json()
        if data["detected"]:
            assert data["quality"] == "CANDIDATE"
            assert len(data["eyeball_checks"]) >= 3
            assert data["entry"] is not None
            assert data["stop"] is not None and data["stop"] < data["entry"]
            assert data["target"] is not None and data["target"] > data["entry"]
            assert data["risk_pct"] <= 8.01
            expected = data["entry"] + 2.0 * (data["entry"] - data["stop"])
            assert abs(data["target"] - expected) < 0.1
        else:
            assert data["entry"] is None
            assert data["eyeball_checks"] == []

    def test_unknown_symbol_no_crash(self, client):
        r = _get(client, "ZZZZZZ")
        assert r.status_code == 200
