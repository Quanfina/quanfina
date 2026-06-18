"""
P506 (18 Haz 2026): /api/stock/{symbol}/pullback endpoint pytest.

FastAPI TestClient + smoke + shape + Carr canon tutarlilik koruma.
quanfina_math.compute_carr_pullback (P504-P505, test_carr_pullback.py 13/13) backend wire.
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
    return client.get(f"/api/stock/{symbol}/pullback")


class TestPullbackEndpoint:
    FIELDS = (
        "detected", "direction", "quality", "signal_close", "entry", "stop",
        "target", "risk_pct", "rr", "sma20", "sma50", "sma200", "stoch_k",
        "mark_says", "is_mock",
    )

    def test_response_shape(self, client):
        r = _get(client, "AAPL")
        assert r.status_code == 200
        data = r.json()
        for f in self.FIELDS:
            assert f in data, f"eksik alan: {f}"

    def test_direction_long_only(self, client):
        """Carr Pullback LONG-only (countertrend MR'den farkli — SHORT yok)."""
        data = _get(client, "NVDA").json()
        assert data["direction"] in ("LONG", None)

    def test_canon_consistency(self, client):
        """detected -> entry/stop/target dolu + %8 cap + 2R; degilse entry None (Kural #26)."""
        data = _get(client, "AAPL").json()
        if data["detected"]:
            assert data["entry"] is not None
            assert data["stop"] is not None and data["stop"] < data["entry"]
            assert data["target"] is not None and data["target"] > data["entry"]
            assert data["risk_pct"] <= 8.01, "Carr s.322 %8 hard cap ihlali"
            expected = data["entry"] + 2.0 * (data["entry"] - data["stop"])
            assert abs(data["target"] - expected) < 0.1
        else:
            assert data["entry"] is None
            assert data["stop"] is None
            assert data["target"] is None

    def test_unknown_symbol_no_crash(self, client):
        """Bilinmeyen sembol -> 200 (sentetik fallback) veya is_mock True, crash yok."""
        r = _get(client, "ZZZZZZ")
        assert r.status_code == 200
