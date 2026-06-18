"""
P520 (18 Haz 2026): /api/stock/{symbol}/gap-down endpoint pytest (SHORT reversal).

FastAPI TestClient + smoke + shape + Carr canon (SHORT, entry=close, CANDIDATE haber teyidi).
quanfina_math.compute_gap_down (P519, test_gap_down.py 10/10) backend wire.
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
    return client.get(f"/api/stock/{symbol}/gap-down")


class TestGapDownEndpoint:
    FIELDS = (
        "detected", "direction", "quality", "signal_close", "entry", "stop",
        "target", "risk_pct", "rr", "sma50", "gap_pct", "eyeball_checks",
        "mark_says", "is_mock",
    )

    def test_response_shape(self, client):
        r = _get(client, "AAPL")
        assert r.status_code == 200
        data = r.json()
        for f in self.FIELDS:
            assert f in data, f"eksik alan: {f}"

    def test_direction_short_only(self, client):
        data = _get(client, "NVDA").json()
        assert data["direction"] in ("SHORT", None)

    def test_canon_short_close_entry(self, client):
        """detected -> SHORT: entry=close (s.270 market emri); stop USTTE/target ALTTA; CANDIDATE."""
        data = _get(client, "AAPL").json()
        if data["detected"]:
            assert data["direction"] == "SHORT"
            assert data["quality"] == "CANDIDATE"
            assert data["entry"] == data["signal_close"]  # entry=close (signal low BEKLEMEZ)
            assert data["stop"] > data["entry"] and data["target"] < data["entry"]
            assert data["risk_pct"] <= 8.01
            assert len(data["eyeball_checks"]) >= 1  # haber teyidi notu
        else:
            assert data["entry"] is None

    def test_unknown_symbol_no_crash(self, client):
        r = _get(client, "ZZZZZZ")
        assert r.status_code == 200
